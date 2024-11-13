package shaif.jobs;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;

/**
 *
 * @param <P> - job parameters
 * @param <C> - job context
 *
*            This class allows two additional operations: pseudosynchronously call another jobs and get its return value and
 *           pseudosleep in execution. This features allow to write job handlers in pure imperative code, for example
 *           instead of
 *           <code>
 *               switch(context.state){
 *                  case INITIAL:
 *                      // do initial operations
 *                      context.jobOne = job.submit(longRunningTaskOne....)
 *                      context.state = LONG_RUNNING_TASK_ONE_COMPLETED;
 *                      return JobState.CONTINUE("Await for end of longRunningTaskOne");
 *                  case LONG_RUNNING_TASK_ONE_COMPLETED:
 *                      context.oneValue = job.getReturnValue(context.jobOne, OneTaskReturnValue.class);
 *                      //do some work
 *                      context.jobTwo = job.submit(longRunningTaskTwo, ....);
 *                      context.state = LONG_RUNNING_TASK_TWO_COMPLETED;
 *                      return JobState.CONTINUE("Await for end of longRunningTaskTwo");
 *                  case LONG_RUNNING_TASK_TWO_COMPLETED:
 *                      ...
 *               }
 *           </code>
 *           use
 *           <code>
 *              // some work
 *              context.oneValue = job.call(longRunningTaskOne, taskOneParameters, "Wait for oneTask", OneTaskReturnValue.class);
 *              // some another work
 *              context.secondValue = job.call(longRunningTaskTwo, taskTwoParameters, "Wait for oneTask", SecondTaskReturnValue.class);
 *              // and yet another work
 *           </code>
 *
 *           You must note, that execution may be call many times in case of application crash, so it must handle such cases
 *           correctly. Also, if some <i>called</i> had completed before crash, next <i>call</i> will return saved result.
 *
 *           Each task used GenericCallCapableJobHandler utilizes one thread completely; so, if you want to yield such thread
 *           you may return JobState.CONTINUE
 */
@Service
@Slf4j
public abstract class GenericCallCapableJobHandler<P,C> extends GenericJobHandler<P,C> {
     static class JobExecutionState{
        BlockingQueue<FromWorker> qFromWorker;
        BlockingQueue<ToWorker> qToWorker;
        CompletableFuture<Void> ft;
        boolean starting=true;
        Object context;
        Object retval;
    }
    Map<Long,JobExecutionState> queuesMap = new ConcurrentHashMap<>();
    ExecutorService es = Executors.newFixedThreadPool(16);

    enum QElementType { CALL, STORE, RETURN, SLEEP };
    public static class FromWorker {
        QElementType type;
        JobState jobState;
        String message;
        Duration sleepDuration;
    }
    public static class ToWorker{}

    FromWorker normal(JobState jobState){
        var rv = new FromWorker();
        rv.jobState = jobState;
        rv.type= QElementType.CALL;
        return rv;
    }

    FromWorker retval(JobState jobState){
        var rv = new FromWorker();
        rv.type= QElementType.RETURN;
        rv.jobState=jobState;
        return rv;
    }

    public class CallCapableJob extends Job {

        public <PJh, CJh, Jh extends GenericJobHandler<PJh, CJh>, T> T call(Jh jobHandler, PJh p, String message, Class<T> clazz) throws InterruptedException {
            var qv = new FromWorker();
            qv.type = QElementType.CALL;
            qv.message = message;
            var jes = queuesMap.get(getId());
            var jen = startAndWait(jobHandler, p);
            jes.qFromWorker.put(qv);
            jes.qToWorker.take();
            return jen.getReturnValue(clazz);
        }

        public void commitAndJoin(String message) throws InterruptedException {
            var qv = new FromWorker();
            qv.type = QElementType.STORE;
            qv.message = message;
            var jes = queuesMap.get(getId());
            jes.qFromWorker.put(qv);
            jes.qToWorker.take();
        }

        public void sleep(Duration duration, String message) throws InterruptedException {
            var qv = new FromWorker();
            qv.type = QElementType.SLEEP;
            qv.message = message;
            qv.sleepDuration = duration;
            var jes = queuesMap.get(getId());
            jes.qFromWorker.put(qv);
            jes.qToWorker.take();
        }

        public CallCapableJob(Job j) {
            setId(j.getId());
            setName(j.getName());
            setParameters(j.getParameters());
            setContext(j.getContext());
            setStatusMessage(j.getStatusMessage());
            setNextRunAfter(j.getNextRunAfter());
            setDone(j.isDone());
            setFailed(j.isFailed());
            setParentJobId(j.getParentJobId());
            setJobExecutor(j.getJobExecutor());
        }
    }

    abstract public JobState realExecute(CallCapableJob job, P p, C c) throws Exception;
    public Map<Class<? extends Throwable>, Duration> getRetryTimeoutsOnExceptions(){
        return Map.of();
    }
    public Duration getTimeoutOnException(Throwable e){
        var timeouts = getRetryTimeoutsOnExceptions();
        if(timeouts.containsKey(e) || e.getCause()!=null && timeouts.containsKey(e.getCause())){
            return timeouts.get(timeouts.containsKey(e) ? e : e.getCause());
        }else {
            return null;
        }

    }

    private final ToWorker toWorker = new ToWorker();

    @Override
    public JobState execute(Job job, P p, C c) throws Exception {
        Long jobId = job.getId();
        final var jes = queuesMap.get(jobId);
        if(jes==null){ //first job run
            var njes = new JobExecutionState();
            njes.qFromWorker = new SynchronousQueue<>(true);
            njes.qToWorker = new SynchronousQueue<>(true);
            queuesMap.put(jobId, njes);
            return JobState.CONTINUE("Started");
        }else{
            if(jes.starting) {
                jes.starting = false;
                jes.context = c;
                jes.ft = CompletableFuture.supplyAsync(()->{
                    CallCapableJob ccJob=null;
                    try {
                        ccJob = new CallCapableJob(job);
                        jes.qFromWorker.put(retval(realExecute(ccJob, p, c)));
                        return null;
                    } catch (InterruptedException e) {
                        return null;
                    } catch (Throwable e) {
                        var duration = getTimeoutOnException(e);
                        if(duration!=null){
                            try {
                                jes.qFromWorker.put(retval(JobState.CONTINUE("Wait " + duration + " for retry", duration)));
                            } catch (InterruptedException ex) {
                                log.warn("Interrupted");
                            }
                            return null;
                        }
                        log.error("Exception", e);
                        try {
                            jes.qFromWorker.put(retval(JobState.STOP(e.getMessage())));
                        } catch (InterruptedException ex) {
                            log.error("Got InterruptedException during exceptional return from job handler");
                            throw new RuntimeException(ex);
                        }
                    }
                    return null;
                }, es);
            }else{
                jes.qToWorker.put(toWorker);
            }
        }

        var fw = jes.qFromWorker.take();
        log.trace("fw:{}", fw);
        if(fw.type==QElementType.CALL) {
            return JobState.CONTINUE(fw.message);
        } else if (fw.type==QElementType.STORE) {
            log.info("STORE:{}", fw.message);
            synchronized (jes) {
                BeanUtils.copyProperties(jes.context, c);
            }
            return JobState.CONTINUE(fw.message);
        }else if(fw.type==QElementType.RETURN){
            synchronized (jes) {
                BeanUtils.copyProperties(jes.context, c);
            }
            queuesMap.remove(jobId);
            return fw.jobState;
        }else if(fw.type == QElementType.SLEEP){
            log.info("SLEEP:{} for {}", fw.message, fw.sleepDuration);
            return JobState.CONTINUE(fw.message, fw.sleepDuration);
        }else {
            throw new RuntimeException("Unknown QElement type");
        }
    }
}
