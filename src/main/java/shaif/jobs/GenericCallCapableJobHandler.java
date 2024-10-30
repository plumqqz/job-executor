package shaif.jobs;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.*;

@Service
@Slf4j
public abstract class GenericCallCapableJobHandler<P,C> extends GenericJobHandler<P,C> {
     static class JobExecutionState{
        BlockingQueue<FromWorker> qFromWorker;
        BlockingQueue<ToWorker> qToWorker;
        CompletableFuture<Void> ft;
        boolean starting=true;
        Object context;
    }
    Map<Long,JobExecutionState> queuesMap = new ConcurrentHashMap<>();
    ExecutorService es = Executors.newFixedThreadPool(16);

    enum QElementType { CALL, STORE, RETURN };
    public static class FromWorker {
        QElementType type;
        JobState jobState;
        String message;
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
        public <PJh, CJh, Jh extends GenericJobHandler<PJh, CJh>, T> T call(Jh jobHandler, PJh p, String message, Class<T> clazz) {
            var qv = new FromWorker();
            qv.type = QElementType.CALL;
            qv.message = message;
            var jes = queuesMap.get(getId());
            try {
                var jen = startAndWait(jobHandler, p);
                jes.qFromWorker.put(qv);
                jes.qToWorker.take();
                return jen.getReturnValue(clazz);
            } catch (InterruptedException e) {
                log.error("Exception", e);
                throw new RuntimeException(e);
            }
        }

        public void join(String message){
            var qv = new FromWorker();
            qv.type = QElementType.STORE;
            qv.message = message;
            var jes = queuesMap.get(getId());
            try {
                jes.qFromWorker.put(qv);
                jes.qToWorker.take();
            } catch (InterruptedException e) {
                log.error("Exception", e);
                throw new RuntimeException(e);
            }
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

    private final ToWorker toWorker = new ToWorker();

    @Override
    public JobState execute(Job job, P p, C c) throws Exception {
        Long jobId = job.getId();
        var jes = queuesMap.get(jobId);
        if(jes==null){ //first job run
            jes = new JobExecutionState();
            jes.qFromWorker = new SynchronousQueue<>(true);
            jes.qToWorker = new SynchronousQueue<>(true);
            queuesMap.put(jobId, jes);
            return JobState.CONTINUE("Started");
        }else{
            if(jes.starting) {
                JobExecutionState finalJes = jes;
                finalJes.starting = false;
                finalJes.context = c;
                finalJes.ft = CompletableFuture.supplyAsync(()->{
                    try {
                        finalJes.qFromWorker.put(retval(realExecute(new CallCapableJob(job), p, c)));
                        return null;
                    } catch (Exception e) {
                        log.error("Exception", e);
                        throw new RuntimeException(e);
                    }
                }, es);
            }else{
                jes.qToWorker.put(toWorker);
            }
        }

        var fw = jes.qFromWorker.take();
        if(fw.type==QElementType.CALL) {
            return JobState.CONTINUE(fw.message);
        } else if (fw.type==QElementType.STORE) {
            log.info("STORE:{}", fw.message);
            synchronized (this) {
                BeanUtils.copyProperties(jes.context, c);
                return JobState.CONTINUE(fw.message);
            }
        }else if(fw.type==QElementType.RETURN){
            return fw.jobState;
        }else {
            throw new RuntimeException("Unknown QElement type");
        }
    }
}
