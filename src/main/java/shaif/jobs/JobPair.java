package shaif.jobs;

/**
 * Просто вспомогательный класс для работы с задачами, состоящими из нескольких шагов, наприме
 * <pre>
 * class Worker extends GenericRestBasedJobHandler<Worker.Parameters, Worker.Context> {
 *      public static class Parameters{
 *          String parameter;
 *      }
 *      public static class Context{
 *          enum Status {INITIAL, STEP1, STEP2, DONE}
 *          Status status
 *      }
 *     @Override
 *     public JobState realExecute(Job job, Parameters p, Context c) throws Exception {
 *         JobPair<Context.JobStatus> rv = switch (c.jobStatus) {
 *            case INITIAL -> {
 *                  // do some work
 *                  yield JobPair.of(STEP1, JobState.CONTINUE("Initialized"));
 *              }
 *            case STEP1 -> {
 *               // do some work
 *               yield JobPair.of(STEP2, JobState.CONTINUE("STEP1 is done"));
 *            }
 *            case STEP2 -> {
 *               // do some work
 *               yield JobPair.of(DONE, JobState.DONE("STEP2 is done"));
 *            }
 *          };
 *          c.status = rv.getValue(c.status);
 *          return rv.getJobState();
 *     }
 * }
 * </pre>
 * @param <T>
 */
public class JobPair<T>{
    T v;
    JobState jobState;

    public static<T> JobPair<T> of(T v, JobState jobState) {
        var rv = new JobPair<T>();
        rv.v = v;
        rv.jobState = jobState;
        return rv;
    }
    public static<T> JobPair<T> of(JobState jobState, T v) {
        var rv = new JobPair<T>();
        rv.v = v;
        rv.jobState = jobState;
        return rv;
    }

    public static<T> JobPair<T> of(JobState jobState) {
        var rv = new JobPair<T>();
        rv.v = null;
        rv.jobState = jobState;
        return rv;
    }

    public T getValue() {
        return v;
    }

    public JobState getJobState() {
        return jobState;
    }

    public T getValue(T v) {
        if(this.v!=null) return this.v;
        return v;
    }

}
