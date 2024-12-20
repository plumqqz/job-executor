package shaif.jobs;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public abstract class GenericRetryableJobHandler<P,C> extends GenericJobHandler<P,C>{
    public abstract Map<Class<? extends Throwable>, Duration> getTimeout();
    abstract public JobState realExecute(Job job, P p, C c) throws Exception;

    @SafeVarargs
    public static <T> T tryCoalesce(Supplier<? extends T>...suppliers){
        List<Exception> exceptions = new ArrayList<>();
        for(var s: suppliers){
            try {
                var rv = s.get();
                if(rv!=null) return rv;
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        var rve = new RuntimeException("Cannot evaluate values");
        for(var se : exceptions){
            rve.addSuppressed(se);
        }
        throw rve;
    }

    @SuppressWarnings("Convert2MethodRef")
    @Override
    public JobState execute(Job job, P p, C c) throws Exception{
        try{
            return realExecute(job, p, c);
        }  catch (Throwable e) {
            var timeouts = getTimeout();
            var ex = e;
            while (ex!=null) {
                if(timeouts.containsKey(ex.getClass())){
                        return JobState.CONTINUE(e.getMessage(), timeouts.get(ex.getClass()));
                }
                ex = ex.getCause();
            }
            throw e;
        }

    }

}
