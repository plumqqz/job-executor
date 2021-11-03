import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import shaif.jobs.GenericJobHandler;
import shaif.jobs.Job;
import shaif.jobs.JobExecutor;
import shaif.jobs.JobState;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;

@Slf4j
public class TestJobs {

    @Slf4j
    public static class TestGJob extends GenericJobHandler<TestGJob.Parameters, TestGJob.Context>{
        public static class Parameters{
            String parameter;
        }
        public static class Context{
            String context;
        }

        @Override
        public JobState execute(Job job, Parameters parameters, Context context) {
            log.info("Parameter parameter:{}", parameters.parameter);
            return JobState.DONE("Done");
        }
    }
    @Test
    public void testGenericJob() throws NoSuchFieldException {
        TestGJob testGJob = new TestGJob();
        log.info("Declared field:{}", (Class<?>)((ParameterizedType)testGJob.getClass().getGenericSuperclass()).getActualTypeArguments()[1]);


    }
}
