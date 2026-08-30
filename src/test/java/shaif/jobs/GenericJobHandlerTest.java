package shaif.jobs;

import org.junit.Test;

import static org.junit.Assert.*;

public class GenericJobHandlerTest {

    // --- тестовые DTO ---
    public static class TestParams {
        public int count;
        public String name;
    }
    public static class TestContext {
        public int iterations;
        public String lastResult;
    }
    public static class TestResult {
        public String output;
    }

    // --- тестовый хендлер ---
    public static class SimpleTestHandler extends GenericJobHandler<TestParams, TestContext> {
        @Override
        public JobState execute(Job job, TestParams parameters, TestContext context) {
            return JobState.DONE("done");
        }
    }

    // --- хендлер без параметризации (должен фейлиться) ---
    public static class NonParameterizedHandler extends GenericJobHandler {
        @Override
        public JobState execute(Job job, Object p, Object c) {
            return JobState.DONE("done");
        }
    }

    @Test
    public void testTypeResolution_params() {
        SimpleTestHandler handler = new SimpleTestHandler();
        assertEquals(TestParams.class, handler.pClass);
    }

    @Test
    public void testTypeResolution_context() {
        SimpleTestHandler handler = new SimpleTestHandler();
        assertEquals(TestContext.class, handler.cClass);
    }

    @Test
    public void testExecute_delegatesToRealExecute() throws Exception {
        SimpleTestHandler handler = new SimpleTestHandler() {
            @Override
            public JobState execute(Job job, TestParams parameters, TestContext context) {
                assertEquals(5, parameters.count);
                assertEquals("test", parameters.name);
                assertEquals(1, context.iterations);
                return JobState.DONE("custom result");
            }
        };

        Job job = new Job();
        job.setParameters("{\"count\":5,\"name\":\"test\"}");
        job.setContext("{\"iterations\":1,\"lastResult\":null}");

        JobState state = handler.execute(job);
        assertEquals(JobState.Status.DONE, state.getStatus());
        assertEquals("custom result", state.getMessage());
    }

    @Test
    public void testExecute_nullParameters() throws Exception {
        SimpleTestHandler handler = new SimpleTestHandler() {
            @Override
            public JobState execute(Job job, TestParams parameters, TestContext context) {
                return JobState.DONE("ok");
            }
        };

        Job job = new Job();
        job.setParameters(null);
        job.setContext("{}");

        // Jackson вернёт null для null-параметров — это ожидаемо
        JobState state = handler.execute(job);
        assertEquals(JobState.Status.DONE, state.getStatus());
    }

    @Test
    public void testExecute_nullContext() throws Exception {
        SimpleTestHandler handler = new SimpleTestHandler() {
            @Override
            public JobState execute(Job job, TestParams parameters, TestContext context) {
                return JobState.DONE("ok");
            }
        };

        Job job = new Job();
        job.setParameters("{}");
        job.setContext(null);

        JobState state = handler.execute(job);
        assertEquals(JobState.Status.DONE, state.getStatus());
    }

    @Test
    public void testBeanName() {
        SimpleTestHandler handler = new SimpleTestHandler();
        handler.setBeanName("myTestHandler");
        assertEquals("myTestHandler", handler.getBeanName());
    }

    @Test(expected = IllegalStateException.class)
    public void testNonParameterizedThrows() {
        // Непараметризованный дженерик-класс не может резолвнуть типы
        new NonParameterizedHandler();
    }
}
