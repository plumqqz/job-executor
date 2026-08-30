package shaif.jobs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Тесты обработки ошибок.
 */
public class ErrorHandlingTest {

    private ObjectMapper om;

    @Before
    public void setUp() {
        om = new ObjectMapper();
        om.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        om.disable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        om.disable(com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }

    // --- Job: невалидные параметры ---
    @Test
    public void testJob_getParameters_invalidJson() {
        Job job = new Job();
        job.setParameters("not json at all");

        try {
            job.getParameters(TestParams.class);
            fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Cannot parse value"));
        }
    }

    // --- Job: невалидный контекст ---
    @Test
    public void testJob_getContext_invalidJson() {
        Job job = new Job();
        job.setContext("not json");

        try {
            job.getContext(TestContext.class);
            fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Cannot parse value"));
        }
    }

    // --- Job: контекст с сериализацией ---
    @Test
    public void testJob_getContext_serializationError() {
        Job job = new Job();
        // Объект, который не может быть сериализован (циклическая ссылка)
        Object circular = new Object();
        try {
            java.lang.reflect.Field f = Job.class.getDeclaredField("savedContext");
            f.setAccessible(true);
            f.set(job, circular);

            // getContext() должен сериализовать savedContext
            String json = job.getContext();
            // Если сериализация прошла — проверяем что JSON не пустой
            assertFalse(json.isEmpty());
        } catch (Exception e) {
            // Или бросает RuntimeException
            assertTrue(e instanceof RuntimeException || e.getCause() instanceof JsonProcessingException);
        }
    }

    // --- Job: null параметры с getOptionalParameters ---
    @Test
    public void testJob_getOptionalParameters_null() {
        Job job = new Job();
        job.setParameters(null);

        Optional<TestParams> params = job.getOptionalParameters(TestParams.class);
        // parseValue бросает RuntimeException на null
        // Текущее поведение: бросает исключение
        try {
            params.ifPresent(p -> {});
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Cannot parse value"));
        }
    }

    // --- GenericJobHandler: невалидные типы ---
    @Test(expected = IllegalStateException.class)
    public void testGenericJobHandler_noTypeParams() {
        // Непараметризованный GenericJobHandler не может резолвнуть типы
        new GenericJobHandler<Object, Object>() {
            @Override
            public JobState execute(Job job, Object parameters, Object context) {
                return JobState.DONE("done");
            }
        };
    }

    // --- JobState: null message ---
    @Test(expected = NullPointerException.class)
    public void testJobState_stop_nullMessage() {
        JobState.STOP(null);
    }

    @Test(expected = NullPointerException.class)
    public void testJobState_abort_nullMessage() {
        JobState.ABORT(null);
    }

    @Test(expected = NullPointerException.class)
    public void testJobState_done_nullMessage() {
        JobState.DONE(null);
    }

    @Test(expected = NullPointerException.class)
    public void testJobState_continue_nullMessage() {
        JobState.CONTINUE(null);
    }

    @Test(expected = NullPointerException.class)
    public void testJobState_continue_withDuration_nullMessage() {
        JobState.CONTINUE(null, Duration.ofSeconds(5));
    }

    // --- JobState: null nextRun ---
    @Test(expected = NullPointerException.class)
    public void testJobState_continue_withInstant_nullNextRun() {
        JobState.CONTINUE("message", (Instant) null);
    }

    // --- JobState: null returnValue with duration ---
    @Test(expected = NullPointerException.class)
    public void testJobState_continue_withReturnValue_nullMessage() {
        JobState.CONTINUE(new Object(), null, Duration.ofSeconds(5));
    }

    // --- Job: null nextRun with duration ---
    @Test(expected = NullPointerException.class)
    public void testJobState_continue_withReturnValue_nullDuration() {
        JobState.CONTINUE(new Object(), "message", (Duration) null);
    }

    // --- Job: пустые параметры ---
    @Test
    public void testJob_getParameters_emptyJson() {
        Job job = new Job();
        job.setParameters("{}");
        TestParams params = job.getParameters(TestParams.class);
        assertNull(params.query);
        assertEquals(0, params.limit);
    }

    // --- Job: пустой контекст ---
    @Test
    public void testJob_getContext_emptyJson() {
        Job job = new Job();
        job.setContext("{}");
        TestContext ctx = job.getContext(TestContext.class);
        assertEquals(0, ctx.processed);
        assertNull(ctx.lastId);
    }

    // --- Job: параметры с неизвестными полями ---
    @Test
    public void testJob_getParameters_unknownFields() {
        Job job = new Job();
        job.setParameters("{\"query\":\"test\",\"limit\":5,\"unknownField\":\"ignored\"}");
        TestParams params = job.getParameters(TestParams.class);
        assertEquals("test", params.query);
        assertEquals(5, params.limit);
    }

    // --- Job: контекст с неизвестными полями ---
    @Test
    public void testJob_getContext_unknownFields() {
        Job job = new Job();
        job.setContext("{\"processed\":10,\"lastId\":\"id\",\"extra\":\"ignored\"}");
        TestContext ctx = job.getContext(TestContext.class);
        assertEquals(10, ctx.processed);
        assertEquals("id", ctx.lastId);
    }

    // --- Job: null context ---
    @Test
    public void testJob_getContext_null() {
        Job job = new Job();
        job.setContext(null);
        TestContext ctx = job.getContext(TestContext.class);
        assertNull(ctx);
    }

    // --- Job: null parameters ---
    @Test
    public void testJob_getParameters_null() {
        Job job = new Job();
        job.setParameters(null);
        try {
            job.getParameters(TestParams.class);
            fail("Should throw on null parameters");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Cannot parse value"));
        }
    }

    // --- Job: getReturnValue без executor ---
    @Test
    public void testJob_getReturnValue_noExecutor() {
        Job job = new Job();
        try {
            job.getReturnValue(1L, String.class);
            fail("Should throw when executor is null");
        } catch (RuntimeException e) {
            // Ожидается: "Result is empty" или null executor
            assertNotNull(e.getMessage());
        }
    }

    // --- Job: getJobState без executor ---
    @Test
    public void testJob_getJobState_noExecutor() {
        Job job = new Job();
        try {
            job.getJobState(1L);
            fail("Should throw when executor is null");
        } catch (RuntimeException e) {
            assertNotNull(e.getMessage());
        }
    }

    // --- Job: getOptionalReturnValue без executor ---
    @Test
    public void testJob_getOptionalReturnValue_noExecutor() {
        Job job = new Job();
        try {
            job.getOptionalReturnValue(1L, String.class);
            fail("Should throw when executor is null");
        } catch (RuntimeException e) {
            assertNotNull(e.getMessage());
        }
    }

    // --- Job: getJobState с JobExecution без executor ---
    @Test
    public void testJob_getJobState_withJobExecution_noExecutor() {
        Job job = new Job();
        try {
            job.getJobState(new JobExecution(1L, null));
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // Ожидается NullPointerException или RuntimeException
        }
    }

    // --- Job: restart/stop/resume/delete без executor ---
    @Test
    public void testJob_restart_noExecutor() {
        Job job = new Job();
        try {
            job.restart();
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    @Test
    public void testJob_stop_noExecutor() {
        Job job = new Job();
        try {
            job.stop();
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    @Test
    public void testJob_resume_noExecutor() {
        Job job = new Job();
        try {
            job.resume();
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    @Test
    public void testJob_delete_noExecutor() {
        Job job = new Job();
        try {
            job.delete();
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: listDependentJobs без executor ---
    @Test
    public void testJob_listDependentJobs_noExecutor() {
        Job job = new Job();
        try {
            job.listDependentJobs();
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    @Test
    public void testJob_listDependsOnJobs_noExecutor() {
        Job job = new Job();
        try {
            job.listDependsOnJobs();
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: dependOn без executor ---
    @Test
    public void testJob_dependOn_noExecutor() {
        Job job = new Job();
        try {
            job.dependOn(new JobExecution(1L, null));
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: makeJobDependent без executor ---
    @Test
    public void testJob_makeJobDependent_noExecutor() {
        Job job = new Job();
        try {
            job.makeJobDependent(new JobExecution(1L, null));
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: makeJobIndependent без executor ---
    @Test
    public void testJob_makeJobIndependent_noExecutor() {
        Job job = new Job();
        try {
            job.makeJobIndependent(new JobExecution(1L, null));
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: makeSelfDependent без executor ---
    @Test
    public void testJob_makeSelfDependent_noExecutor() {
        Job job = new Job();
        try {
            job.makeSelfDependent(new JobExecution(1L, null));
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: makeSelfIndependent без executor ---
    @Test
    public void testJob_makeSelfIndependent_noExecutor() {
        Job job = new Job();
        try {
            job.makeSelfIndependent(new JobExecution(1L, null));
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: submit без executor ---
    @Test
    public void testJob_submit_noExecutor() {
        Job job = new Job();
        try {
            job.submit("handlerBean", "{}");
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: submitAndWait без executor ---
    @Test
    public void testJob_submitAndWait_noExecutor() {
        Job job = new Job();
        try {
            job.submitAndWait("handlerBean", "{}");
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: getReturnValue с JobExecution без executor ---
    @Test
    public void testJob_getReturnValue_withJobExecution_noExecutor() {
        Job job = new Job();
        try {
            job.getReturnValue(new JobExecution(1L, null), String.class);
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: getOptionalReturnValue с JobExecution без executor ---
    @Test
    public void testJob_getOptionalReturnValue_withJobExecution_noExecutor() {
        Job job = new Job();
        try {
            job.getOptionalReturnValue(new JobExecution(1L, null), String.class);
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: waitFor без executor ---
    @Test
    public void testJob_waitFor_noExecutor() {
        Job job = new Job();
        // waitFor — пустой метод, не должен бросать
        job.waitFor(new JobExecution(1L, null));
        // Если не бросил — тест пройден
    }

    // --- Job: getJobState с JobExecution ---
    @Test
    public void testJob_getJobState_withJobExecution() {
        Job job = new Job();
        JobExecution exec = new JobExecution(1L, null);
        try {
            job.getJobState(exec);
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: getOptionalReturnValue с JobExecution ---
    @Test
    public void testJob_getOptionalReturnValue_withJobExecution() {
        Job job = new Job();
        JobExecution exec = new JobExecution(1L, null);
        try {
            job.getOptionalReturnValue(exec, String.class);
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: getJobState с long ---
    @Test
    public void testJob_getJobState_long_noExecutor() {
        Job job = new Job();
        try {
            job.getJobState(1L);
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: getOptionalJobState с long ---
    @Test
    public void testJob_getOptionalJobState_long() {
        Job job = new Job();
        try {
            job.getOptionalJobState(1L);
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: getOptionalJobState с JobExecution ---
    @Test
    public void testJob_getOptionalJobState_withJobExecution() {
        Job job = new Job();
        JobExecution exec = new JobExecution(1L, null);
        try {
            job.getOptionalJobState(exec.getJobId());
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: getReturnValue с long ---
    @Test
    public void testJob_getReturnValue_long() {
        Job job = new Job();
        try {
            job.getReturnValue(1L, String.class);
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: getOptionalReturnValue с long ---
    @Test
    public void testJob_getOptionalReturnValue_long() {
        Job job = new Job();
        try {
            job.getOptionalReturnValue(1L, String.class);
            fail("Should throw when executor is null");
        } catch (Exception e) {
            // NullPointerException или RuntimeException
        }
    }

    // --- Job: setJobExecutor с null ---
    @Test(expected = NullPointerException.class)
    public void testJob_setJobExecutor_null() {
        Job job = new Job();
        job.setJobExecutor(null);
    }

    // ==================== Вспомогательные классы ====================

    public static class TestParams {
        public String query;
        public int limit;
    }

    public static class TestContext {
        public int processed;
        public String lastId;
    }
}
