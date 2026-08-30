package shaif.jobs;

import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Интеграционные тесты JobExecutor.
 * Требует работающий PostgreSQL.
 *
 * Переменные окружения:
 *   JOB_EXECUTOR_DB_URL=jdbc:postgresql://localhost:5432/postgres
 *   JOB_EXECUTOR_DB_USER=postgres
 *   JOB_EXECUTOR_DB_PASSWORD=postgres
 */
public class JobExecutorTest extends JobExecutorBaseTest {

    @Test
    public void testSubmitDoneJob() throws Exception {
        JobHandler handler = createHandler("testDoneHandler", JobState.DONE("completed successfully"));
        JobExecution execution = submitAndWait(handler, "{}", 5000);
        assertJobDone(execution);

        JobState state = jobExecutor.getOptionalJobState(execution.getJobId()).orElse(null);
        assertNotNull(state);
        assertEquals(JobState.Status.DONE, state.getStatus());
        assertEquals("completed successfully", state.getMessage());
    }

    @Test
    public void testSubmitContinueJob() throws Exception {
        JobHandler handler = createHandler("testContinueHandler",
                JobState.CONTINUE("keep going", Duration.ofMillis(200)),
                JobState.DONE("finally done")
        );
        JobExecution execution = submitAndWait(handler, "{}", 5000);
        assertJobDone(execution);

        JobState state = jobExecutor.getOptionalJobState(execution.getJobId()).orElse(null);
        assertNotNull(state);
        assertEquals(JobState.Status.DONE, state.getStatus());
        assertEquals("finally done", state.getMessage());
    }

    @Test
    public void testSubmitAbortJob() throws Exception {
        JobHandler handler = createHandler("testAbortHandler", JobState.ABORT("aborting"));
        JobExecution execution = submitAndWait(handler, "{}", 5000);
        assertJobFailed(execution);

        JobState state = jobExecutor.getOptionalJobState(execution.getJobId()).orElse(null);
        assertNotNull(state);
        assertEquals(JobState.Status.ABORT, state.getStatus());
        assertEquals("aborting", state.getMessage());
    }

    @Test
    public void testSubmitStopJob() throws Exception {
        JobHandler handler = createHandler("testStopHandler", JobState.STOP("stopping gracefully"));
        JobExecution execution = submitAndWait(handler, "{}", 5000);
        assertJobFailed(execution);

        JobState state = jobExecutor.getOptionalJobState(execution.getJobId()).orElse(null);
        assertNotNull(state);
        assertEquals(JobState.Status.STOP, state.getStatus());
        assertEquals("stopping gracefully", state.getMessage());
    }

    @Test
    public void testSubmitWithReturnValue() throws Exception {
        JobHandler handler = createHandler("testReturnValueHandler",
                JobState.DONE("{\"result\":\"success\"}", "done with data")
        );
        JobExecution execution = submitAndWait(handler, "{}", 5000);
        assertJobDone(execution);

        Optional<String> returnValue = jobExecutor.getOptionalReturnValue(
                execution.getJobId(), String.class);
        assertTrue(returnValue.isPresent());
        assertEquals("{\"result\":\"success\"}", returnValue.get());
    }

    @Test
    public void testSubmitWithParameters() throws Exception {
        String params = "{\"query\":\"SELECT 1\",\"limit\":42}";
        JobHandler handler = createHandler("testParamsHandler", JobState.DONE("params received"));

        JobExecution execution = submitAndWait(handler, params, 5000);
        assertJobDone(execution);
    }

    @Test
    public void testSubmitWithException() throws Exception {
        JobHandler handler = createFailingHandler("testFailingHandler", new RuntimeException("boom!"));
        JobExecution execution = submitAndWait(handler, "{}", 5000);
        assertJobFailed(execution);

        JobState state = jobExecutor.getOptionalJobState(execution.getJobId()).orElse(null);
        assertNotNull(state);
        assertTrue(state.getMessage().contains("boom"));
    }

    @Test
    public void testRestartJob() throws Exception {
        JobHandler handler = createFailingHandler("testRestartHandler", new RuntimeException("initial fail"));
        JobExecution execution = submitAndWait(handler, "{}", 5000);
        assertJobFailed(execution);

        JobState before = jobExecutor.getOptionalJobState(execution.getJobId()).orElse(null);
        assertNotNull(before);
        assertTrue(before.getStatus() == JobState.Status.ABORT || before.getStatus() == JobState.Status.STOP);

        jobExecutor.restartJob(execution.getJobId());
        JobState after = jobExecutor.getOptionalJobState(execution.getJobId()).orElse(null);
        assertNotNull(after);
        assertEquals(JobState.Status.CONTINUE, after.getStatus());
    }

    @Test
    public void testStopJob() throws Exception {
        JobHandler handler = createHandler("testStopJobHandler", JobState.DONE("done"));
        JobExecution execution = submitAndWait(handler, "{}", 5000);
        assertJobDone(execution);

        jobExecutor.stopJob(execution.getJobId());
    }

    @Test
    public void testDeleteJob() throws Exception {
        JobHandler handler = createHandler("testDeleteHandler", JobState.DONE("to be deleted"));
        JobExecution execution = submitAndWait(handler, "{}", 5000);
        assertJobDone(execution);

        jobExecutor.deleteJob(execution.getJobId());

        Optional<JobState> state = jobExecutor.getOptionalJobState(execution.getJobId());
        assertTrue(state.isEmpty());
    }

    @Test
    public void testRunJob() throws Exception {
        JobHandler handler = createHandler("testRunJobHandler", JobState.DONE("ran immediately"));
        JobExecution execution = submitAndWait(handler, "{}", 5000);
        assertJobDone(execution);

        jobExecutor.runJob(execution.getJobId());
    }

    @Test
    public void testGetOptionalReturnValue_nonExistent() {
        try {
            jobExecutor.getOptionalReturnValue(99999L, String.class);
            fail("Should throw NoJobFoundException");
        } catch (NoJobFoundException e) {
            // expected
        }
    }

    @Test
    public void testGetOptionalJobState_nonExistent() {
        Optional<JobState> state = jobExecutor.getOptionalJobState(99999L);
        assertTrue(state.isEmpty());
    }

    @Test
    public void testJobDependencies() throws Exception {
        JobHandler handlerA = createHandler("testDepA", JobState.DONE("A done"));
        JobHandler handlerB = createHandler("testDepB", JobState.DONE("B done"));

        Long jobIdA = jobExecutor.submit(handlerA, "{}");
        Long jobIdB = jobExecutor.submit(handlerB, "{}");

        jobExecutor.dependOn(jobIdB, jobIdA);

        submitAndWait(handlerA, "{}", 5000);

        List<JobExecution> dependents = jobExecutor.listDependentJobs(jobIdA);
        assertTrue("B should depend on A", dependents.stream()
                .anyMatch(j -> j.getJobId() == jobIdB));

        List<JobExecution> dependsOn = jobExecutor.listDependsOnJobs(jobIdB);
        assertTrue("A should be in B's dependsOn", dependsOn.stream()
                .anyMatch(j -> j.getJobId() == jobIdA));
    }

    @Test
    public void testIndependOn() throws Exception {
        JobHandler handler1 = createHandler("testIndepOn1", JobState.DONE("done"));
        JobHandler handler2 = createHandler("testIndepOn2", JobState.DONE("done"));

        Long jobIdA = jobExecutor.submit(handler1, "{}");
        Long jobIdB = jobExecutor.submit(handler2, "{}");

        jobExecutor.dependOn(jobIdB, jobIdA);
        jobExecutor.independOn(jobIdB, jobIdA);

        List<JobExecution> dependsOn = jobExecutor.listDependsOnJobs(jobIdB);
        assertFalse("A should not be in B's dependsOn", dependsOn.stream()
                .anyMatch(j -> j.getJobId() == jobIdA));
    }

    @Test
    public void testGetJobIdsByName() throws Exception {
        JobHandler handler = createHandler("namedTestHandler", JobState.DONE("done"));

        jobExecutor.submit(handler, "{}");

        List<Long> ids = jobExecutor.getJobIdsByName("namedTestHandler");
        assertTrue(ids.size() ==1);
    }

    @Test
    public void testGetJobById() throws Exception {
        JobHandler handler = createHandler("testGetById", JobState.DONE("done"));

        Long jobId = jobExecutor.submit(handler, "{}");
        Job job = jobExecutor.getJobById(jobId);

        assertNotNull(job);
        assertEquals(jobId, job.getId());
        assertNotNull(job.getName());
        assertNotNull(job.getParameters());
    }

    @Test
    public void testGetJobById_nonExistent() {
        Job job = jobExecutor.getJobById(99999L);
        assertNull(job);
    }

    @Test
    public void testStopAllJobs() throws Exception {
        JobHandler handler = createHandler("stopAllHandler", JobState.DONE("done"));

        jobExecutor.submit(handler, "{}");
        jobExecutor.submit(handler, "{}");

        List<Long> ids = jobExecutor.getJobIdsByName("stopAllHandler");
        jobExecutor.stopAllJObs(handler);

        for (Long id : ids) {
            Optional<JobState> state = jobExecutor.getOptionalJobState(id);
            assertTrue(state.isPresent());
        }
    }
}
