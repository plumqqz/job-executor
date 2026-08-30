package shaif.jobs;

import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.*;

public class JobStateTest {

    @Test
    public void testStop() {
        JobState state = JobState.STOP("stopped");
        assertEquals(JobState.Status.STOP, state.getStatus());
        assertEquals("stopped", state.getMessage());
        assertNull(state.getReturnValue());
    }

    @Test
    public void testAbort() {
        JobState state = JobState.ABORT("aborted");
        assertEquals(JobState.Status.ABORT, state.getStatus());
        assertEquals("aborted", state.getMessage());
    }

    @Test
    public void testDone_noReturnValue() {
        JobState state = JobState.DONE("all good");
        assertEquals(JobState.Status.DONE, state.getStatus());
        assertEquals("all good", state.getMessage());
        assertNull(state.getReturnValue());
    }

    @Test
    public void testDone_withReturnValue() {
        Object rv = new Object();
        JobState state = JobState.DONE(rv, "completed");
        assertEquals(JobState.Status.DONE, state.getStatus());
        assertEquals("completed", state.getMessage());
        assertSame(rv, state.getReturnValue());
    }

    @Test
    public void testContinue_immediate() {
        Instant before = Instant.now();
        JobState state = JobState.CONTINUE("keep going");
        assertEquals(JobState.Status.CONTINUE, state.getStatus());
        assertEquals("keep going", state.getMessage());
        assertTrue(state.getNextRun().isAfter(before) || state.getNextRun().equals(before));
    }

    @Test
    public void testContinue_withInstant() {
        Instant future = Instant.now().plusSeconds(30);
        JobState state = JobState.CONTINUE("wait 30s", future);
        assertEquals(JobState.Status.CONTINUE, state.getStatus());
        assertEquals("wait 30s", state.getMessage());
        assertEquals(future, state.getNextRun());
    }

    @Test
    public void testContinue_withDuration() {
        Instant before = Instant.now();
        JobState state = JobState.CONTINUE("wait 5s", Duration.ofSeconds(5));
        assertEquals(JobState.Status.CONTINUE, state.getStatus());
        assertEquals("wait 5s", state.getMessage());
        assertTrue(state.getNextRun().isAfter(before));
    }

    @Test
    public void testContinue_withReturnValueAndDuration() {
        Object rv = new Object();
        JobState state = JobState.CONTINUE(rv, "partial", Duration.ofSeconds(10));
        assertEquals(JobState.Status.CONTINUE, state.getStatus());
        assertEquals("partial", state.getMessage());
        assertSame(rv, state.getReturnValue());
        assertTrue(state.getNextRun().isAfter(Instant.now()));
    }

    @Test
    public void testContinue_withReturnValueAndInstant() {
        Object rv = new Object();
        Instant future = Instant.now().plusSeconds(60);
        JobState state = JobState.CONTINUE(rv, "scheduled", future);
        assertEquals(JobState.Status.CONTINUE, state.getStatus());
        assertSame(rv, state.getReturnValue());
        assertEquals(future, state.getNextRun());
    }

    @Test
    public void testDefaultMessage() {
        JobState state = new JobState();
        state.status = JobState.Status.DONE;
        assertEquals("Unknown error", state.getMessage());
    }
}
