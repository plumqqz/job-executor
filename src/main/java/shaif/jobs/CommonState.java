package shaif.jobs;

import lombok.experimental.UtilityClass;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@UtilityClass
public class CommonState {
    static final AtomicInteger activeCount = new AtomicInteger(0);
    static final AtomicLong workerThreadId = new AtomicLong(-1);
}
