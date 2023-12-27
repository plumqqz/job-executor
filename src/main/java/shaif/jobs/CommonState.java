package shaif.jobs;

import lombok.experimental.UtilityClass;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@UtilityClass
public class CommonState {
    static final AtomicReference<Thread> workerThread = new AtomicReference<>();
}
