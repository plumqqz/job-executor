package shaif.jobs;

import lombok.Data;

import java.time.Instant;
import java.time.LocalDateTime;

public interface BackgroundJobHandler extends JobHandler{
    @Data
    public static class Parameters{
        String jobExecutorName;
    }
}
