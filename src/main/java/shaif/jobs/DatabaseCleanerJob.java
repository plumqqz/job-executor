package shaif.jobs;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

@Slf4j
@Data
@Service
@Lazy
public class DatabaseCleanerJob implements JobHandler, BeanNameAware {
    String beanName;

    @Autowired
    @ToString.Exclude
    JdbcTemplate jdbcTemplate;

    @Override
    public JobState execute(Job job) {
        jdbcTemplate.update(job.getJobExecutor().getClearJobDependsOnQry());
        jdbcTemplate.update(job.getJobExecutor().getClearJobQry());
        return JobState.CONTINUE("Cleanup done at " + Instant.now().toString(), Duration.ofMinutes(15));
    }

}
