package shaif.jobs;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@Data
@Service
@Lazy
public class DatabaseCleanerJob implements JobHandler{
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
