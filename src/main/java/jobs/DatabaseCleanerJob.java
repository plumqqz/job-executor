package jobs;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

@Slf4j
@Data
@Service
public class DatabaseCleanerJob implements JobHandler{
    String beanName;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    JobExecutor jobExecutor;

    @Override
    public JobState execute(Job job) {
        jdbcTemplate.update(jobExecutor.clearJobDependsOnQry);
        jdbcTemplate.update(jobExecutor.clearJobQry);
        return JobState.CONTINUE("Cleanup done at " + Instant.now().toString(), Duration.ofMinutes(15));
    }

}
