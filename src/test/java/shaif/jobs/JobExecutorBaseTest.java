package shaif.jobs;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.junit.*;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Базовый класс для интеграционных тестов.
 * Без Spring — только JdbcTemplate, TransactionManager и PostgreSQL.
 */
@Slf4j
public abstract class JobExecutorBaseTest {

    protected static final String DB_URL = System.getenv("JOB_EXECUTOR_DB_URL")
            != null ? System.getenv("JOB_EXECUTOR_DB_URL")
            : "jdbc:postgresql://localhost:5432/postgres";
    protected static final String DB_USER = System.getenv("JOB_EXECUTOR_DB_USER") != null
            ? System.getenv("JOB_EXECUTOR_DB_USER") : "postgres";
    protected static final String DB_PASSWORD = System.getenv("JOB_EXECUTOR_DB_PASSWORD") != null
            ? System.getenv("JOB_EXECUTOR_DB_PASSWORD") : "root";

    protected static String schemaName;
    protected static Connection dbConnection;
    protected static JdbcTemplate jt;
    protected static PlatformTransactionManager txManager;
    protected static JobExecutor jobExecutor;

    // Реальный GenericApplicationContext — хендлеры регистрируются через registerSingleton()
    protected static GenericApplicationContext springContext;

    static TransactionTemplate tt;

    @BeforeClass
    public static void setup() throws Exception {
        // Убираем отладочный лог от Spring JDBC
        for(var s: List.of("org.springframework.jdbc.core", "org.springframework.jdbc.datasource",
                "org.springframework.jdbc.datasource.DataSourceTransactionManager", "org.springframework.jdbc.support.JdbcUtils")) {
            Logger logger = (Logger) LoggerFactory.getLogger(s);
            if (logger != null) logger.setLevel(Level.WARN);
        }

        Class.forName("org.postgresql.Driver");
        dbConnection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        dbConnection.setAutoCommit(false);

        schemaName = "job_executor_test";
        try (Statement stmt = dbConnection.createStatement()) {
            stmt.execute("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
            stmt.execute("CREATE SCHEMA " + schemaName);
            stmt.execute(
                    "CREATE TABLE " + schemaName + ".job(\n" +
                    "  id bigint generated always as identity primary key,\n" +
                    "  name text not null,\n" +
                    "  parameters jsonb not null,\n" +
                    "  context jsonb not null,\n" +
                    "  is_done boolean not null default false,\n" +
                    "  is_failed boolean not null default false,\n" +
                    "  next_run_after timestamptz not null default now(),\n" +
                    "  status_message text,\n" +
                    "  parent_job_id bigint,\n" +
                    "  return_value jsonb\n" +
                    ")\n"
            );
            stmt.execute(
                    "CREATE TABLE " + schemaName + ".job_depends_on(\n" +
                    "  job_id bigint not null,\n" +
                    "  depends_on_job_id bigint not null check(depends_on_job_id<>job_id),\n" +
                    "  return_value jsonb,\n" +
                    "  primary key(job_id, depends_on_job_id),\n" +
                    "  unique(depends_on_job_id, job_id)\n" +
                    ")\n"
            );
            stmt.execute(
                    "CREATE UNIQUE INDEX ON " + schemaName + ".job((md5(name||parameters::text)))\n"
            );
        }
        dbConnection.commit();

        jt = new JdbcTemplate();
        //jt.setDataSource(new SingleConnectionDataSource(dbConnection, true));
        //SingleConnectionDataSource dataSource = new SingleConnectionDataSource(dbConnection, true);
        //DataSource dataSource = new DriverManagerDataSource(DB_URL, DB_USER, DB_PASSWORD);
        HikariDataSource dataSource = new HikariDataSource(); //new DriverManagerDataSource(DB_URLs = new HikariDataSource();
        dataSource.setJdbcUrl(DB_URL);
        dataSource.setUsername(DB_USER);
        dataSource.setPassword(DB_PASSWORD);
        dataSource.setMaximumPoolSize(5);
        jt.setDataSource(dataSource);

        DataSourceTransactionManager txManagerImpl = new DataSourceTransactionManager();
        txManagerImpl.setDataSource(dataSource);
        txManager = txManagerImpl;
        tt = new TransactionTemplate(txManager);

        // Создаём JobExecutor
        jobExecutor = new JobExecutor();
        jobExecutor.jt = jt;
        jobExecutor.transactionManager = txManager;
        jobExecutor.setThreadsCount(5);
        jobExecutor.setSchemaName(schemaName);
        jobExecutor.setJobNameFilter("true");

        // Создаём реальный GenericApplicationContext
        springContext = new GenericApplicationContext();

        // Регистрируем databaseCleanerJob (JobExecutor.init() ищет его)
        DatabaseCleanerJob cleanerJob = new DatabaseCleanerJob();
        JdbcTemplate cleanerJt = new JdbcTemplate();
        cleanerJt.setDataSource(new DriverManagerDataSource(DB_URL, DB_USER, DB_PASSWORD));
        cleanerJob.setJdbcTemplate(cleanerJt);
        cleanerJob.setBeanName("databaseCleanerJob");
        springContext.registerBean("databaseCleanerJob", DatabaseCleanerJob.class, () -> cleanerJob);

        // Регистрируем jobExecutor
        springContext.registerBean("jobExecutor", JobExecutor.class, () -> jobExecutor);
        jobExecutor.setApplicationContext(springContext);
        jobExecutor.init();
        springContext.refresh();
        Thread.sleep(500);
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (jobExecutor != null) {
            jobExecutor.shutdown();
            Thread.sleep(2000);
        }
        if (dbConnection != null && !dbConnection.isClosed()) {
            try (Statement stmt = dbConnection.createStatement()) {
                //stmt.execute("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
            }
            dbConnection.close();
        }
    }

    @Before
    public void resetContext() throws Exception {
        // Пересоздаём контекст
        springContext = new GenericApplicationContext();

        DatabaseCleanerJob cleanerJob = new DatabaseCleanerJob();
        JdbcTemplate cleanerJt = new JdbcTemplate();
        cleanerJt.setDataSource(new SingleConnectionDataSource(dbConnection, true));
        cleanerJob.setJdbcTemplate(cleanerJt);
        cleanerJob.setBeanName("databaseCleanerJob");
        springContext.registerBean("databaseCleanerJob", DatabaseCleanerJob.class, () -> cleanerJob);

        // Регистрируем jobExecutor
        springContext.registerBean("jobExecutor", JobExecutor.class, () -> jobExecutor);
        springContext.refresh();
        jobExecutor.setApplicationContext(springContext);

        jobExecutor.init();
        Thread.sleep(500);
    }

    // --- Хелперы ---

    protected void registerHandler(JobHandler handler) {
        // Добавляем в кэш синглтонов напрямую
        springContext.getBeanFactory().registerSingleton(handler.getBeanName(), handler);
    }

    protected JobHandler getHandler(String name) {
        return (JobHandler) springContext.getBean(name);
    }

    protected JobExecution submitAndWait(JobHandler handler, Object params, long timeoutMs) throws Exception {
        registerHandler(handler);

        Long jobId = jobExecutor.submit(handler, params);
        assertNotNull(jobId);
        assertTrue(jobId > 0);

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            JobState state = jobExecutor.getOptionalJobState(jobId).orElse(null);
            if (state != null && (state.getStatus() == JobState.Status.DONE
                    || state.getStatus() == JobState.Status.ABORT
                    || state.getStatus() == JobState.Status.STOP)) {
                break;
            }
            Thread.sleep(100);
        }
        return new JobExecution(jobId, jobExecutor);
    }

    protected void assertJobDone(JobExecution execution) {
        assertEquals(JobState.Status.DONE, execution.getJobState().getStatus());
    }

    protected void assertJobFailed(JobExecution execution) {
        JobState state = execution.getJobState();
        assertTrue(state.getStatus() == JobState.Status.ABORT || state.getStatus() == JobState.Status.STOP);
    }

    protected JobHandler createHandler(String name, JobState... states) {
        return new JobHandler() {
            private final String thisName = name;
            private int index = 0;

            @Override public void setBeanName(String n) {}
            @Override public String getBeanName() { return thisName; }
            @Override public JobState execute(Job job) {
                log.info("Job executing: {}", job.getName());
                return states[Math.min(index++, states.length - 1)];
            }
        };
    }

    protected JobHandler createFailingHandler(String name, Exception e) {
        return new JobHandler() {
            private final String thisName = name;

            @Override public void setBeanName(String n) {}
            @Override public String getBeanName() { return thisName; }
            @Override public JobState execute(Job job) throws Exception {
                if (e != null) throw e;
                return JobState.DONE("done");
            }
        };
    }
}
