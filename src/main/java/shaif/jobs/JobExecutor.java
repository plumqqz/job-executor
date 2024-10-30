package shaif.jobs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParserContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/*

create schema tsy;
create table tsy.job(
 id bigint generated always as identity primary key,
 name text not null,
 parameters jsonb not null,
 context jsonb not null,
 is_done boolean not null default false,
 is_failed boolean not null default false,
 next_run_after timestamptz not null default now(),
 status_message text,
 parent_job_id bigint,
 return_value jsonb
);
create table tsy.job_depends_on(
 job_id bigint not null references tsy.job(id),
 depends_on_job_id bigint not null check(depends_on_job_id<>job_id),
 return_value jsonb
);
create unique index on tsy.job((md5(name||parameters::text)));

insert into tsy.job(name, parameters,context)values('pollerJob','{"repeatCount":4}'::jsonb,'{}'::jsonb) returning id;
insert into tsy.job(name, parameters,context)values('pollerJob','{"repeatCount":4}'::jsonb,'{}'::jsonb);
insert into tsy.job_depends_on(job_id, depends_on_job_id) values(106,107);

with recursive tq as(
select 1 as level, to_char(id, '000000000') as path, * from tsy.job where parent_job_id is null
union all
select tq.level+1, tq.path||to_char(job.id, '000000000'), job.* from tsy.job, tq where job.parent_job_id=tq.id)
select id,
  repeat(' ',level) || name as name,
  case when is_done then 'DONE'
       when is_failed then 'FAILED'
       when next_run_after>now() then 'WAIT ' ||(next_run_after-now())
       else 'ACTIVE'
  end || coalesce((select ', depends on ' ||string_agg('#'||j.id,'') from tsy.job_depends_on jdo, tsy.job j where jdo.job_id=tq.id and jdo.depends_on_job_id=j.id and not j.is_done),'')
  as job_state,
  status_message
from tq order by path

*/

/**
 * Сервис для выполнения связанных заданий.
 * Идея простая:
 * <ul>
 *  <li>Для каждой строки в спецтаблице в беспокнечном цикле вызывается метод execute бина, чье имя указано в таблице name</li>
 *  <li>У этого задания есть параметры и контекст выполнения, который можно изменять в методе execute</li>
 *  <li>Выполнение останавливается тогда, когда метод вернет специальное значени</li>
 *  </ul>
 *
 *  Каждый вызов execute происходит в транзакции.
 *
 *  Таким образом получаются следующие сущности
 *  <ul>
 *  <li>Метод объекта, взаимодействующий с БД и использующий соответствующие бины {@link JobExecutor}</li>
 *  <li>Бин, обрабатывающий задания {@link JobHandler}</li>
 *  <li>Выполняющееся задание {@link Job}</li>
 *  <li>Ссылка на выполняющееся задание {@link JobExecution}</li>
 *  <ul><li>Состояние выполняющегося задания {@link JobState}</li></ul>
 *  <li>Параметры выполняющегося задания - некоторый класс</li>
 *  <li>Контекст выполняющегося задания - некоторый класс, который сериализуеся после очередного вызова execute и дресериализуется перед вызовом.
 *  Он позволяет сохранять состояния между вызовами</li>
 *  <li>Результат работы задания - например, задание может опращивать несколько сторонних веб-сервисов и собирать их ответы
 *  в единый результат, который и возвращает</li>
 *  </ul>
 *
 *  Метод может возвращать значения
 *  <ul>
 *      <li><code>ABORT</code> - остановка выполнения задания, больше execute вызываться не будет. Транзакция откатывается</li>
 *      <li><code>STOP</code> - то же самое, что и <code>ABORT</code>, но при этом сохраняется контекст. Транзакция откатывается</li>
 *      <li><code>DONE</code> - окончание выполнения задания</li>
 *      <li><code>CONTINUE</code> - продолжение выполнения задания</li>*
 *  </ul>
 *
 *  Для каждого состояния необходимо указывать текствов сообщение, описывающее состояние задания.
 *
 *  Для <code>DONE</code> можно указывать возвращаемое значение. Для <code>CONTINUE</code> - время ожидания перед следующим выполнением.
 *
 *  Параметры, контекст и возвращаемое значение сериализуются/десереиализуются с помощью Jackson.
 *
 *  Задания могут зависеть друг от друга - если задание А зависит от задания Б, то выполнение задания А начнется
 *  только после окончания задания Б.
 *
 *  Задание может вернуть значение. Это значение можно получить в других заданиях.
 *
 * Задания берутся из таблицы job, которые not is_done & not is_failed, и которые не ожидают окончания других заданий.
 * name - это имя сприрговского бина, который обрабатывает задание и должен реализовывать интерфейс JobHandler.
 * Этот сервис работает только под Spring.
 * Каждый класс, соответствуюзщий заданию, должен иметь вид:
 * <pre>
 * @ Service // для Спринга
 * @ Slf4j // log точно будет нужен
 * @ Data // для реализации BeanNameAware
 * public class PollerJob implements JobHandler {
 *      String beanName; // для реализации BeanNameAware
 *
 *     public JobResult execute(Job job) {
 *         MyJobParameters params = job.getParameters(MyJobParameters.class);
 *         log.info("Count:{}/jobId:"+job.getId(), params.getRepeatCount());
 *
 *         MyJobContext ctx = job.getContext(MyJobContext.class);
 *         ... что-то делаем ...
 *         ctx.setProperty(valueOfProperty);
 *         if(...) return JobResult.CONTINUE("Continue working");
 *         return JobResult.DONE(new MyJobReturnValue(....), "Message");
 * </pre>
 */

@Slf4j
@Service
@Data
public class JobExecutor implements BeanNameAware {
    @Autowired
    @ToString.Exclude
    JdbcTemplate jt;

    @Autowired
    @Lazy
    @ToString.Exclude
    ApplicationContext applicationContext;

    @ToString.Exclude
    TransactionTemplate transactionTemplate;

    @Autowired
    @Lazy
    @ToString.Exclude
    PlatformTransactionManager transactionManager;

    private JobExecutor self;
    public JobExecutor getSelf(){
        if(self!=null) return self;
        self = applicationContext.getBean(getBeanName(), JobExecutor.class);
        return self;
    };

    @Value("${job-executor.schema-name:tsy}")
    String schemaName;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @ToString.Exclude
    private ExpressionParser spelExpressionParser = new SpelExpressionParser();

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @ToString.Exclude
    private ParserContext parserContext = new TemplateParserContext();
    private String beanName;

    public String expandSpelExpression(String querySource) {
        var rv= spelExpressionParser.parseExpression(querySource, parserContext).getValue(this, String.class);
        return rv;
    }

    @Value("${job-executor.threads:10}")
    int threadsCount;

    @ToString.Exclude
    ExecutorService executorService;

    private String selectRowToProcessQry = "select " +
            " * " +
            " from #{schemaName}.job where not job.is_done and not job.is_failed " +
            " and not exists(select * from #{schemaName}.job_depends_on jdo, #{schemaName}.job j2 where job.id=jdo.job_id and jdo.depends_on_job_id=j2.id and not j2.is_done)" +
            " and job.next_run_after<=now()" +
            " for update skip locked" +
            " limit 1";

    @ToString.Exclude
    private String updateOnAbortQry = "update #{schemaName}.job set status_message=?, is_failed=true where id=?";
    @ToString.Exclude
    private String updateOnDoneQry = "update #{schemaName}.job set status_message=?, context=?::jsonb, is_done=true, return_value=?::jsonb where id=?";
    @ToString.Exclude
    private String updateOnStopQry = "update #{schemaName}.job set status_message=?, context=?::jsonb, is_failed=true where id=?";
    @ToString.Exclude
    private String updateOnContinueQry = "update #{schemaName}.job set status_message=?, context=?::jsonb, next_run_after=coalesce(to_timestamp(?),next_run_after) where id=?";
    @ToString.Exclude
    private String updateOnExceptionQry = "update #{schemaName}.job set status_message=?, is_failed=true where id=?";
    @ToString.Exclude
    private String insertOnSubmitQry = "insert into #{schemaName}.job(name, parameters,context,is_done,is_failed, next_run_after, status_message, parent_job_id)" +
            "values(?,?::jsonb,jsonb_build_object(),false,false,to_timestamp(?),'started',?) " +
            "on conflict(md5(name||parameters::text)) do nothing " +
            "returning id";
    @ToString.Exclude
    private String getExistsJobIdQry="select id from #{schemaName}.job where md5((name || (parameters)::text))=md5(?||?::jsonb::text)";
    @ToString.Exclude
    private String insertDependsOnQry = "insert into #{schemaName}.job_depends_on(job_id,depends_on_job_id) select ?, j.id from #{schemaName}.job j where j.id=?";
    @ToString.Exclude
    private String insertDependentOfQry = "insert into #{schemaName}.job_depends_on(job_id,depends_on_job_id) select j.id,? from #{schemaName}.job j where j.id=?";
    @ToString.Exclude
    private String getJobStateQry = "select *, is_done as done, is_failed as failed from #{schemaName}.job where id=?";
    @ToString.Exclude
    protected String clearJobDependsOnQry = "delete from #{schemaName}.job_depends_on jdo\n" +
            " where not exists(select * from #{schemaName}.job j where jdo.job_id=j.id and (not j.is_done or j.is_failed))\n" +
            "  and not exists(select * from #{schemaName}.job j where jdo.depends_on_job_id=j.id and (not j.is_done or j.is_failed))";
    @ToString.Exclude
    protected String clearJobQry = "delete from #{schemaName}.job j\n" +
            " where not exists(select * from #{schemaName}.job_depends_on jdo where j.id=jdo.job_id)\n" +
            "   and not exists(select * from #{schemaName}.job_depends_on jdo where j.id=jdo.depends_on_job_id)" +
            " and (j.is_done and not j.is_failed) and j.next_run_after<now()-make_interval(mins:=120)";

    @ToString.Exclude
    final ObjectMapper om = new ObjectMapper();
    {
        om.registerModule(new JavaTimeModule());
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd H:m:s")));
        javaTimeModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'H:m:s.SSS")));
        JodaModule jodaModule = new JodaModule();
        om.registerModule(javaTimeModule);
        om.registerModule(jodaModule);
    }
    public ObjectMapper getObjectMapper(){
        return om;
    }

    /**
     * Число воркеров
     */
    @Value("${job-executor.workers:10}")
    int workersCount;

    private void createTables(){
        if(jt.queryForObject(expandSpelExpression("select 2=(\n" +
                "  select count(*) from information_schema.tables t where t.table_schema='#{schemaName}'\n" +
                "  and table_name in('job','job_depends_on')\n" +
                ")"),Boolean.class)){
            log.info(String.format("Required tables were found in %s schema", getSchemaName()));
            return;
        }
        String createTablesScript = "create table #{schemaName}.job(\n" +
                " id bigint generated always as identity primary key,\n" +
                " name text not null,\n" +
                " parameters jsonb not null,\n" +
                " context jsonb not null,\n" +
                " is_done boolean not null default false,\n" +
                " is_failed boolean not null default false,\n" +
                " next_run_after timestamptz not null default now(),\n" +
                " status_message text,\n" +
                " parent_job_id bigint,\n" +
                " return_value jsonb\n" +
                ");\n" +
                "create table #{schemaName}.job_depends_on(\n" +
                " job_id bigint not null,\n" +
                " depends_on_job_id bigint not null check(depends_on_job_id<>job_id)," +
                "primary key(job_id, depends_on_job_id),\n" +
                "unique(depends_on_job_id, job_id),\n" +
                " return_value jsonb\n" +
                ");\n" +
                "create unique index on #{schemaName}.job((md5(name||parameters::text)));\n";
        jt.execute(expandSpelExpression(createTablesScript));
    }

    @PostConstruct
    public void init(){
        transactionTemplate = new TransactionTemplate(transactionManager);

        createTables();
        log.info("Starting with worker number={}", workersCount);
        selectRowToProcessQry = expandSpelExpression(selectRowToProcessQry);
        updateOnAbortQry = expandSpelExpression(updateOnAbortQry);
        updateOnDoneQry = expandSpelExpression(updateOnDoneQry);
        updateOnStopQry = expandSpelExpression(updateOnStopQry);
        updateOnContinueQry = expandSpelExpression(updateOnContinueQry);
        updateOnExceptionQry = expandSpelExpression(updateOnExceptionQry);
        insertOnSubmitQry = expandSpelExpression(insertOnSubmitQry);
        getExistsJobIdQry = expandSpelExpression(getExistsJobIdQry);
        insertDependsOnQry = expandSpelExpression(insertDependsOnQry);
        insertDependentOfQry = expandSpelExpression(insertDependentOfQry);
        getJobStateQry = expandSpelExpression(getJobStateQry);
        clearJobDependsOnQry = expandSpelExpression(clearJobDependsOnQry);
        clearJobQry = expandSpelExpression(clearJobQry);
        if(threadsCount==0){
            throw new RuntimeException("threadCount is not set");
        }
        executorService = Executors.newFixedThreadPool(threadsCount);
        submit("databaseCleanerJob", "{}", Instant.now(), null, List.of(), List.of(), true);
        for (int i = 0; i < workersCount; i++) {
            try {
                executorService.submit(this::doWork);
            }catch (Throwable ex){
                log.error("Exception:{}", ex.getMessage(), ex);
            }
        }
        if(applicationContext==null){
            throw new RuntimeException("ApplicationContext is not set");
        }
        for(BackgroundJobHandler jobHandler: applicationContext.getBeansOfType(BackgroundJobHandler.class).values()){
            submitBackgroudJob(jobHandler);
        }
    }

    private void submitBackgroudJob(BackgroundJobHandler jobHandler) {
        var newJobParameters = new BackgroundJobHandler.Parameters();
        newJobParameters.setJobExecutorName("jobExecutor");
        submit(jobHandler.getBeanName(),
                newJobParameters,
                Instant.now(),
                null,
                List.of(),
                List.of(),
                true);
    }

    private final BeanPropertyRowMapper<Job> beanPropertyRowMapper = new BeanPropertyRowMapper<>(Job.class);
    private final DefaultTransactionAttribute transactionAttribute = new DefaultTransactionAttribute();
    private volatile boolean stopProcessing = false;
    private volatile boolean restart = false;
    private final AtomicInteger activeWorkers = new AtomicInteger(0);

    AtomicReference<Instant> lastDbCheck = new AtomicReference<>(Instant.now());

    @SneakyThrows
    public void shutdown(){
        stopProcessing = true;
        executorService.shutdown();
    }

    private void doWork() {
        boolean somethingFound;
        activeWorkers.incrementAndGet();
        try {
            while (true) {
                if(stopProcessing){
                    activeWorkers.decrementAndGet();
                    return;
                }

                if (Duration.between(lastDbCheck.get(), Instant.now()).get(ChronoUnit.NANOS) < 500000000L) {
                    Thread.sleep(500);
                    continue;
                }

                somethingFound = false;

                var checkInstant = Instant.now();
                TransactionStatus ts = transactionManager.getTransaction(transactionAttribute);
                for (Job jr : jt.query(selectRowToProcessQry, beanPropertyRowMapper)) {
                    somethingFound =true;
                    Object svp = ts.createSavepoint();
                    jr.setJobExecutor(this.getSelf());
                    try {
                        final JobHandler executionBean = applicationContext.getBean(jr.getName(), JobHandler.class);
                        JobState result = executionBean.execute(jr);
                        if (result == null) {
                            result = JobState.DONE("Done");
                        }
                        if (result.getStatus() == JobState.Status.ABORT) {
                            ts.rollbackToSavepoint(svp);
                            log.error("ABORT job {}/{}:{}", jr.getName(), jr.getId(), result.getMessage());

                            jt.update(updateOnAbortQry, result.getMessage(), jr.getId());
                        } else if (result.getStatus() == JobState.Status.DONE) {
                            ts.releaseSavepoint(svp);
                            log.info("DONE job {}/{}:{}", jr.getName(), jr.getId(), result.getMessage());

                            jt.update(updateOnDoneQry,
                                    result.getMessage(),
                                    jr.getContext(),
                                    result.getReturnValue() != null ? om.writeValueAsString(result.getReturnValue()) : null,
                                    jr.getId());
                            if(executionBean instanceof BackgroundJobHandler){
                                this.submitBackgroudJob((BackgroundJobHandler) executionBean);
                            }
                        } else if (result.getStatus() == JobState.Status.STOP) {
                            ts.releaseSavepoint(svp);
                            log.info("STOP job {}/{}:{}", jr.getName(), jr.getId(), result.getMessage());
                            jt.update(updateOnStopQry, result.getMessage(), jr.getContext(), jr.getId());
                        } else if (result.getStatus() == JobState.Status.CONTINUE) {
                            ts.releaseSavepoint(svp);
                            log.info("CONTINUE job {}/{}, next run at {}:{}", jr.getName(), jr.getId(), result.getNextRun(), result.getMessage());
                            jt.update(updateOnContinueQry, result.getMessage(), jr.getContext(), result.getNextRun().toEpochMilli() / 1000.0, jr.getId());
                        }
                    } catch (Throwable ex) {
                        log.error("EXCEPTION in job {}/{}:{}", jr.getName(), jr.getId(), ex.getMessage(), ex);
                        ts.rollbackToSavepoint(svp);
                        log.warn("Rollback to savepoint");
                        jt.update(updateOnExceptionQry, ex.getMessage(), jr.getId());
                    }
                    break;
                }
                transactionManager.commit(ts);
                log.debug("TX commited");

                if(somethingFound){
                    restart = true;
                    continue;
                }
                restart = false;

                lastDbCheck.accumulateAndGet(checkInstant, (oldValue, newValue)->{
                    if(oldValue.isAfter(newValue)) return oldValue;
                    return newValue;
                });

                /*
                Пытаемся минимизировать доступ к базе в случае отсутствия сообщений.
                Если все воркеры простаивают, то к базе лезет только один и смотрит,
                если ли там чего
                 */
                try {
                    while (true) {
                        if(stopProcessing){
                            activeWorkers.decrementAndGet();
                            return;
                        }
                        if(restart) break;

                        final long myId = Thread.currentThread().getId();

                        //кто первый встал - того и тапки
                        var workerThread = CommonState.workerThread.updateAndGet(t ->{
                            if(t==null || !t.isAlive()){
                                return Thread.currentThread();
                            }
                            return t;
                        });

                        if (workerThread.getId() == myId) {
                            //noinspection BusyWait
                            Thread.sleep(500);
                            break;
                        }
                        //noinspection BusyWait
                        Thread.sleep(2000);
                    }
                } catch (NullPointerException ex) {
                    log.error("Get unexpected null pointer exception");
                    return;
                } catch (InterruptedException e) {
                    log.error("Interrupted");
                    return;
                }
            }
        } catch (Exception e) {
            log.error("Got transaction exception:", e);
        }
    }

    /**
     * Постановка задания в очередь на выполнение. Так как это происходит в рамках транзакции, добавляемое задание
     * становится видимым для остальных только по окончанию транзакции, т.е. при вызове в execute - только по окончанию
     * этого метода
     * @param beanName имя бина, реализующего задание
     * @param parameters ссылка на объект с параметрами задания. Он будет сериализован в JSON
     * @param runAfter время, после которого задание должно выполниться
     * @param parentJobId id родительского задания, используется для отображения иерархии заданий для просомтра состояния всех заданий
     * @param dependsOn список id заданий, окончания выполнения которых будет ожидать отправляемое задание
     * @param dependentOf список id заданий, которые будут дожидаться окончания выполнения отправляемого задания
     * @param ignoreExistingJob игнорировать ли уже существующее задание с таким же именем и параметрами
     * @return id добавленного или уже существующего задания
     */
    public Long submit(@NonNull String beanName,
                       @NonNull Object parameters,
                       @NonNull Instant runAfter,
                       Long parentJobId,
                       @NonNull List<Long> dependsOn,
                       @NonNull List<Long> dependentOf,
                       boolean ignoreExistingJob)
    {
        AtomicReference<String> toInsertRef = new AtomicReference<>();
        while(true) {
            var rv = transactionTemplate.execute(ts -> {
                Long iid = null;
                try {
                    String toInsert = parameters instanceof String ? (String) parameters : om.writeValueAsString(parameters);

                    var iids = jt.queryForList(insertOnSubmitQry, Long.class,
                            beanName, toInsert, runAfter.toEpochMilli() / 1000.0, parentJobId);
                    if (!iids.isEmpty()) {
                        iid = iids.get(0);
                    } else {
                        toInsertRef.set(toInsert);
                    }
                } catch (JsonProcessingException ex) {
                    throw new CannotAddRowToJobTableException(String.format("Cannot add row to job table:%s", ex.getMessage()), ex);
                }

                if (iid != null) {
                    for (Long jid : dependsOn) {
                        jt.update(insertDependsOnQry, iid, jid);
                    }
                    for (Long jid : dependentOf) {
                        jt.update(insertDependentOfQry, iid, jid);
                    }
                } else {
                    if (ignoreExistingJob) {
                        log.warn("Submitted job already exists in job table");
                        iid = 0L;
                        var iids = jt.queryForList(getExistsJobIdQry, Long.class, beanName, toInsertRef.get());
                        if (!iids.isEmpty()) iid = iids.get(0);
                    } else {
                        throw new JobAlreadyExistsException(String.format("Job with name %s and specified parameters already exist", beanName));
                    }
                }
                return iid;
            });
            if(rv==null || rv>0) return rv;
        }
    }

    public Long submit(@NonNull JobHandler bean,
                       @NonNull Object parameters,
                       @NonNull Instant runAfter,
                       Long parentJobId,
                       @NonNull List<Long> dependsOn,
                       @NonNull List<Long> dependentOf,
                       boolean ignoreExistingJob){
        return submit(bean.getBeanName(), parameters,runAfter, parentJobId, dependsOn, dependentOf, ignoreExistingJob);
    }
    public Long submit(@NonNull JobHandler bean,
                       @NonNull Object parameters){
        return submit(bean.getBeanName(), parameters,Instant.now(), null, List.of(), List.of(), true);
    }
    public Long submit(@NonNull JobHandler bean,
                       @NonNull Object parameters,
                       Long parentJobId
                       ){
        return submit(bean.getBeanName(), parameters,Instant.now(), parentJobId, List.of(), List.of(), true);
    }
    public Long submit(@NonNull JobHandler bean,
                       @NonNull Object parameters,
                       @NonNull List<Long> dependsOn
                       ){
        return submit(bean.getBeanName(), parameters,Instant.now(), null, dependsOn, List.of(), true);
    }
    public Long submit(@NonNull JobHandler bean,
                       @NonNull Object parameters,
                       Long parentJobId,
                       @NonNull List<Long> dependsOn
    ){
        return submit(bean.getBeanName(), parameters,Instant.now(), parentJobId, dependsOn, List.of(), true);
    }

    public<P, C, T extends GenericJobHandler<P,C>> Long start(@NonNull T bean,
                                                               @NonNull P parameters,
                                                               @NonNull Instant runAfter,
                                                               Long parentJobId,
                                                               @NonNull List<Long> dependsOn,
                                                               @NonNull List<Long> dependentOf,
                                                               boolean ignoreExistingJob
    ){
        return submit(bean, parameters, runAfter, parentJobId, dependsOn, dependentOf, ignoreExistingJob);
    }

    public<P, C, T extends GenericJobHandler<P,C>> Long start(@NonNull T bean,
                                                              @NonNull P parameters)
    {
        return submit(bean, parameters);
    }

    public<P, C, T extends GenericJobHandler<P,C>> Long start(@NonNull T bean,
                                                              @NonNull P parameters,
                                                              @NonNull List<Long> dependsOn
    ){
        return submit(bean, parameters, dependsOn);
    }
    public<P, C, T extends GenericJobHandler<P,C>> Long start(@NonNull T bean,
                                                              @NonNull P parameters,
                                                              Long parentJobId,
                                                              @NonNull List<Long> dependsOn
    ){
        return submit(bean, parameters, parentJobId, dependsOn);
    }



    public<P, C, T extends GenericJobHandler<P,C>> Long start(@NonNull Class<T> beanClass,
                       @NonNull P parameters,
                       @NonNull Instant runAfter,
                       Long parentJobId,
                       @NonNull List<Long> dependsOn,
                       @NonNull List<Long> dependentOf,
                       boolean ignoreExistingJob
                       ){
        return submit(applicationContext.getBean(beanClass), parameters, runAfter, parentJobId, dependsOn, dependentOf, ignoreExistingJob);
    }

    public<P, C, T extends GenericJobHandler<P,C>> Long start(@NonNull Class<T> beanClass,
                                                               @NonNull P parameters)
    {
        return submit(applicationContext.getBean(beanClass), parameters);
    }

    public<P, C, T extends GenericJobHandler<P,C>> Long start(@NonNull Class<T> beanClass,
                                                               @NonNull P parameters,
                                                               @NonNull List<Long> dependsOn
    ){
        return submit(applicationContext.getBean(beanClass), parameters, dependsOn);
    }
    public<P, C, T extends GenericJobHandler<P,C>> Long start(@NonNull Class<T> beanClass,
                                                               @NonNull P parameters,
                                                               Long parentJobId,
                                                               @NonNull List<Long> dependsOn
    ){
        return submit(applicationContext.getBean(beanClass), parameters, parentJobId, dependsOn);
    }

    /**
         * Получение возвращаемого значения задания
         * @param jobId id задания
         * @param clazz класс, который необходимо десериализовать из JSON
         * @param <T> тип, который необходимо десериализовать из JSON
         * @return обернутый в Optional результат выполнения задания
         * @throws CannotDeserializeReturnValueException если не удалась десериализация
         * @throws NoJobFoundException не удалось найти задние с соответствующим id
         */
    public<T> Optional<T> getOptionalReturnValue(Long jobId, Class<T> clazz){
        try {
            String qry = expandSpelExpression("select return_value::text from #{schemaName}.job where job.id=?");
            log.debug("return value query:{}, jobId={}", qry, jobId);
            final String content = jt.queryForObject(qry, String.class, jobId);
            if(content==null){
                return Optional.empty();
            }
            T v = om.readValue(content, clazz);
            return Optional.of(v);
        } catch (IOException e) {
            throw new CannotDeserializeReturnValueException(String.format("Cannot deserialize return value for passed class:%s", clazz.getCanonicalName()), e);
        } catch (EmptyResultDataAccessException ex){
            throw new NoJobFoundException("No job found for id=" + jobId, ex);
        }
    }

    /**
     * Получение текущего состояния задания
     * @param jobId id задания
     * @return JobState, обернутый в Optional. Доступен только статус и сообщение.
     */
    Optional<JobState> getOptionalJobState(long jobId){
        List<Job> jobs = jt.query(getJobStateQry, beanPropertyRowMapper, jobId);
        if(jobs.size()>1){
            throw new GetMoreThanOneRowFromJobTable("Get more than one row from job table when access by primary key");
        }
        if(jobs.size()==0){
            return Optional.empty();
        }
        Job job = jobs.get(0);

        if(job.isDone()){
            return Optional.of(JobState.DONE(job.getStatusMessage()));
        }else if(job.isFailed()){
            return Optional.of(JobState.ABORT(job.getStatusMessage()));
        }else{
            return Optional.of(JobState.CONTINUE(job.getStatusMessage()));
        }
    }

    /**
     * Добавляет пару зависимостей заданий
     * @param jobId задание, которое становится зависимым
     * @param dependsOnJobId задание, от которого оно будет зависеть
     */
    public void dependOn(long jobId, long dependsOnJobId){
        jt.update(expandSpelExpression("insert into #{schemaName}.job_depends_on(job_id,depends_on_job_id)values(?,?) on conflict do nothing"), jobId, dependsOnJobId);
    }

    public void independOn(long jobId, long dependsOnJobId){
        jt.update(expandSpelExpression("delete from #{schemaName}.job_depends_on jdo where (jdo.job_id,jdo.depends_on_job_id)=(?,?)"), jobId, dependsOnJobId);
    }

    /**
     * Получение задания по его id
     * @param jobId id задания
     * @return задание
     */
    public Job getJobById(@NonNull Long jobId){
        Job job = DataAccessUtils.singleResult(jt.query(expandSpelExpression("select * from #{schemaName}.job where id=?"),
                (rs, rowNum) -> {
                    var rv = new Job();
                    rv.setFailed(rs.getBoolean("is_failed"));
                    rv.setDone(rs.getBoolean("is_done"));
                    rv.setContext(rs.getString("context"));
                    rv.setParameters(rs.getString("parameters"));
                    rv.setStatusMessage(rs.getString("status_message"));
                    rv.setId(rs.getLong("id"));
                    rv.setName(rs.getString("name"));
                    rv.setParentJobId(rs.getLong("parent_job_id"));
                    rv.setNextRunAfter(((java.sql.Timestamp)rs.getObject("next_run_after")).toInstant());
                    return rv;
                }, jobId));
        if(null==job) return null;
        job.setJobExecutor(this.getSelf());
        return job;
    }

    public List<Long> getJobIdsByName(@NonNull String jobName){
        return jt.queryForList(expandSpelExpression("select j.id from #{schemaName}.job j where j.name=?"), Long.class, jobName);
    }

    /**
     * restart failed job
     * @return voi
     */
    public void restartJob(@NonNull Long jobId){
        jt.update(expandSpelExpression("update #{schemaName}.job set is_failed=false where id=?"),jobId);
    }

    /**
     * Run job now
     */
    public void runJob(@NonNull Long jobId){
        jt.update(expandSpelExpression("update #{schemaName}.job set next_run_after=now() where id=?"),jobId);
    }
    /**
     * Stop job by setting run time to infinity
     */
    public void stopJob(@NonNull Long jobId){
        jt.update(expandSpelExpression("update #{schemaName}.job set next_run_after='infinity' where id=?"),jobId);
    }
    public void deleteJob(@NonNull Long jobId){
        jt.update(expandSpelExpression("delete from #{schemaName}.job where id=?"),jobId);
    }

    public void runAllJobs(JobHandler jh){
        for(var jid: getJobIdsByName(jh.getBeanName()))
            runJob(jid);
    }
    public void  stopAllJObs(JobHandler jh){
        for (var jid: getJobIdsByName(jh.getBeanName()))
            stopJob(jid);
    }

    public List<JobExecution> listDependentJobs(@NonNull Long jobId){
        return jt.queryForList(expandSpelExpression("select jdo.job_id from #{schemaName}.job_depends_on jdo where jdo.depends_on_job_id=?"),Long.class, jobId)
                .stream()
                .map(v-> getJobExecution(v))
                .collect(Collectors.toList());
    }

    public List<JobExecution> listDependsOnJobs(@NonNull Long jobId){
        return jt.queryForList(expandSpelExpression("select jdo.depends_on_job_id from #{schemaName}.job_depends_on jdo where jdo.job_id=?"),Long.class, jobId)
                .stream()
                .map(v-> getJobExecution(v))
                .collect(Collectors.toList());
    }

    public JobExecution getJobExecution(@NonNull Long jobId){
        return new JobExecution(jobId, this);
    }

    @Override
    public void setBeanName(String s) {
        this.beanName = s;
    }
}
