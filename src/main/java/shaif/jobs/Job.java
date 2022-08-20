package shaif.jobs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Getter
@Setter
@Slf4j
public class Job {
    static ObjectMapper om = new ObjectMapper();
    static {
        om.registerModule(new JavaTimeModule());
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd H:m:s")));
        javaTimeModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'H:m:s.SSS")));
        om.registerModule(javaTimeModule);
        om.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    Long id;
    String name;
    String parameters;
    String context;
    String statusMessage;
    Instant nextRunAfter;
    boolean isDone;
    boolean isFailed;
    Long parentJobId;

    private JobExecutor jobExecutor;

    private Object savedContext;

    InjectableValues ivs = null;
    protected void setJobExecutor(@NonNull JobExecutor jobExecutor){
        this.jobExecutor = jobExecutor;
        ivs = new InjectableValues.Std().addValue(JobExecutor.class, jobExecutor);
    }

    private<T> T parseValue(String value, Class<T> clazz) {
        try {
            return om.reader(ivs).readValue(value, clazz);
        } catch (IOException ex) {
            throw new RuntimeException("Cannot parse value:" + ex.getMessage(), ex);
        }
    }

    /**
     * Получение параметров
     * @param clazz класс параметров
     * @param <T> тип класса параметров
     * @return десериализованный объект указанного класса
     */
    public<T> T getParameters(Class<T> clazz) {
        return parseValue(getParameters(), clazz);
    }

    /**
     * Получение параметров
     * @param clazz - класс параметров
     * @param <T> - тип класса параметров
     * @return - десериализованный объект указанного класса, завернутый в Optional
     */
    public<T> Optional<T> getOptionalParameters(Class<T> clazz) {
        return Optional.ofNullable(parseValue(getParameters(), clazz));
    }

    /**
     * Получение контекста выполнения задания. Его можно менять в execute, после завершения
     * execute новое значение будет сериализовано и сохранено
     * @param clazz класс контекста
     * @param <T> тип класса контекста
     * @return десериализованный контекст
     */
    public<T> T getContext(Class<T> clazz) {
        if(savedContext!=null) {
            //noinspection unchecked
            return (T) savedContext;
        }

        savedContext = parseValue(getContext(), clazz);
        //noinspection unchecked
        return (T)savedContext;
    }

    protected String getContext(){
        if(savedContext!=null) {
            try {
                return om.writeValueAsString(savedContext);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot serialize context object:"+e.getMessage(),e);
            }
        }
        return context;
    }

    /**
     * Отправка задания на выполнение
     * @param beanName имя бина
     * @param parameters параметры задания
     * @return ссылка на процесс выполнения задания
     */
    public JobExecution submit(@NonNull String beanName, @NonNull Object parameters){
        return new JobExecution(getJobExecutor().submit(beanName, parameters, Instant.now(), getId(), List.of(), List.of(), true), getJobExecutor());
    }

    /**
     * Отправка задания на выполнение
     * @param beanName имя бина
     * @param parameters параметры задания
     * @param runAfter время начала выполнения
     * @return ссылка на процесс выполнения задания
     */
    public JobExecution submit(@NonNull String beanName, @NonNull Object parameters, @NonNull Instant runAfter){
        return new JobExecution(getJobExecutor().submit(beanName, parameters, runAfter, getId(), List.of(), List.of(), true), getJobExecutor());
    }

    /**
     * Отправка задания на выполнение и ожидание его окончания. ЭТО НЕ БЛОКИРУЮЩИЙ МЕТОД, текушее задание
     * становится зависимым от добавляемого
     * @param beanName имя бина
     * @param parameters параметры задания
     * @return ссылка на процесс выполнения задания
     */
    public JobExecution submitAndWait(@NonNull String beanName, @NonNull Object parameters){
        return new JobExecution(getJobExecutor().submit(beanName, parameters, Instant.now(), getId(), List.of(), List.of(getId()), true), getJobExecutor());
    }

    /**
     * Отправка задания на выполнение и ожидание его окончания. ЭТО НЕ БЛОКИРУЮЩИЙ МЕТОД, текушее задание
     * становится зависимым от добавляемого
     * @param beanName имя бина
     * @param parameters параметры задания
     * @param runAfter время начала выполнения
     * @return ссылка на процесс выполнения задания
     */
    public JobExecution submitAndWait(@NonNull  String beanName, @NonNull Object parameters, @NonNull Instant runAfter){
        return new JobExecution(getJobExecutor().submit(beanName, parameters, runAfter, getId(), List.of(), List.of(getId()), true), getJobExecutor());
    }

    /**
     * Отправка задания на выполнение и ожидание его окончания. ЭТО НЕ БЛОКИРУЮЩИЙ МЕТОД, текушее задание
     * становится зависимым от добавляемого
     * @param beanClass класс бина. Этот бин должен существовать в ApplicationContext
     * @param parameters параметры задания
     * @return ссылка на процесс выполнения задания
     */
    public JobExecution submit(@NonNull Class<? extends JobHandler> beanClass, @NonNull Object parameters){
        return submit(beanClass, parameters, Instant.now());
    }

    /**
     * Отправка задания на выполнение
     * @param beanClass класс бина. Этот бин должен существовать в ApplicationContext
     * @param parameters параметры задания
     * @param runAfter время начала выполнения
     * @return ссылка на процесс выполнения задания
     */
    public JobExecution submit(@NonNull Class<? extends JobHandler> beanClass, @NonNull Object parameters, @NonNull Instant runAfter){
        return new JobExecution(getJobExecutor().submit(getOnlyBeanNameOfType(beanClass), parameters, runAfter, getId(), List.of(), List.of(getId()), true), getJobExecutor());
    }

    /**
     * Отправка задания на выполнение и ожидание его окончания. ЭТО НЕ БЛОКИРУЮЩИЙ МЕТОД, текушее задание
     * становится зависимым от добавляемого
     * @param beanClass класс бина. Этот бин должен существовать в ApplicationContext
     * @param parameters параметры задания
     * @return ссылка на процесс выполнения задания
     */
    public JobExecution submitAndWait(@NonNull Class<? extends JobHandler> beanClass, @NonNull Object parameters){
        return submitAndWait(beanClass, parameters, Instant.now());
    }
    /**
     * Отправка задания на выполнение и ожидание его окончания. ЭТО НЕ БЛОКИРУЮЩИЙ МЕТОД, текушее задание
     * становится зависимым от добавляемого
     * @param beanClass класс бина. Этот бин должен существовать в ApplicationContext
     * @param parameters параметры задания
     * @param runAfter время начала выполнения
     * @return ссылка на процесс выполнения задания
     */
    public JobExecution submitAndWait(@NonNull Class<? extends JobHandler> beanClass, @NonNull Object parameters, @NonNull Instant runAfter){
        return new JobExecution(getJobExecutor().submit(getOnlyBeanNameOfType(beanClass), parameters, runAfter, getId(), List.of(), List.of(getId()), true), getJobExecutor());
    }

    private String getOnlyBeanNameOfType(@NonNull Class<? extends JobHandler> beanClass) {
        Map<String, ? extends JobHandler> map = getJobExecutor().applicationContext.getBeansOfType(beanClass);
        if (map.size() > 1) {
            throw new RuntimeException(String.format("More than one bean of %s exists", beanClass.getCanonicalName()));
        }
        if (map.size() == 0) {
            throw new RuntimeException(String.format("No one bean of %s was found", beanClass.getCanonicalName()));
        }
        //noinspection SuspiciousMethodCalls
        return map.get(map.keySet().toArray()[0]).getBeanName();
    }

    /**
     * Отправка задания на выполнение
     * @param bean бин, реализующий задание
     * @param parameters параметры задания
     * @return ссылка на процесс выполнения задания
     */
    public JobExecution submit(@NonNull JobHandler bean, @NonNull Object parameters){
        return submit(bean, parameters, Instant.now());
    }

    /**
     * Отправка задания на выполнение
     * @param bean бин, реализующий задание
     * @param parameters параметры задания
     * @param runAfter время начала выполнения
     * @return ссылка на процесс выполнения задания
     */
    public JobExecution submit(@NonNull JobHandler bean, @NonNull Object parameters, @NonNull Instant runAfter){
        return new JobExecution(getJobExecutor().submit(bean.getBeanName(), parameters, runAfter, getId(), List.of(), List.of(), true), getJobExecutor());
    }

    /**
     * Отправка задания на выполнение
     * @param bean бин, реализующий задание
     * @param parameters параметры задания
     * @param dependsOn задания, от которых зависит текущее
     * @return ссылка на процесс выполнения задания
     */
    public JobExecution submit(@NonNull JobHandler bean, @NonNull Object parameters, @NonNull List<Long> dependsOn){
        return new JobExecution(getJobExecutor().submit(bean.getBeanName(), parameters, Instant.now(), getId(), dependsOn, List.of(), true), getJobExecutor());
    }

    /**
     * Отправка задания на выполнение и ожидание его окончания. ЭТО НЕ БЛОКИРУЮЩИЙ МЕТОД, текушее задание
     * становится зависимым от добавляемого
     * @param bean класс бина. Этот бин должен существовать в ApplicationContext
     * @param parameters параметры задания
     * @return ссылка на процесс выполнения задания
     */
    public JobExecution submitAndWait(@NonNull JobHandler bean, Object parameters){
        return submitAndWait(bean, parameters, Instant.now());
    }
    /**
     * Отправка задания на выполнение и ожидание его окончания. ЭТО НЕ БЛОКИРУЮЩИЙ МЕТОД, текушее задание
     * становится зависимым от добавляемого
     * @param bean класс бина. Этот бин должен существовать в ApplicationContext
     * @param parameters параметры задания
     * @param runAfter время начала выполнения
     * @return ссылка на процесс выполнения задания
     */
    public JobExecution submitAndWait(@NonNull JobHandler bean, Object parameters, @NonNull Instant runAfter){
        return new JobExecution(getJobExecutor().submit(bean.getBeanName(), parameters, runAfter, getId(), List.of(), List.of(getId()), true), getJobExecutor());
    }

    /**
     * Получает результат выполнения задания после окончания
     * @param jobId id задания
     * @param clazz класс, в который надо десериализовывать результат
     * @param <T> тип класса clazz
     * @return результат выполнения задания
     */
    public<T> T getReturnValue(Long jobId, Class<T> clazz){
        return getJobExecutor().getOptionalReturnValue(jobId, clazz).orElseThrow(()->new RuntimeException("Unknown job or empty result"));
    }

    /**
     * Получает результат выполнения задания после окончания
     * @param er результат выполнения
     * @param clazz класс, в который надо десериализовывать результат
     * @param <T> тип класса clazz
     * @return значение, возвращенное заданием
     */
    public<T> T getReturnValue(JobExecution er, Class<T> clazz){
        return getReturnValue(er.jobId, clazz);
    }

    /**
     * Получить состояние задания
     * @param jobId id задания
     * @return состояние задания
     */
    public JobState getJobState(long jobId){
        return getJobExecutor().getOptionalJobState(jobId).orElseThrow(()->new RuntimeException("Unknown job!"));
    }

    /**
     * Получить состояние задания
     * @param er ссылка на процесс выполнения задания
     * @return состояние задания
     */
    public JobState getJobState(@NonNull JobExecution er){
        return getJobState(er.getJobId());
    }

    /**
     * Получает результат выполнения задания после окончания как Optional
     * @param jobId результат выполнения
     * @param clazz класс, в который надо десериализовывать результат
     * @param <T> тип класса clazz
     * @return значение, возвращенное заданием
     */
    public<T> Optional<T> getOptionalReturnValue(Long jobId, Class<T> clazz){
        return getJobExecutor().getOptionalReturnValue(jobId, clazz);
    }

    /**
     * Получает результат выполнения задания после окончания как Optional
     * @param er ссылка на процесс выполнения задания
     * @param clazz класс, в который надо десериализовывать результат
     * @param <T> тип класса clazz
     * @return значение, возвращенное заданием
     */
    public<T> Optional<T> getOptionalReturnValue(JobExecution er, Class<T> clazz){
        return getOptionalReturnValue(er.getJobId(), clazz);
    }

    /**
     * Получить состояние задания как Optional
     * @param jobId id задания
     * @return состояние задания
     */
    public Optional<JobState> getOptionalJobState(long jobId){
        return getJobExecutor().getOptionalJobState(jobId);
    }

    /**
     * Устанавлиает зависимость данного задания от указанного
     * @param dependOn ссылка на задание, от которого надо стать зависимым
     */
    public void dependOn(JobExecution dependOn){
        getJobExecutor().dependOn(getId(), dependOn.getJobId());
    }

    /**
     * Делает указанный процесс выполнения зависимым от данного задания
     * @param depender процесс выпонения, который должен стать зависимым
     */
    public void makeJobDependent(JobExecution depender){
        getJobExecutor().dependOn(depender.getJobId(), getId());
    }

    /**
     * Удаляет зависимость указанного задания от данного
     * @param depender - задание, для которого необходимо удалить зависимость от заданного
     */
    public void makeJobIndependent(JobExecution depender){
        getJobExecutor().independOn(depender.getJobId(), getId());
    }

    /**
     * Делает данное задание зависимым от указанного задания
     * @param dependsOn - задание, от которого должно зависеть данное задание
     */
    public void makeSelfDependent(JobExecution dependsOn){
        getJobExecutor().dependOn(getId(), dependsOn.getJobId());
    }

    /**
     * Удаляет зависимость данного задания от указанного
     * @param dependsOn задание, зависиость от которого надо удалить
     */
    public void makeSelfIndependent(JobExecution dependsOn){
        getJobExecutor().independOn(getId(), dependsOn.getJobId());
    }

    public void restart(){
        jobExecutor.restartJob(getId());
    }

    public void stop(){
        jobExecutor.stopJob(getId());
    }

    public void resume(){
        jobExecutor.runJob(getId());
    }
    public void delete(){
        jobExecutor.deleteJob(getId());
    }

    public List<JobExecution> listDependentJobs(){
        return jobExecutor.listDependentJobs(id);
    }

    public List<JobExecution> listDependsOnJobs(){
        return jobExecutor.listDependsOnJobs(id);
    }

}
