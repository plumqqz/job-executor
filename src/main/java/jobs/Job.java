package jobs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Data
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

    public JobExecutor getJobExecutor() {
        return jobExecutor;
    }

    InjectableValues ivs = null;
    protected void setJobExecutor(JobExecutor jobExecutor){
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
     * ?????????????????? ????????????????????
     * @param clazz ?????????? ????????????????????
     * @param <T> ?????? ???????????? ????????????????????
     * @return ?????????????????????????????????? ???????????? ???????????????????? ????????????
     */
    public<T> T getParameters(Class<T> clazz) {
        return parseValue(getParameters(), clazz);
    }

    /**
     * ?????????????????? ????????????????????
     * @param clazz - ?????????? ????????????????????
     * @param <T> - ?????? ???????????? ????????????????????
     * @return - ?????????????????????????????????? ???????????? ???????????????????? ????????????, ???????????????????? ?? Optional
     */
    public<T> Optional<T> getOptionalParameters(Class<T> clazz) {
        return Optional.ofNullable(parseValue(getParameters(), clazz));
    }

    /**
     * ?????????????????? ?????????????????? ???????????????????? ??????????????. ?????? ?????????? ???????????? ?? execute, ?????????? ????????????????????
     * execute ?????????? ???????????????? ?????????? ?????????????????????????? ?? ??????????????????
     * @param clazz ?????????? ??????????????????
     * @param <T> ?????? ???????????? ??????????????????
     * @return ?????????????????????????????????? ????????????????
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
                throw new RuntimeException("Cannot serialize context object");
            }
        }
        return context;
    }

    /**
     * ???????????????? ?????????????? ???? ????????????????????
     * @param beanName ?????? ????????
     * @param parameters ?????????????????? ??????????????
     * @return ???????????? ???? ?????????????? ???????????????????? ??????????????
     */
    public JobExecution submit(@NonNull String beanName, @NonNull Object parameters){
        return new JobExecution(getJobExecutor().submit(beanName, parameters, Instant.now(), getId(), List.of(), List.of(), false), getJobExecutor());
    }

    /**
     * ???????????????? ?????????????? ???? ????????????????????
     * @param beanName ?????? ????????
     * @param parameters ?????????????????? ??????????????
     * @param runAfter ?????????? ???????????? ????????????????????
     * @return ???????????? ???? ?????????????? ???????????????????? ??????????????
     */
    public JobExecution submit(@NonNull String beanName, @NonNull Object parameters, @NonNull Instant runAfter){
        return new JobExecution(getJobExecutor().submit(beanName, parameters, runAfter, getId(), List.of(), List.of(), false), getJobExecutor());
    }

    /**
     * ???????????????? ?????????????? ???? ???????????????????? ?? ???????????????? ?????? ??????????????????. ?????? ???? ?????????????????????? ??????????, ?????????????? ??????????????
     * ???????????????????? ?????????????????? ???? ????????????????????????
     * @param beanName ?????? ????????
     * @param parameters ?????????????????? ??????????????
     * @return ???????????? ???? ?????????????? ???????????????????? ??????????????
     */
    public JobExecution submitAndWait(@NonNull String beanName, @NonNull Object parameters){
        return new JobExecution(getJobExecutor().submit(beanName, parameters, Instant.now(), getId(), List.of(), List.of(getId()), false), getJobExecutor());
    }

    /**
     * ???????????????? ?????????????? ???? ???????????????????? ?? ???????????????? ?????? ??????????????????. ?????? ???? ?????????????????????? ??????????, ?????????????? ??????????????
     * ???????????????????? ?????????????????? ???? ????????????????????????
     * @param beanName ?????? ????????
     * @param parameters ?????????????????? ??????????????
     * @param runAfter ?????????? ???????????? ????????????????????
     * @return ???????????? ???? ?????????????? ???????????????????? ??????????????
     */
    public JobExecution submitAndWait(@NonNull  String beanName, @NonNull Object parameters, @NonNull Instant runAfter){
        return new JobExecution(getJobExecutor().submit(beanName, parameters, runAfter, getId(), List.of(), List.of(getId()), false), getJobExecutor());
    }

    /**
     * ???????????????? ?????????????? ???? ???????????????????? ?? ???????????????? ?????? ??????????????????. ?????? ???? ?????????????????????? ??????????, ?????????????? ??????????????
     * ???????????????????? ?????????????????? ???? ????????????????????????
     * @param beanClass ?????????? ????????. ???????? ?????? ???????????? ???????????????????????? ?? ApplicationContext
     * @param parameters ?????????????????? ??????????????
     * @return ???????????? ???? ?????????????? ???????????????????? ??????????????
     */
    public JobExecution submit(@NonNull Class<? extends JobHandler> beanClass, @NonNull Object parameters){
        return submit(beanClass, parameters, Instant.now());
    }

    /**
     * ???????????????? ?????????????? ???? ????????????????????
     * @param beanClass ?????????? ????????. ???????? ?????? ???????????? ???????????????????????? ?? ApplicationContext
     * @param parameters ?????????????????? ??????????????
     * @param runAfter ?????????? ???????????? ????????????????????
     * @return ???????????? ???? ?????????????? ???????????????????? ??????????????
     */
    public JobExecution submit(@NonNull Class<? extends JobHandler> beanClass, @NonNull Object parameters, @NonNull Instant runAfter){
        return new JobExecution(getJobExecutor().submit(getOnlyBeanNameOfType(beanClass), parameters, runAfter, getId(), List.of(), List.of(getId()), false), getJobExecutor());
    }

    /**
     * ???????????????? ?????????????? ???? ???????????????????? ?? ???????????????? ?????? ??????????????????. ?????? ???? ?????????????????????? ??????????, ?????????????? ??????????????
     * ???????????????????? ?????????????????? ???? ????????????????????????
     * @param beanClass ?????????? ????????. ???????? ?????? ???????????? ???????????????????????? ?? ApplicationContext
     * @param parameters ?????????????????? ??????????????
     * @return ???????????? ???? ?????????????? ???????????????????? ??????????????
     */
    public JobExecution submitAndWait(@NonNull Class<? extends JobHandler> beanClass, @NonNull Object parameters){
        return submitAndWait(beanClass, parameters, Instant.now());
    }
    /**
     * ???????????????? ?????????????? ???? ???????????????????? ?? ???????????????? ?????? ??????????????????. ?????? ???? ?????????????????????? ??????????, ?????????????? ??????????????
     * ???????????????????? ?????????????????? ???? ????????????????????????
     * @param beanClass ?????????? ????????. ???????? ?????? ???????????? ???????????????????????? ?? ApplicationContext
     * @param parameters ?????????????????? ??????????????
     * @param runAfter ?????????? ???????????? ????????????????????
     * @return ???????????? ???? ?????????????? ???????????????????? ??????????????
     */
    public JobExecution submitAndWait(@NonNull Class<? extends JobHandler> beanClass, @NonNull Object parameters, @NonNull Instant runAfter){
        return new JobExecution(getJobExecutor().submit(getOnlyBeanNameOfType(beanClass), parameters, runAfter, getId(), List.of(), List.of(getId()), false), getJobExecutor());
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
     * ???????????????? ?????????????? ???? ????????????????????
     * @param bean ??????, ?????????????????????? ??????????????
     * @param parameters ?????????????????? ??????????????
     * @return ???????????? ???? ?????????????? ???????????????????? ??????????????
     */
    public JobExecution submit(@NonNull JobHandler bean, @NonNull Object parameters){
        return submit(bean, parameters, Instant.now());
    }

    /**
     * ???????????????? ?????????????? ???? ????????????????????
     * @param bean ??????, ?????????????????????? ??????????????
     * @param parameters ?????????????????? ??????????????
     * @param runAfter ?????????? ???????????? ????????????????????
     * @return ???????????? ???? ?????????????? ???????????????????? ??????????????
     */
    public JobExecution submit(@NonNull JobHandler bean, @NonNull Object parameters, @NonNull Instant runAfter){
        return new JobExecution(getJobExecutor().submit(bean.getBeanName(), parameters, runAfter, getId(), List.of(), List.of(), false), getJobExecutor());
    }

    /**
     * ???????????????? ?????????????? ???? ???????????????????? ?? ???????????????? ?????? ??????????????????. ?????? ???? ?????????????????????? ??????????, ?????????????? ??????????????
     * ???????????????????? ?????????????????? ???? ????????????????????????
     * @param bean ?????????? ????????. ???????? ?????? ???????????? ???????????????????????? ?? ApplicationContext
     * @param parameters ?????????????????? ??????????????
     * @return ???????????? ???? ?????????????? ???????????????????? ??????????????
     */
    public JobExecution submitAndWait(@NonNull JobHandler bean, Object parameters){
        return submitAndWait(bean, parameters, Instant.now());
    }
    /**
     * ???????????????? ?????????????? ???? ???????????????????? ?? ???????????????? ?????? ??????????????????. ?????? ???? ?????????????????????? ??????????, ?????????????? ??????????????
     * ???????????????????? ?????????????????? ???? ????????????????????????
     * @param bean ?????????? ????????. ???????? ?????? ???????????? ???????????????????????? ?? ApplicationContext
     * @param parameters ?????????????????? ??????????????
     * @param runAfter ?????????? ???????????? ????????????????????
     * @return ???????????? ???? ?????????????? ???????????????????? ??????????????
     */
    public JobExecution submitAndWait(@NonNull JobHandler bean, Object parameters, @NonNull Instant runAfter){
        return new JobExecution(getJobExecutor().submit(bean.getBeanName(), parameters, runAfter, getId(), List.of(), List.of(getId()), false), getJobExecutor());
    }

    /**
     * ???????????????? ?????????????????? ???????????????????? ?????????????? ?????????? ??????????????????
     * @param jobId id ??????????????
     * @param clazz ??????????, ?? ?????????????? ???????? ?????????????????????????????????? ??????????????????
     * @param <T> ?????? ???????????? clazz
     * @return ?????????????????? ???????????????????? ??????????????
     */
    public<T> T getReturnValue(Long jobId, Class<T> clazz){
        return getJobExecutor().getOptionalReturnValue(jobId, clazz).orElseThrow(()->new RuntimeException("Unknown job or empty result"));
    }

    /**
     * ???????????????? ?????????????????? ???????????????????? ?????????????? ?????????? ??????????????????
     * @param er ?????????????????? ????????????????????
     * @param clazz ??????????, ?? ?????????????? ???????? ?????????????????????????????????? ??????????????????
     * @param <T> ?????? ???????????? clazz
     * @return ????????????????, ???????????????????????? ????????????????
     */
    public<T> T getReturnValue(JobExecution er, Class<T> clazz){
        return getReturnValue(er.jobId, clazz);
    }

    /**
     * ???????????????? ?????????????????? ??????????????
     * @param jobId id ??????????????
     * @return ?????????????????? ??????????????
     */
    public JobState getJobState(long jobId){
        return getJobExecutor().getOptionalJobState(jobId).orElseThrow(()->new RuntimeException("Unknown job!"));
    }

    /**
     * ???????????????? ?????????????????? ??????????????
     * @param er ???????????? ???? ?????????????? ???????????????????? ??????????????
     * @return ?????????????????? ??????????????
     */
    public JobState getJobState(@NonNull JobExecution er){
        return getJobState(er.getJobId());
    }

    /**
     * ???????????????? ?????????????????? ???????????????????? ?????????????? ?????????? ?????????????????? ?????? Optional
     * @param jobId ?????????????????? ????????????????????
     * @param clazz ??????????, ?? ?????????????? ???????? ?????????????????????????????????? ??????????????????
     * @param <T> ?????? ???????????? clazz
     * @return ????????????????, ???????????????????????? ????????????????
     */
    public<T> Optional<T> getOptionalReturnValue(Long jobId, Class<T> clazz){
        return getJobExecutor().getOptionalReturnValue(jobId, clazz);
    }

    /**
     * ???????????????? ?????????????????? ???????????????????? ?????????????? ?????????? ?????????????????? ?????? Optional
     * @param er ???????????? ???? ?????????????? ???????????????????? ??????????????
     * @param clazz ??????????, ?? ?????????????? ???????? ?????????????????????????????????? ??????????????????
     * @param <T> ?????? ???????????? clazz
     * @return ????????????????, ???????????????????????? ????????????????
     */
    public<T> Optional<T> getOptionalReturnValue(JobExecution er, Class<T> clazz){
        return getOptionalReturnValue(er.getJobId(), clazz);
    }

    /**
     * ???????????????? ?????????????????? ?????????????? ?????? Optional
     * @param jobId id ??????????????
     * @return ?????????????????? ??????????????
     */
    public Optional<JobState> getOptionalJobState(long jobId){
        return getJobExecutor().getOptionalJobState(jobId);
    }

    /**
     * ???????????????????????? ?????????????????????? ?????????????? ?????????????? ???? ????????????????????
     * @param dependOn ???????????? ???? ??????????????, ???? ???????????????? ???????? ?????????? ??????????????????
     */
    public void dependOn(JobExecution dependOn){
        getJobExecutor().dependOn(getId(), dependOn.getJobId());
    }

    /**
     * ???????????? ?????????????????? ?????????????? ???????????????????? ?????????????????? ???? ?????????????? ??????????????
     * @param depender ?????????????? ??????????????????, ?????????????? ???????????? ?????????? ??????????????????
     */
    public void makeJobDependent(JobExecution depender){
        getJobExecutor().dependOn(depender.getJobId(), getId());
    }
}
