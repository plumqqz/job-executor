package shaif.jobs;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.ToString;

import java.util.Optional;

/**
 * Процесс выполнения задания.
 */
public class JobExecution {
    Long jobId;

    @JsonIgnore
    @JacksonInject
    @ToString.Exclude
    JobExecutor jobExecutor;

    /**
     * Получение возвращаемого значения задания. Так как задание может еще рабоать на момент вызова или не возвращать вообще
     * никакого значения, то доступен только вариант с Optional
     * @param clazz класс для десериализуемого объекта
     * @param <T> тип этого класса
     * @return десериализованный объект, обернутый в Optional
     */
    public<T> Optional<T> getOptionalReturnValue(Class<T> clazz){
        return jobExecutor.getOptionalReturnValue(jobId, clazz);
    }
    public<T> T getReturnValue(Class<T> clazz){
        return getOptionalReturnValue(clazz).get();
    }

    protected JobExecution(Long jobId, JobExecutor jobExecutor){
        this.jobId=jobId;
        this.jobExecutor =jobExecutor;
    }
    /* надо для десеоиализации */
    public JobExecution(){
    }

    /**
     * Получение id задания
     * @return id задания
     */
    public long getJobId() {
        return jobId;
    }

    /**
     * Получение состояния выполняющегося задания
     * @return состояние выполняющегося задания
     */
    public JobState getJobState(){
        return getOptionalJobState().orElseThrow(()->new UnknownJobIdException("Unknown job"));
    }

    /**
     * Получение состояния выполняющегося задания, обернутого в Optional
     * @return состояние выполняющегося задания
     */
    public Optional<JobState> getOptionalJobState(){
        if(jobId==null) throw  new UnknownJobIdException("jobId has not been set");
        return jobExecutor.getOptionalJobState(jobId);
    }
}
