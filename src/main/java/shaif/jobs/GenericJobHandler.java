package shaif.jobs;

import lombok.Data;

import java.lang.reflect.ParameterizedType;
import java.util.List;

@Data
public abstract class GenericJobHandler<P,C> implements JobHandler{
    String beanName;
    Class<P> pClass = (Class<P>)((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    Class<C> cClass = (Class<C>)((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[1];
    /**
     * Собственно обработчик
     *
     * @param job контекс выполнения, параметры и вспомогательные методы
     * @return что делать дальше - остановиться, продолжить выполнение или закончиться
     */
    @Override
    public JobState execute(Job job) {
        P parameters = (P) job.getParameters(pClass);
        C context = (C) job.getContext(cClass);
        return execute(job, parameters, context);
    }

     public abstract JobState execute(Job job, P parameters, C context);
}
