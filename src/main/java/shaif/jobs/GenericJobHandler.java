package shaif.jobs;

import lombok.Data;

import java.lang.reflect.ParameterizedType;
import java.util.List;

@Data
public class GenericJobHandler<P,C> implements JobHandler{
    String beanName;
    public P p;
    public C c;

    /**
     * Собственно обработчик
     *
     * @param job контекс выполнения, параметры и вспомогательные методы
     * @return что делать дальше - остановиться, продолжить выполнение или закончиться
     */
    @Override
    public JobState execute(Job job) {
        P parameters = (P) job.getParameters((Class<P>)((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
        C context = (C) job.getContext((Class<C>)((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[1]);
        return execute(job, parameters, context);
    }

     public JobState execute(Job job, P parameters, C context) {
         throw new RuntimeException("Must be overriden");
    }
}
