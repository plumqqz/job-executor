package shaif.jobs;

import lombok.Data;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

@Data
public abstract class GenericJobHandler<P,C> implements JobHandler{
    String beanName;
//    Class<P> pClass = (Class<P>)((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
//    Class<C> cClass = (Class<C>)((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[1];
    Class<P> pClass = (Class<P>) resolveTypeArgument(this.getClass(),0);
    Class<C> cClass = (Class<C>) resolveTypeArgument(this.getClass(),1);
    /**
     * Собственно обработчик
     *
     * @param job контекс выполнения, параметры и вспомогательные методы
     * @return что делать дальше - остановиться, продолжить выполнение или закончиться
     */
    @Override
    public JobState execute(Job job) throws Exception {
        P parameters = (P) job.getParameters(pClass);
        C context = (C) job.getContext(cClass);
        return execute(job, parameters, context);
    }

     public abstract JobState execute(Job job, P parameters, C context) throws Exception;

    private static Class<?> resolveTypeArgument(Class<?> clazz, int ix) {
        Type type = clazz.getGenericSuperclass();
        while (!(type instanceof ParameterizedType)) {
            if (type instanceof Class<?>) {
                var c=(Class<?>)type;
                if (c == Object.class) {
                    throw new IllegalStateException(
                            "Не найден параметризованный суперкласс. " +
                                    "Тип не задан конкретно.");
                }
                type = c.getGenericSuperclass();
            } else {
                throw new IllegalStateException("Неожиданный тип: " + type);
            }
        }
        Type arg = ((ParameterizedType) type).getActualTypeArguments()[ix];
        if (arg instanceof Class<?>) {
            return (Class<?>)arg;
        }
        if (arg instanceof ParameterizedType) {
            var pt = (ParameterizedType) arg;
            return (Class<?>) pt.getRawType();
        }
        // arg это TypeVariable — тип так и не был задан конкретно
        throw new IllegalStateException(
                "Тип-аргумент не разрешён в конкретный класс: " + arg);
    }
}
