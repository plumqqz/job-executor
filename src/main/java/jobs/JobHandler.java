package jobs;

import org.springframework.beans.factory.BeanNameAware;

public interface JobHandler extends BeanNameAware {
    /**
     * Необходимо для получения имени бина во время создания нового задания
     * @return имя бина
     */
    String getBeanName();

    /**
     * Собственно обработчик
     * @param job контекс выполнения, параметры и вспомогательные методы
     * @return что делать дальше - остановиться, продолжить выполнение или закончиться
     */
    JobState execute(Job job);
}
