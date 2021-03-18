package jobs;

import lombok.Data;
import lombok.NonNull;

import java.time.Duration;
import java.time.Instant;

import static jobs.JobState.Status.*;

/**
 * Состояние выполяемого задания.
 * Задание может находиться в следующих состояниях {@link Status}
 */
@Data
public class JobState {
    private Instant nextRun = Instant.now();
    private Object returnValue=null;
    Status status;

    Object getReturnValue(){
        return returnValue;
    }

    /**
     *
     * @return Время следующего выполнения
     */
    public Instant getNextRun(){
        return nextRun;
    }
    private String message="Unknown error";

    private JobState(){}

    /**
     *
     * @return Сообщение последнего выполнения
     */
    public String getMessage(){
        return message;
    }

    /**
     * Создает состояние STOP
     * @param message сообщение
     * @return созданное состояние
     */
    public static JobState STOP(@NonNull String message){
        var rv = new JobState();
        rv.status = STOP;
        rv.message= message;
        return rv;
    }

    /**
     * Создает состояние ABORT
     * @param message сообщение
     * @return созданное состояние
     */
    public static JobState ABORT(@NonNull String message){
        var rv = new JobState();
        rv.status = ABORT;
        rv.message= message;
        return rv;
    }

    /**
     * Создает состояние DONE
     * @param message сообщение
     * @return созданное состояние
     */
    public static JobState DONE(@NonNull String message){
        var rv = new JobState();
        rv.status = DONE;
        rv.message= message;
        rv.returnValue = null;
        return rv;
    }

    /**
     * Создает состояние STOP и устанавливает возвращаемое значение
     * @param returnValue возвращаемое значение
     * @param message сообщение
     * @return созданное состояние
     */
    public static JobState DONE(@NonNull Object returnValue, @NonNull String message){
        var rv = new JobState();
        rv.status = DONE;
        rv.message = message;
        rv.returnValue = returnValue;
        return rv;
    }

    /**
     * Создает состояние CONTINUE
     * Задание готово к немедленному выполнению
     * @param message сообщение
     * @return созданное состояние
     */
    public static JobState CONTINUE(@NonNull String message){
        var rv = new JobState();
        rv.status = CONTINUE;
        rv.message = message;
        return rv;
    }

    /**
     * Создает состояние CONTINUE
     * Задание готово к выполнению после nextRun
     * @param message сообщение
     * @param nextRun время выполнения
     * @return созданное состояние
     */
    public static JobState CONTINUE(@NonNull String message, @NonNull Instant nextRun){
        var rv = new JobState();
        rv.status =CONTINUE;
        rv.message=message;
        rv.nextRun = nextRun;
        return rv;
    }

    /**
     * Создает состояние CONTINUE
     * Задание готово к выполнению после истечения периода времени duration
     * @param message сообщение
     * @param duration период ожидания
     * @return созданное состояние
     */
    public static JobState CONTINUE(@NonNull String message, @NonNull Duration duration){
        var rv = new JobState();
        rv.status = CONTINUE;
        rv.message = message;
        rv.nextRun = Instant.now().plus(duration);
        return rv;
    }

    /**
     * Возможные резуотаты выполнения
     * <ul>
     * <li><code>STOP</code> - остановка выполнения задания, при возврате этого значения контекст сохраняется, транзакцкция фиксируется, задание помечатся как неудачное</li>
     * <li><code>ABORT</code> - остановка выполнения задания, при возврате этого значения контекст не сохраняется, транзакция откатывается, задание помечатся как неудачное</li>
     * <li><code>DONE</code> - окончание выполнения задания, транзакция фиксируется, контекст сохраняется (хоть в обычном случае он больше не потребуется), можно указать возвращаемое значение
     * <li><code>CONTINUE</code> - продолжение выполнения задания, контекст сохраняется, можно указать время следующего запуска или период ожидания
     * </ul>
     */
    public enum Status {
        STOP,
        ABORT,
        DONE,
        CONTINUE
    }
}
