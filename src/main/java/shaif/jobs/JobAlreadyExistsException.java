package shaif.jobs;

public class JobAlreadyExistsException extends RuntimeException {
    public JobAlreadyExistsException(String msg) {
        super(msg);
    }
}
