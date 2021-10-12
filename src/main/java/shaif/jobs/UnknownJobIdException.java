package shaif.jobs;

public class UnknownJobIdException extends RuntimeException {
    public UnknownJobIdException(String msg) {
        super(msg);
    }
}
