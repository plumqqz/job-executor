package shaif.jobs;

import com.fasterxml.jackson.core.JsonProcessingException;

public class CannotAddRowToJobTableException extends RuntimeException {
    public CannotAddRowToJobTableException(String msg, JsonProcessingException ex) {
        super(msg,ex);
    }
}
