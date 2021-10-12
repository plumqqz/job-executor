package shaif.jobs;

import org.springframework.dao.EmptyResultDataAccessException;

public class NoJobFoundException extends RuntimeException {
    public NoJobFoundException(String s, EmptyResultDataAccessException ex) {
        super(s, ex);
    }
}
