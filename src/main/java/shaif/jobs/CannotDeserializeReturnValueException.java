package shaif.jobs;

import java.io.IOException;

public class CannotDeserializeReturnValueException extends RuntimeException {
    public CannotDeserializeReturnValueException(String msg, IOException e) {
        super(msg, e);
    }
}
