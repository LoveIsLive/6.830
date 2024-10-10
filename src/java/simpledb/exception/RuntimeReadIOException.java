package simpledb.exception;

public class RuntimeReadIOException extends RuntimeException {
    private static final long serialVersionUID = -1727764565588017447L;

    public RuntimeReadIOException(String message) {
        super(message);
    }
}
