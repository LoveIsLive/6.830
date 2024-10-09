package simpledb.exception;

public class TypeMismatchException extends  RuntimeException {

    private static final long serialVersionUID = -655192313955898974L;

    public TypeMismatchException(String message) {
        super(message);
    }
}
