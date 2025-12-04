package org.fusadora.dataflow.exception;

/**
 * org.fusadora.dataflow.exception.StaticUtilityException
 * Exception raises inside any utility class.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class StaticUtilityException extends RuntimeException {

    public StaticUtilityException() {
        super();
    }

    public StaticUtilityException(String message) {
        super(message);
    }

    public StaticUtilityException(String message, Throwable exception) {
        super(message, exception);
    }

    public StaticUtilityException(Throwable t) {
        super(t);
    }
}
