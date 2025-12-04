package org.fusadora.dataflow.exception;

/**
 * org.fusadora.dataflow.exception.DataFlowException
 * Exception caught during Dataflow execution.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class DataFlowException extends RuntimeException {
    public DataFlowException() {
        super();
    }

    public DataFlowException(String message) {
        super(message);
    }

    public DataFlowException(String message, Throwable exception) {
        super(message, exception);
    }

    public DataFlowException(Throwable t) {
        super(t);
    }
}
