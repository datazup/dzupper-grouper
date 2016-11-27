package org.datazup.grouper.exceptions;

/**
 * Created by ninel on 11/26/16.
 */
public class DimensionKeyException extends RuntimeException {
    public DimensionKeyException(String message){
        super(message);
    }

    public DimensionKeyException(String message, Throwable e){
        super(message, e);
    }
}
