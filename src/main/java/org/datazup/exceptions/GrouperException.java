package org.datazup.exceptions;

public class GrouperException extends Exception {

    public GrouperException(String message){
        super(message);
    }

    public GrouperException(String message, Throwable e){
        super(message, e);
    }
}
