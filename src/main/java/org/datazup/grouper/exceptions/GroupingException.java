package org.datazup.grouper.exceptions;

/**
 * Created by admin@datazup on 11/26/16.
 */
public class GroupingException extends RuntimeException {
    public GroupingException(String message){ super(message);}
    public GroupingException(String message, Throwable e) { super(message, e);}
}
