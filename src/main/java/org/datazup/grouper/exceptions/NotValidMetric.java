package org.datazup.grouper.exceptions;

/**
 * Created by ninel on 11/26/16.
 */
public class NotValidMetric extends RuntimeException{
    public NotValidMetric(String message){ super(message);}
    public NotValidMetric(String message, Throwable e){ super(message, e);}
}
