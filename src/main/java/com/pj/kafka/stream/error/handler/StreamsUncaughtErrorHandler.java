package com.pj.kafka.stream.error.handler;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public class StreamsUncaughtErrorHandler implements StreamsUncaughtExceptionHandler {
    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        if(exception instanceof StreamsException) {
            Throwable originException = exception.getCause();
            if("Retryable transient error".equalsIgnoreCase(originException.getMessage())) {
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            }
        }
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION; // shutdown all instances
       // return StreamThreadExceptionResponse.SHUTDOWN_CLIENT; //shutdown those instances where we are facing issues
    }
}
