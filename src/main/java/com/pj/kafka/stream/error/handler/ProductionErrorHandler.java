package com.pj.kafka.stream.error.handler;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

public class ProductionErrorHandler implements ProductionExceptionHandler {
    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        if(exception instanceof RecordBatchTooLargeException) {
            return ProductionExceptionHandlerResponse.CONTINUE;
        }
        return ProductionExceptionHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
