package com.pj.kafka.stream.processor.api.supplier;

import com.pj.kafka.stream.processor.api.WordCountProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Collections;
import java.util.Set;

public class CustomProcessorSupplier implements ProcessorSupplier<String,String,String,String> {
    private String storeName;
    private StoreBuilder storeBuilder;

    public CustomProcessorSupplier(String storeName, StoreBuilder storeBuilder) {
        this.storeName = storeName;
        this.storeBuilder = storeBuilder;
    }

    @Override
    public Processor<String, String, String, String> get() {
        return new WordCountProcessor(storeName);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Collections.singleton(storeBuilder);
    }
}
