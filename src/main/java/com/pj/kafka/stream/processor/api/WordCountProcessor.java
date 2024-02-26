package com.pj.kafka.stream.processor.api;


import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class WordCountProcessor implements Processor<String,String,String,String> {

    private ProcessorContext context;
    private String storeName;
    private KeyValueStore<String, String> kvStore;

    public WordCountProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
       // Processor.super.init(context);
        // keep the processor context locally because we need it in punctuate() and commit()
        this.context = context;
        // retrieve the key-value store named "Counts"
        kvStore = (KeyValueStore) context.getStateStore(storeName);
        // schedule a punctuate() method every 1000 milliseconds based on stream-time
        this.context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, (timestamp) -> {
            try (KeyValueIterator<String, String> iter = this.kvStore.all()) {
                while (iter.hasNext()) {
                    KeyValue<String, String> entry = iter.next();
                    Record<String, String> record = new Record<>(entry.key, entry.value, timestamp);
                    context.forward(record);
                }
            }

            // commit the current processing progress
          //  context.commit();
           // context.
        });
    }

    @Override
    public void process(Record<String, String> record) {
        String key = record.key();
        String storeValue = kvStore.get(key);
        if(storeValue == null) {
            System.out.println("No value present at state stor with key =" + key + " value =" + storeValue);
           // storeValue = "default-value";
        }
        kvStore.put(key,storeValue);

       System.out.println("record: " + record);
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
