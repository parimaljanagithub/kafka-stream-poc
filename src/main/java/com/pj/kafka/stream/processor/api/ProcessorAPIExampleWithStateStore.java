package com.pj.kafka.stream.processor.api;

import com.pj.kafka.stream.error.handler.ProductionErrorHandler;
import com.pj.kafka.stream.error.handler.StreamDeserializationErrorHandler;
import com.pj.kafka.stream.error.handler.StreamsUncaughtErrorHandler;
import com.pj.kafka.stream.processor.api.supplier.CustomProcessorSupplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ProcessorAPIExampleWithStateStore {

    final static String STORE_NAME = "name-store";
    private static StoreBuilder<KeyValueStore<String, String>> countStoreSupplier =
            Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(STORE_NAME),
                    Serdes.String(),
                    Serdes.String());

    public static void main(String[] args) {
        Serde<String> stringSerde = Serdes.String();
        Topology topology = new Topology();
        // add the source processor node that takes Kafka topic "source-topic" as input
        topology.addSource("Source",
                        stringSerde.deserializer(),
                        stringSerde.deserializer(),
                        "glass_rweb");

        // add the WordCountProcessor node which takes the source processor as its upstream processor
        topology.addProcessor("Process",
                        new CustomProcessorSupplier(STORE_NAME,countStoreSupplier),
                        "Source");

        //adding statestore
        topology.addStateStore(countStoreSupplier,"Process");

        // add the sink processor node that takes Kafka topic "sink-topic" as output
        // and the WordCountProcessor node as its upstream processor
        topology.addSink("Sink",
                "test_sink_topic",
                stringSerde.serializer(),
                stringSerde.serializer(),
                "Process");

        startStream(topology);
    }

    private static Properties getProperty() {
        Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-processor-api-group-id");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                StreamDeserializationErrorHandler.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000000");
      //  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ProductionErrorHandler.class);
        return props;
    }

    private static void startStream( Topology topology) {
        final CountDownLatch latch = new CountDownLatch(1);
        final KafkaStreams streams = new KafkaStreams(topology, getProperty());
        streams.setUncaughtExceptionHandler(new StreamsUncaughtErrorHandler());

        try {
            Runtime.getRuntime().addShutdownHook(new Thread("stream-custom-shutdown-hook") {
                @Override
                public void run() {
                    System.out.println("shutdown hook called");
                    streams.close();
                    latch.countDown();
                }
            });
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
