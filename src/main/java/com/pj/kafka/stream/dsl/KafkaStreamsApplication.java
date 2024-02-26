package com.pj.kafka.stream.dsl;

import com.fasterxml.jackson.databind.JsonNode;
import com.pj.kafka.stream.error.handler.ProductionErrorHandler;
import com.pj.kafka.stream.error.handler.StreamDeserializationErrorHandler;
import com.pj.kafka.stream.error.handler.StreamsUncaughtErrorHandler;
import com.pj.kafka.stream.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
/*import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger; */


import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class KafkaStreamsApplication {

    public static final String INPUT_TOPIC_1 = "glass_rweb";
    public static final String INPUT_TOPIC_2 = "glass_app_unified";

    public static void main(String[] args) {

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);



        //Create serde 
        final Serde<String> keyType = Serdes.String();
        final Serde<JsonNode> valueType = jsonSerde; // Serdes.String();

        //creating producer config
        KafkaProducer producer = KafkaProducer.Builder.getBuilder().addKafkaParams(getProducerConfigConfig()).build();

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> stream = createKStream(keyType, valueType, builder);
       // stream.print(Printed.toSysOut());


        applyBranch(stream,producer);
        startStream(stream,builder);
    }

    private static <K, V>KStream<K, V> createKStream(Serde<K> keyType, Serde<V> valueType,StreamsBuilder builder ) {
        Collection<String> topics = Arrays.asList(INPUT_TOPIC_1,INPUT_TOPIC_2);
        KStream<K, V> views = builder.stream(
                topics,
                Consumed.with(keyType, valueType)
        );
        return views;
    }

    private static void startStream(KStream kstream, StreamsBuilder builder) {
        final CountDownLatch latch = new CountDownLatch(1);
        final KafkaStreams streams = new KafkaStreams(builder.build(), getConsumerConfig());
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

    private static void applyBranch( KStream<String, JsonNode> stream, KafkaProducer producer) {
       /* stream.split().branch((key,value)-> true,
                Branched.withConsumer((ks) -> ks.to("output-topic-name"))
                ); */

       stream.split().branch(
                (key, value) -> "mxoa".equalsIgnoreCase(value.get("applId").asText()) &&
                        "".equalsIgnoreCase(value.get("subAppId").asText()),  // MX-OD
                Branched.withFunction(ks -> {
                    System.out.println("Record is for mx-od banner");
                    ks.foreach((k,v) ->{
                        System.out.println("MX-OD Record" + v.toString());
                        producer.sendAsync("mx-od-output-topic",k,v.toString());
                    });
                    return null;
                })
        ).branch(
                (key, value) -> "mxoa".equalsIgnoreCase(value.get("applId").asText()) &&
                        "gmoa".equalsIgnoreCase(value.get("subAppId").asText()), //MX-EA

               Branched.withFunction(ks -> {
                   System.out.println("Record is for mx-ea banner");
                   ks.foreach((k,v) ->{
                       System.out.println("MX-EA Record" + v.toString());
                       producer.sendAsync("mx-ea-output-topic",k,v.toString());
                   });
                   return null;
               })
        ).branch(
                (key, value) -> "bdoa".equalsIgnoreCase(value.get("applId").asText()) &&
                        "gmoa".equalsIgnoreCase(value.get("subAppId").asText()), //MX-BODEGA-EA
               Branched.withFunction(ks -> {
                   System.out.println("Record is for mx-bodega-ea banner");
                   ks.foreach((k,v) ->{
                       System.out.println("MX-BODEGA-EA Record" + v.toString());
                       producer.sendAsync("mx-bodega-ea-output-topic",k,v.toString());
                   });
                   return null;
               })
        ).branch(
                (key, value) -> "bdoa".equalsIgnoreCase(value.get("applId").asText()) &&
                        "groa".equalsIgnoreCase(value.get("subAppId").asText()), //MX-BODEGA-OD
               Branched.withFunction(ks -> {
                   System.out.println("Record is for mx-bodega-od banner");
                   ks.foreach((k,v) ->{
                       System.out.println("MX-BODGEA-OD Record" + v.toString());
                       producer.sendAsync("mx-bodgea-od-output-topic",k,v.toString());
                   });
                   return null;
               })
        );


    }

    private static Properties getConsumerConfig() {
        Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-group-id");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                StreamDeserializationErrorHandler.class);
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ProductionErrorHandler.class);
        return props;
    }

    private static HashMap<String, Object> getProducerConfigConfig() {
        HashMap<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return kafkaParams;
    }
}
