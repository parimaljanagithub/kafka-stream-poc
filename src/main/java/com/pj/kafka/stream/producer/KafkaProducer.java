package com.pj.kafka.stream.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.HashMap;
import java.util.concurrent.Future;

public class KafkaProducer<K,V> {

    private Builder builder;
    private Producer<K, V> producer ;
    private KafkaProducer(Builder builder) {
        this.builder = builder;
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(this.builder.getKafkaParams());
        this.addShutdownHook();
    }

    public static class Builder {
        private HashMap<String, Object> kafkaParams;

        public static Builder getBuilder(){
            return new Builder();
        }

        public HashMap<String, Object> getKafkaParams() {
            return kafkaParams;
        }

        public Builder addKafkaParams(HashMap<String, Object> kafkaParams) {
            this.kafkaParams = kafkaParams;
            return this;
        }

        public KafkaProducer build() {
            return new KafkaProducer(this);
        }
    }

    public Future<RecordMetadata> sendAsync(String topic, K key, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<K, V>(topic,key,value);
        return producer.send(record);
    }


    public Future<RecordMetadata> sendWithTransaction(String topic, K key, V value) {
        Future<RecordMetadata> output;
        producer.initTransactions();
        ProducerRecord<K, V> record = new ProducerRecord<K, V>(topic,key,value);
        try {
            producer.beginTransaction();
            output = producer.send(record);
            producer.commitTransaction();
            return output;
        } catch(ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e){
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
            throw e;
        }catch (Exception e) {
            producer.abortTransaction();
            throw e;
        }
    }

    public void closeProducer() {
        producer.close();
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread( this::closeProducer));
    }


}
