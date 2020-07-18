package com.kafka;

public class KafkaApp {
    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.TOPIC).start();
        new KafkaConsumer(KafkaProperties.TOPIC).start();
    }
}