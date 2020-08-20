package com.example.kafkatest;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Service
public class KafkaConsumer {
    private final Logger logger;
    public final static String TOPIC = "test.foo.sub";

    public KafkaConsumer(Logger logger) {
        System.out.println("consumer created");
        this.logger = logger;
    }

    @KafkaListener(topics = TOPIC, groupId = "consumer-hoge", id = "consumer1234")
    void consume (@Payload String message) {
        System.out.println("message = " + message);
        logger.info(message);
    }
}
