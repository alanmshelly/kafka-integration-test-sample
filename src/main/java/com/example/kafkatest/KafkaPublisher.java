package com.example.kafkatest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaPublisher {
    final private KafkaTemplate<String, String> kafkaTemplate;
    final private String TOPIC = "test.foo.pub";

    public KafkaPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    void publish(MyMessage myMessage) {
        try {
            String s = new ObjectMapper().writeValueAsString(myMessage);
            kafkaTemplate.send(TOPIC, s);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
