package com.example.kafkatest;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {
    final private KafkaPublisher kafkaPublisher;

    public KafkaController(KafkaPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }

    @PostMapping("publish")
    void publish(@RequestBody MyMessage message) {
        kafkaPublisher.publish(message);
    }
}
