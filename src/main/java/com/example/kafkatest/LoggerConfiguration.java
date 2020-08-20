package com.example.kafkatest;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.logging.Logger;

@Configuration
public class LoggerConfiguration {
    @Bean
    Logger logger() {
        return Logger.getLogger("consumer");
    }
}
