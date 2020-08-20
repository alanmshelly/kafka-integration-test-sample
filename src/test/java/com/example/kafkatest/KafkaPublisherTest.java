package com.example.kafkatest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
)
@EmbeddedKafka(
        partitions = 1,
        controlledShutdown = true,
        topics = "test.foo.pub"
)
class KafkaPublisherTest {
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;


    @Value("${spring.embedded.kafka.brokers}")
    private String brokerAddresses;

    private Consumer<String, String> consumer;
    private String TOPIC = "test.foo.pub";

    @Autowired
    KafkaPublisher kafkaPublisher;

    @BeforeEach
    void setUp() {
        consumer = configureConsumer();
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    private Consumer<String, String> configureConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps)
                .createConsumer();
        consumer.subscribe(Collections.singleton(TOPIC));
        return consumer;
    }

    @Test
    void publisherPublishesMessage() throws JsonProcessingException {
        MyMessage message = MyMessage.builder()
                .id("TESTID")
                .number(10)
                .list(singletonList(
                        Item.builder().key(200).value("hello").build()
                ))
                .build();
        kafkaPublisher.publish(message);


        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, TOPIC);
        MyMessage receivedMessage = new ObjectMapper().readValue(singleRecord.value(), MyMessage.class);

        assertThat(receivedMessage).isEqualTo(message);
    }
}