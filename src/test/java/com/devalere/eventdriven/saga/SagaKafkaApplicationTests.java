package com.devalere.eventdriven.saga;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"saga-commands", "saga-replies"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:9094", "port=9094"}
)
@DirtiesContext
class SagaKafkaApplicationTests {

    @Test
    void contextLoads() {
    }
}
