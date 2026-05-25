package com.devalere.eventdriven.saga.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    public static final String SAGA_COMMANDS_TOPIC = "saga-commands";
    public static final String SAGA_REPLIES_TOPIC = "saga-replies";

    @Bean
    public NewTopic sagaCommandsTopic() {
        return TopicBuilder.name(SAGA_COMMANDS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic sagaRepliesTopic() {
        return TopicBuilder.name(SAGA_REPLIES_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
