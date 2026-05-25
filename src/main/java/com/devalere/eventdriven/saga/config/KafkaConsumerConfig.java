package com.devalere.eventdriven.saga.config;

import com.devalere.eventdriven.saga.common.SagaCommand;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration Kafka pour les consumers de commandes (SagaCommand).
 * Chaque service a son propre consumer group, donc chaque service
 * recoit toutes les commandes et filtre par targetService.
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, SagaCommand> commandConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<SagaCommand> deserializer = new JsonDeserializer<>(SagaCommand.class);
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(false);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SagaCommand> commandListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, SagaCommand> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(commandConsumerFactory());
        return factory;
    }
}
