package com.ajay.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Defining the topic here means Spring Boot creates it automatically on startup
 * (via KafkaAdmin) if it doesn't already exist - same topic you created manually
 * via CLI in Chapter 1, just declared as code now. Safe to run even if the
 * topic already exists; Spring/Kafka will just leave it alone.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name("order-events")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
