package com.dataguardian.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaConfig {
    private String brokers;
    private String rawTopic;
    private String issuesTopic;
}