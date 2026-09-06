package com.hmdp.config.kafka;

import com.hmdp.kafka.message.VoucherOrderMessage;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * 秒杀订单 Kafka Producer 基础配置。
 */
@Configuration
public class KafkaProducerConfig {

    private final String bootstrapServers;
    private final String acknowledgments;
    private final Integer retries;

    /**
     * 从 Spring Kafka 配置中读取连接地址、确认级别和重试次数。
     */
    public KafkaProducerConfig(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.producer.acks:all}") String acknowledgments,
            @Value("${spring.kafka.producer.retries:3}") Integer retries) {
        this.bootstrapServers = bootstrapServers;
        this.acknowledgments = acknowledgments;
        this.retries = retries;
    }

    /**
     * 创建以字符串为 key、JSON 为 value 的订单消息 ProducerFactory。
     */
    @Bean
    public ProducerFactory<String, VoucherOrderMessage> voucherOrderProducerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, acknowledgments);
        properties.put(ProducerConfig.RETRIES_CONFIG, retries);
        return new DefaultKafkaProducerFactory<>(properties);
    }

    /**
     * 提供秒杀订单专用 KafkaTemplate，当前阶段尚未接入核心业务流程。
     */
    @Bean
    public KafkaTemplate<String, VoucherOrderMessage> voucherOrderKafkaTemplate() {
        return new KafkaTemplate<>(voucherOrderProducerFactory());
    }
}
