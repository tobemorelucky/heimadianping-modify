package com.hmdp.config.kafka;

import com.hmdp.kafka.message.VoucherOrderMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * 秒杀订单 Kafka Consumer 基础配置。
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    private final String bootstrapServers;
    private final String groupId;
    private final String autoOffsetReset;

    /**
     * 从 Spring Kafka 配置中读取消费者连接和消费组参数。
     */
    public KafkaConsumerConfig(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id:hmdp-seckill-order-create-v1}") String groupId,
            @Value("${spring.kafka.consumer.auto-offset-reset:earliest}") String autoOffsetReset) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.autoOffsetReset = autoOffsetReset;
    }

    /**
     * 创建订单消息 ConsumerFactory，并限定 JSON 反序列化的可信包。
     */
    @Bean
    public ConsumerFactory<String, VoucherOrderMessage> voucherOrderConsumerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        JsonDeserializer<VoucherOrderMessage> valueDeserializer =
                new JsonDeserializer<>(VoucherOrderMessage.class);
        valueDeserializer.addTrustedPackages("com.hmdp.kafka.message");

        return new DefaultKafkaConsumerFactory<>(
                properties,
                new StringDeserializer(),
                valueDeserializer);
    }

    /**
     * 创建订单监听容器；Phase 1 在监听方法正常返回后按单条记录确认。
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, VoucherOrderMessage>
    voucherOrderKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, VoucherOrderMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(voucherOrderConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
