package com.hmdp.config.kafka;

import com.hmdp.kafka.message.VoucherOrderMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * 秒杀订单 Kafka Consumer 基础配置。
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    /** Retry Topic 内再次执行消费的固定等待时间。 */
    static final long RETRY_BACK_OFF_MILLIS = 1000L;

    /** Retry Topic 初次消费失败后最多再尝试一次。 */
    static final long RETRY_MAX_ATTEMPTS = 1L;

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

        // 将反序列化失败转换为可由监听器和错误处理器控制的空消息，避免容器越过业务确认边界。
        ErrorHandlingDeserializer<String> keyDeserializer =
                new ErrorHandlingDeserializer<>(new StringDeserializer());
        ErrorHandlingDeserializer<VoucherOrderMessage> errorHandlingValueDeserializer =
                new ErrorHandlingDeserializer<>(valueDeserializer);

        return new DefaultKafkaConsumerFactory<>(
                properties,
                keyDeserializer,
                errorHandlingValueDeserializer);
    }

    /** 主 Topic 处理失败后，将原记录转发到 Retry Topic 的恢复器。 */
    @Bean
    public DeadLetterPublishingRecoverer voucherOrderMainRecoverer(
            KafkaTemplate<String, VoucherOrderMessage> voucherOrderKafkaTemplate,
            @Value("${hmdp.kafka.topics.voucher-order-retry:hmdp.seckill.order.retry.v1}")
                    String retryTopic) {
        return new ReliableDeadLetterPublishingRecoverer(
                voucherOrderKafkaTemplate,
                (record, exception) -> new TopicPartition(retryTopic, record.partition()));
    }

    /** Retry Topic 超过有限重试次数后，将原记录转发到 DLT。 */
    @Bean
    public DeadLetterPublishingRecoverer voucherOrderRetryRecoverer(
            KafkaTemplate<String, VoucherOrderMessage> voucherOrderKafkaTemplate,
            @Value("${hmdp.kafka.topics.voucher-order-dlt:hmdp.seckill.order.dlt.v1}")
                    String dltTopic) {
        return new ReliableDeadLetterPublishingRecoverer(
                voucherOrderKafkaTemplate,
                (record, exception) -> new TopicPartition(dltTopic, record.partition()));
    }

    /**
     * 主 Topic 不在原分区循环重试；第一次失败即转入 Retry Topic。
     */
    @Bean
    public SeekToCurrentErrorHandler voucherOrderMainErrorHandler(
            @Qualifier("voucherOrderMainRecoverer") DeadLetterPublishingRecoverer recoverer) {
        SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler(
                recoverer,
                new FixedBackOff(0L, 0L));
        configureRecoveredOffsetCommit(errorHandler);
        return errorHandler;
    }

    /**
     * Retry Topic 使用固定退避进行一次额外尝试，耗尽后转入 DLT。
     */
    @Bean
    public SeekToCurrentErrorHandler voucherOrderRetryErrorHandler(
            @Qualifier("voucherOrderRetryRecoverer") DeadLetterPublishingRecoverer recoverer) {
        SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler(
                recoverer,
                new FixedBackOff(RETRY_BACK_OFF_MILLIS, RETRY_MAX_ATTEMPTS));
        configureRecoveredOffsetCommit(errorHandler);
        return errorHandler;
    }

    /**
     * 创建主 Topic 监听容器；业务成功由监听方法确认，失败转发成功由错误处理器确认。
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, VoucherOrderMessage>
    voucherOrderKafkaListenerContainerFactory(
            @Qualifier("voucherOrderMainErrorHandler") SeekToCurrentErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, VoucherOrderMessage> factory =
                createManualAckFactory();
        factory.setErrorHandler(errorHandler);
        return factory;
    }

    /** 创建 Retry Topic 监听容器并绑定有限重试错误处理器。 */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, VoucherOrderMessage>
    voucherOrderRetryKafkaListenerContainerFactory(
            @Qualifier("voucherOrderRetryErrorHandler") SeekToCurrentErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, VoucherOrderMessage> factory =
                createManualAckFactory();
        factory.setErrorHandler(errorHandler);
        return factory;
    }

    /** 创建 DLT 监听容器；失败订单日志写入成功后由监听器手动确认。 */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, VoucherOrderMessage>
    voucherOrderDltKafkaListenerContainerFactory() {
        return createManualAckFactory();
    }

    /** 统一创建关闭自动提交、启用立即手动确认的监听容器。 */
    private ConcurrentKafkaListenerContainerFactory<String, VoucherOrderMessage>
    createManualAckFactory() {
        ConcurrentKafkaListenerContainerFactory<String, VoucherOrderMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(voucherOrderConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    /**
     * 恢复器成功转发后提交源记录 offset；转发抛出异常时不提交并继续由容器处理。
     */
    private void configureRecoveredOffsetCommit(SeekToCurrentErrorHandler errorHandler) {
        errorHandler.setCommitRecovered(true);
        errorHandler.setAckAfterHandle(false);
    }
}
