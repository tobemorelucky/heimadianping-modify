package com.hmdp.kafka.integration;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.consumer.VoucherOrderKafkaConsumer;
import com.hmdp.kafka.message.VoucherOrderMessage;
import com.hmdp.kafka.producer.VoucherOrderKafkaProducer;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.service.impl.VoucherOrderTransactionalService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 本地 Kafka 独立联调测试。
 *
 * <p>测试只操作专用 Topic，不加载 Spring Boot 应用上下文，也不会调用正式秒杀消费者。</p>
 */
@Slf4j
@EnabledIfEnvironmentVariable(named = "KAFKA_INTEGRATION_TEST_ENABLED", matches = "true")
class KafkaLocalIntegrationTest {

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TEST_TOPIC = "hmdp.seckill.order.integration-test.v1";
    private static final String BUSINESS_SWITCH_TEST_TOPIC =
            "hmdp.seckill.order.business-switch-test.v1";
    private static final long OPERATION_TIMEOUT_SECONDS = 10L;
    private static final long CONSUME_TIMEOUT_SECONDS = 20L;

    /**
     * 检查 broker、创建测试 Topic，并验证 Spring Kafka JSON 消息可以完成发送和消费闭环。
     */
    @Test
    void shouldSendAndConsumeVoucherOrderMessageThroughLocalKafka() throws Exception {
        String bootstrapServers = bootstrapServers();
        checkKafkaHealthAndCreateTopic(bootstrapServers, TEST_TOPIC);

        DefaultKafkaProducerFactory<String, VoucherOrderMessage> producerFactory =
                createProducerFactory(bootstrapServers);
        KafkaTemplate<String, VoucherOrderMessage> kafkaTemplate =
                new KafkaTemplate<>(producerFactory);

        try (Consumer<String, VoucherOrderMessage> consumer = createConsumer(bootstrapServers)) {
            consumer.subscribe(Collections.singleton(TEST_TOPIC));

            VoucherOrderMessage expected = new VoucherOrderMessage(
                    System.currentTimeMillis(),
                    920250907L,
                    930250907L,
                    LocalDateTime.now().withNano(0));
            SendResult<String, VoucherOrderMessage> sendResult =
                    sendTestOrderMessage(kafkaTemplate, expected);

            ConsumerRecord<String, VoucherOrderMessage> actualRecord =
                    consumeExpectedRecord(consumer, expected.getOrderId());
            VoucherOrderMessage actual = actualRecord == null ? null : actualRecord.value();

            assertNotNull(actual, "在限定时间内未消费到本次发送的测试消息");
            assertEquals(expected.getOrderId(), actual.getOrderId());
            assertEquals(expected.getUserId(), actual.getUserId());
            assertEquals(expected.getVoucherId(), actual.getVoucherId());
            assertEquals(expected.getCreateTime(), actual.getCreateTime());
            assertEquals(TEST_TOPIC, sendResult.getRecordMetadata().topic());
            consumer.commitSync();
        } finally {
            // 测试结束时关闭独立 ProducerFactory，不影响应用正式 Kafka Bean。
            producerFactory.destroy();
        }
    }

    /**
     * 验证正式切换链路：秒杀入口在 Kafka 模式下投递，真实 broker 传输后由
     * 正式 Consumer 转换订单、调用事务服务并手动确认。
     */
    @Test
    @SuppressWarnings("unchecked")
    void shouldCompleteBusinessSwitchFlowThroughKafka() throws Exception {
        String bootstrapServers = bootstrapServers();
        checkKafkaHealthAndCreateTopic(bootstrapServers, BUSINESS_SWITCH_TEST_TOPIC);

        DefaultKafkaProducerFactory<String, VoucherOrderMessage> producerFactory =
                createProducerFactory(bootstrapServers);
        KafkaTemplate<String, VoucherOrderMessage> kafkaTemplate =
                new KafkaTemplate<>(producerFactory);
        VoucherOrderKafkaProducer kafkaProducer =
                new VoucherOrderKafkaProducer(kafkaTemplate, BUSINESS_SWITCH_TEST_TOPIC);

        RedisIdWorker redisIdWorker = mock(RedisIdWorker.class);
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        VoucherOrderTransactionalService transactionalService =
                mock(VoucherOrderTransactionalService.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        VoucherOrderServiceImpl entryService = new VoucherOrderServiceImpl();
        long orderId = System.currentTimeMillis();
        long userId = 920260907L;
        long voucherId = 930260907L;

        ReflectionTestUtils.setField(entryService, "redisIdWorker", redisIdWorker);
        ReflectionTestUtils.setField(entryService, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(entryService, "kafkaProducer", kafkaProducer);
        ReflectionTestUtils.setField(entryService, "messageMode", "kafka");
        when(redisIdWorker.nextId("order")).thenReturn(orderId);
        doReturn(0L).when(redisTemplate).execute(
                any(DefaultRedisScript.class),
                eq(Collections.emptyList()),
                any(), any(), any(), any());

        UserDTO user = new UserDTO();
        user.setId(userId);
        UserHolder.saveUser(user);

        try (Consumer<String, VoucherOrderMessage> consumer = createConsumer(bootstrapServers)) {
            consumer.subscribe(Collections.singleton(BUSINESS_SWITCH_TEST_TOPIC));

            Result result = entryService.seckillVoucher(voucherId);
            ConsumerRecord<String, VoucherOrderMessage> actualRecord =
                    consumeExpectedRecord(consumer, orderId);
            assertNotNull(actualRecord, "Kafka 未收到秒杀入口发送的订单消息");

            VoucherOrderKafkaConsumer businessConsumer =
                    new VoucherOrderKafkaConsumer(transactionalService);
            businessConsumer.listen(actualRecord, acknowledgment);

            assertTrue(result.getSuccess());
            assertEquals(orderId, result.getData());
            ArgumentCaptor<VoucherOrder> orderCaptor =
                    ArgumentCaptor.forClass(VoucherOrder.class);
            verify(transactionalService).createVoucherOrder(orderCaptor.capture());
            verify(acknowledgment).acknowledge();
            assertEquals(orderId, orderCaptor.getValue().getId());
            assertEquals(userId, orderCaptor.getValue().getUserId());
            assertEquals(voucherId, orderCaptor.getValue().getVoucherId());
            verify(redisTemplate).execute(
                    any(DefaultRedisScript.class),
                    eq(Collections.emptyList()),
                    eq(String.valueOf(voucherId)),
                    eq(String.valueOf(userId)),
                    eq(String.valueOf(orderId)),
                    eq("kafka"));

            // 正式 Consumer 已确认后，提交本测试专用消费组的 offset。
            consumer.commitSync();
        } finally {
            UserHolder.removeUser();
            producerFactory.destroy();
        }
    }

    /** 使用 AdminClient 验证集群可达，并按需创建独立联调 Topic。 */
    private void checkKafkaHealthAndCreateTopic(String bootstrapServers, String topic)
            throws Exception {
        Map<String, Object> adminProperties = new HashMap<>();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(adminProperties)) {
            String clusterId = adminClient.describeCluster().clusterId()
                    .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Collection<Node> nodes = adminClient.describeCluster().nodes()
                    .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertFalse(nodes.isEmpty(), "Kafka 集群没有可用 broker");
            log.info(
                    "KAFKA_INTEGRATION_HEALTH_OK clusterId={}, brokerCount={}, bootstrapServers={}",
                    clusterId,
                    nodes.size(),
                    bootstrapServers);

            if (!adminClient.listTopics().names()
                    .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .contains(topic)) {
                NewTopic testTopic = new NewTopic(topic, 1, (short) 1);
                adminClient.createTopics(Collections.singleton(testTopic)).all()
                        .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                log.info("KAFKA_INTEGRATION_TOPIC_CREATED topic={}", topic);
            }
        }
    }

    /** 创建仅供本测试使用的 Spring Kafka ProducerFactory。 */
    private DefaultKafkaProducerFactory<String, VoucherOrderMessage> createProducerFactory(
            String bootstrapServers) {
        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(
                producerProperties,
                new StringSerializer(),
                new JsonSerializer<>());
    }

    /** 创建独立消费组和 JSON 反序列化器，避免读取或提交正式消费组的 offset。 */
    private Consumer<String, VoucherOrderMessage> createConsumer(String bootstrapServers) {
        Map<String, Object> consumerProperties = new HashMap<>();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "hmdp-kafka-local-integration-" + UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        JsonDeserializer<VoucherOrderMessage> valueDeserializer =
                new JsonDeserializer<>(VoucherOrderMessage.class);
        valueDeserializer.addTrustedPackages("com.hmdp.kafka.message");
        return new DefaultKafkaConsumerFactory<>(
                consumerProperties,
                new StringDeserializer(),
                valueDeserializer).createConsumer();
    }

    /**
     * 测试 Producer 方法：使用 userId 作为 key，等待 broker 返回发送结果并记录元数据。
     */
    private SendResult<String, VoucherOrderMessage> sendTestOrderMessage(
            KafkaTemplate<String, VoucherOrderMessage> kafkaTemplate,
            VoucherOrderMessage message) throws Exception {
        SendResult<String, VoucherOrderMessage> result = kafkaTemplate.send(
                        TEST_TOPIC,
                        String.valueOf(message.getUserId()),
                        message)
                .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        log.info(
                "KAFKA_INTEGRATION_MESSAGE_SENT topic={}, partition={}, offset={}, orderId={}, userId={}, voucherId={}",
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset(),
                message.getOrderId(),
                message.getUserId(),
                message.getVoucherId());
        return result;
    }

    /** 轮询专用 Topic，返回本次发送的原始 Kafka 记录。 */
    private ConsumerRecord<String, VoucherOrderMessage> consumeExpectedRecord(
            Consumer<String, VoucherOrderMessage> consumer,
            Long expectedOrderId) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(CONSUME_TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, VoucherOrderMessage> records =
                    consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, VoucherOrderMessage> record : records) {
                VoucherOrderMessage message = record.value();
                if (message != null && expectedOrderId.equals(message.getOrderId())) {
                    log.info(
                            "KAFKA_INTEGRATION_MESSAGE_CONSUMED topic={}, partition={}, offset={}, key={}, orderId={}, userId={}, voucherId={}",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            message.getOrderId(),
                            message.getUserId(),
                            message.getVoucherId());
                    return record;
                }
            }
        }
        return null;
    }

    /** 优先读取联调环境变量，否则连接本地 Compose 暴露的 9092 端口。 */
    private String bootstrapServers() {
        String configured = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        return configured == null || configured.trim().isEmpty()
                ? DEFAULT_BOOTSTRAP_SERVERS
                : configured;
    }
}
