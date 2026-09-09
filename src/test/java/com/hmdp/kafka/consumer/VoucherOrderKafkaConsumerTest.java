package com.hmdp.kafka.consumer;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.message.VoucherOrderMessage;
import com.hmdp.service.impl.VoucherOrderTransactionalService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * 验证 Kafka Consumer 的参数校验、订单服务调用和手动确认边界。
 */
@ExtendWith(MockitoExtension.class)
class VoucherOrderKafkaConsumerTest {

    private static final String TOPIC = "hmdp.seckill.order.create.v1";

    @Mock
    private VoucherOrderTransactionalService transactionalService;

    @Mock
    private Acknowledgment acknowledgment;

    private VoucherOrderKafkaConsumer consumer;

    /**
     * 每个测试使用独立 Consumer，避免共享调用状态。
     */
    @BeforeEach
    void setUp() {
        consumer = new VoucherOrderKafkaConsumer(transactionalService);
    }

    /**
     * 有效消息必须先调用订单事务服务，再确认 Kafka offset。
     */
    @Test
    void shouldCreateOrderBeforeAcknowledgingMessage() {
        LocalDateTime createTime = LocalDateTime.of(2026, 9, 6, 22, 30);
        VoucherOrderMessage message = new VoucherOrderMessage(1001L, 2001L, 3001L, createTime);
        ConsumerRecord<String, VoucherOrderMessage> record = record(message);
        ArgumentCaptor<VoucherOrder> orderCaptor = ArgumentCaptor.forClass(VoucherOrder.class);
        InOrder inOrder = inOrder(transactionalService, acknowledgment);

        consumer.listen(record, acknowledgment);

        inOrder.verify(transactionalService).createVoucherOrder(orderCaptor.capture());
        inOrder.verify(acknowledgment).acknowledge();
        VoucherOrder order = orderCaptor.getValue();
        assertEquals(message.getOrderId(), order.getId());
        assertEquals(message.getUserId(), order.getUserId());
        assertEquals(message.getVoucherId(), order.getVoucherId());
        assertEquals(message.getCreateTime(), order.getCreateTime());
    }

    /**
     * 字段不完整的消息不能调用订单服务，也不能提交 offset。
     */
    @Test
    void shouldRejectInvalidMessageWithoutAcknowledging() {
        VoucherOrderMessage message = new VoucherOrderMessage(1001L, null, 3001L, LocalDateTime.now());

        assertThrows(IllegalArgumentException.class, () -> consumer.listen(record(message), acknowledgment));

        verify(transactionalService, never()).createVoucherOrder(any());
        verify(acknowledgment, never()).acknowledge();
    }

    /**
     * 反序列化失败产生的空消息不能调用订单服务，也不能提交 offset。
     */
    @Test
    void shouldRejectNullMessageWithoutAcknowledging() {
        assertThrows(IllegalArgumentException.class, () -> consumer.listen(record(null), acknowledgment));

        verify(transactionalService, never()).createVoucherOrder(any());
        verify(acknowledgment, never()).acknowledge();
    }

    /**
     * 订单事务异常必须继续抛出给重试机制，并且不能提交 offset。
     */
    @Test
    void shouldNotAcknowledgeWhenOrderServiceFails() {
        VoucherOrderMessage message =
                new VoucherOrderMessage(1001L, 2001L, 3001L, LocalDateTime.now());
        doThrow(new IllegalStateException("database unavailable"))
                .when(transactionalService).createVoucherOrder(any(VoucherOrder.class));

        assertThrows(IllegalStateException.class, () -> consumer.listen(record(message), acknowledgment));

        verify(transactionalService).createVoucherOrder(any(VoucherOrder.class));
        verify(acknowledgment, never()).acknowledge();
    }

    /**
     * 临时异常在 Retry Topic 再次消费成功后，才允许确认 Retry Topic 的 offset。
     */
    @Test
    void shouldAcknowledgeRetryMessageAfterTransientFailureRecovers() {
        VoucherOrderMessage message =
                new VoucherOrderMessage(1002L, 2002L, 3002L, LocalDateTime.now());
        ConsumerRecord<String, VoucherOrderMessage> retryRecord =
                new ConsumerRecord<>("hmdp.seckill.order.retry.v1", 2, 11L, "2002", message);
        doThrow(new IllegalStateException("temporary database timeout"))
                .doNothing()
                .when(transactionalService).createVoucherOrder(any(VoucherOrder.class));

        assertThrows(
                IllegalStateException.class,
                () -> consumer.listenRetry(retryRecord, acknowledgment));
        verify(acknowledgment, never()).acknowledge();

        consumer.listenRetry(retryRecord, acknowledgment);

        verify(transactionalService, org.mockito.Mockito.times(2))
                .createVoucherOrder(any(VoucherOrder.class));
        verify(acknowledgment).acknowledge();
    }

    /**
     * 构造包含 Kafka 元数据的测试记录。
     */
    private ConsumerRecord<String, VoucherOrderMessage> record(VoucherOrderMessage message) {
        String key = message == null || message.getUserId() == null
                ? null
                : String.valueOf(message.getUserId());
        return new ConsumerRecord<>(TOPIC, 2, 10L, key, message);
    }
}
