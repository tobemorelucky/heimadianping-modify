package com.hmdp.config.kafka;

import com.hmdp.kafka.message.VoucherOrderMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 验证主 Topic、Retry Topic 和 DLT 之间的失败路由。 */
class KafkaConsumerRetryRoutingTest {

    private KafkaConsumerConfig config;
    private KafkaTemplate<String, VoucherOrderMessage> kafkaTemplate;

    /** 每个用例使用独立的配置和 KafkaTemplate mock。 */
    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        config = new KafkaConsumerConfig("127.0.0.1:9092", "test-group", "earliest");
        kafkaTemplate = mock(KafkaTemplate.class);
        SettableListenableFuture<SendResult<String, VoucherOrderMessage>> completedFuture =
                new SettableListenableFuture<>();
        completedFuture.set(null);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(completedFuture);
    }

    /** 主 Topic 第一次处理失败必须离开原分区并进入 Retry Topic。 */
    @Test
    void shouldRouteMainFailureToRetryTopic() {
        DeadLetterPublishingRecoverer recoverer = config.voucherOrderMainRecoverer(
                kafkaTemplate,
                "hmdp.seckill.order.retry.v1");

        recoverer.accept(record("hmdp.seckill.order.create.v1"), new IllegalStateException("temporary"));

        ProducerRecord<?, ?> forwarded = capturedForwardedRecord();
        assertEquals("hmdp.seckill.order.retry.v1", forwarded.topic());
        assertEquals(Integer.valueOf(3), forwarded.partition());
    }

    /** Retry Topic 有限重试耗尽后必须进入 DLT。 */
    @Test
    void shouldRouteExhaustedRetryToDlt() {
        DeadLetterPublishingRecoverer recoverer = config.voucherOrderRetryRecoverer(
                kafkaTemplate,
                "hmdp.seckill.order.dlt.v1");

        recoverer.accept(record("hmdp.seckill.order.retry.v1"), new IllegalStateException("persistent"));

        ProducerRecord<?, ?> forwarded = capturedForwardedRecord();
        assertEquals("hmdp.seckill.order.dlt.v1", forwarded.topic());
        assertEquals(Integer.valueOf(3), forwarded.partition());
        assertEquals(1000L, KafkaConsumerConfig.RETRY_BACK_OFF_MILLIS);
        assertEquals(1L, KafkaConsumerConfig.RETRY_MAX_ATTEMPTS);
    }

    /** 下一级 Topic 发送失败时恢复器必须抛错，阻止源 offset 被错误提交。 */
    @Test
    void shouldFailRecoveryWhenForwardingFails() {
        SettableListenableFuture<SendResult<String, VoucherOrderMessage>> failedFuture =
                new SettableListenableFuture<>();
        failedFuture.setException(new IllegalStateException("broker unavailable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failedFuture);
        DeadLetterPublishingRecoverer recoverer = config.voucherOrderMainRecoverer(
                kafkaTemplate,
                "hmdp.seckill.order.retry.v1");

        assertThrows(
                IllegalStateException.class,
                () -> recoverer.accept(
                        record("hmdp.seckill.order.create.v1"),
                        new IllegalStateException("database unavailable")));
    }

    /** 捕获恢复器发送的消息。 */
    private ProducerRecord<?, ?> capturedForwardedRecord() {
        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        return captor.getValue();
    }

    /** 构造保持 userId key 和分区不变的失败消息。 */
    private ConsumerRecord<String, VoucherOrderMessage> record(String topic) {
        VoucherOrderMessage message = new VoucherOrderMessage(
                1001L,
                2001L,
                3001L,
                LocalDateTime.of(2026, 9, 7, 10, 0));
        return new ConsumerRecord<>(topic, 3, 20L, "2001", message);
    }
}
