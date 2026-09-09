package com.hmdp.kafka.consumer;

import com.hmdp.kafka.message.VoucherOrderMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** 验证 DLT 消费者记录失败消息后确认 offset。 */
class VoucherOrderDltConsumerTest {

    /** DLT 记录含异常原因时应完成日志处理并确认。 */
    @Test
    void shouldAcknowledgeAfterRecordingFailedOrder() {
        VoucherOrderDltConsumer consumer = new VoucherOrderDltConsumer();
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        VoucherOrderMessage message = new VoucherOrderMessage(
                1001L,
                2001L,
                3001L,
                LocalDateTime.of(2026, 9, 7, 10, 0));
        ConsumerRecord<String, VoucherOrderMessage> record = new ConsumerRecord<>(
                "hmdp.seckill.order.dlt.v1",
                3,
                30L,
                "2001",
                message);
        record.headers().add(
                KafkaHeaders.DLT_EXCEPTION_MESSAGE,
                "database unavailable".getBytes(StandardCharsets.UTF_8));

        consumer.listen(record, acknowledgment);

        verify(acknowledgment).acknowledge();
    }
}
