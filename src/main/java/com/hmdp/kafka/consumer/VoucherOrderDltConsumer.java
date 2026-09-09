package com.hmdp.kafka.consumer;

import com.hmdp.kafka.message.VoucherOrderMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

/**
 * 秒杀订单 DLT 消费者。
 *
 * <p>当前阶段只负责持久化日志语义的失败记录，不自动补单；人工确认原因后再决定重放。</p>
 */
@Slf4j
@Component
public class VoucherOrderDltConsumer {

    /**
     * 记录进入 DLT 的订单、最后一次异常和处理时间，日志成功后确认 DLT offset。
     */
    @KafkaListener(
            topics = "${hmdp.kafka.topics.voucher-order-dlt:hmdp.seckill.order.dlt.v1}",
            groupId = "${hmdp.kafka.consumer.dlt-group-id:hmdp-seckill-order-dlt-log-v1}",
            containerFactory = "voucherOrderDltKafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, VoucherOrderMessage> record,
                       Acknowledgment acknowledgment) {
        VoucherOrderMessage message = record.value();
        String failureReason = readHeader(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        LocalDateTime failureRecordedAt = LocalDateTime.now();

        log.error(
                "Kafka秒杀失败订单进入DLT, orderId={}, userId={}, voucherId={}, failureReason={}, failureRecordedAt={}, topic={}, partition={}, offset={}",
                message == null ? null : message.getOrderId(),
                message == null ? null : message.getUserId(),
                message == null ? null : message.getVoucherId(),
                failureReason,
                failureRecordedAt,
                record.topic(),
                record.partition(),
                record.offset());

        // 失败信息完成日志记录后再确认，避免静默丢弃 DLT 记录。
        acknowledgment.acknowledge();
    }

    /** 读取错误处理器写入的 UTF-8 异常消息头。 */
    private String readHeader(ConsumerRecord<String, VoucherOrderMessage> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return header == null ? "unknown" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
