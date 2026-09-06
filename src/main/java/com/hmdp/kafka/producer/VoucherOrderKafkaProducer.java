package com.hmdp.kafka.producer;

import com.hmdp.kafka.message.VoucherOrderMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * 秒杀订单 Kafka Producer。
 */
@Slf4j
@Component
public class VoucherOrderKafkaProducer {

    private final KafkaTemplate<String, VoucherOrderMessage> kafkaTemplate;
    private final String topic;

    /**
     * 注入订单消息模板和目标 Topic。
     */
    public VoucherOrderKafkaProducer(
            KafkaTemplate<String, VoucherOrderMessage> voucherOrderKafkaTemplate,
            @Value("${hmdp.kafka.topics.voucher-order}") String topic) {
        this.kafkaTemplate = voucherOrderKafkaTemplate;
        this.topic = topic;
    }

    /**
     * 异步发送订单消息，并记录 broker 确认或发送失败信息。
     *
     * @param message 秒杀订单消息
     * @return 可供后续业务阶段继续处理发送结果的 Future
     */
    public ListenableFuture<SendResult<String, VoucherOrderMessage>> sendOrderMessage(
            VoucherOrderMessage message) {
        Assert.notNull(message, "voucher order message must not be null");
        Assert.notNull(message.getUserId(), "voucher order message userId must not be null");

        String messageKey = String.valueOf(message.getUserId());
        try {
            ListenableFuture<SendResult<String, VoucherOrderMessage>> future =
                    kafkaTemplate.send(topic, messageKey, message);
            future.addCallback(
                    result -> log.info(
                            "Kafka秒杀订单消息发送成功, orderId={}, userId={}, voucherId={}, topic={}, partition={}, offset={}",
                            message.getOrderId(),
                            message.getUserId(),
                            message.getVoucherId(),
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset()),
                    throwable -> log.error(
                            "Kafka秒杀订单消息异步发送失败, orderId={}, userId={}, voucherId={}, topic={}",
                            message.getOrderId(),
                            message.getUserId(),
                            message.getVoucherId(),
                            topic,
                            throwable));
            return future;
        } catch (RuntimeException exception) {
            // 同步异常通常发生在序列化或 Producer 初始化阶段，记录后继续抛给调用方。
            log.error(
                    "Kafka秒杀订单消息发送异常, orderId={}, userId={}, voucherId={}, topic={}",
                    message.getOrderId(),
                    message.getUserId(),
                    message.getVoucherId(),
                    topic,
                    exception);
            throw exception;
        }
    }
}
