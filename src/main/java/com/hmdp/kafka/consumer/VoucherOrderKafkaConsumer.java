package com.hmdp.kafka.consumer;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.message.VoucherOrderMessage;
import com.hmdp.service.impl.VoucherOrderTransactionalService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 秒杀订单 Kafka Consumer。
 *
 * <p>消息参数校验和数据库事务成功后才手动确认 offset；失败时抛出异常，
 * 由对应监听容器转入 Retry Topic 或 DLT。</p>
 */
@Slf4j
@Component
public class VoucherOrderKafkaConsumer {

    private final VoucherOrderTransactionalService transactionalService;

    /**
     * 注入现有秒杀订单数据库事务服务。
     */
    public VoucherOrderKafkaConsumer(VoucherOrderTransactionalService transactionalService) {
        this.transactionalService = transactionalService;
    }

    /**
     * 消费秒杀订单消息，事务成功后手动确认 offset。
     */
    @KafkaListener(
            topics = "${hmdp.kafka.topics.voucher-order:hmdp.seckill.order.create.v1}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "voucherOrderKafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, VoucherOrderMessage> record,
                       Acknowledgment acknowledgment) {
        consume(record, acknowledgment);
    }

    /**
     * 消费 Retry Topic；复用与主 Topic 完全相同的校验、事务和确认边界。
     */
    @KafkaListener(
            topics = "${hmdp.kafka.topics.voucher-order-retry:hmdp.seckill.order.retry.v1}",
            groupId = "${hmdp.kafka.consumer.retry-group-id:hmdp-seckill-order-retry-v1}",
            containerFactory = "voucherOrderRetryKafkaListenerContainerFactory")
    public void listenRetry(ConsumerRecord<String, VoucherOrderMessage> record,
                            Acknowledgment acknowledgment) {
        consume(record, acknowledgment);
    }

    /** 执行主 Topic 与 Retry Topic 共享的订单消费逻辑。 */
    private void consume(ConsumerRecord<String, VoucherOrderMessage> record,
                         Acknowledgment acknowledgment) {
        VoucherOrderMessage message = record.value();
        try {
            validateMessage(message);
            VoucherOrder voucherOrder = toVoucherOrder(message);

            // 事务服务内部执行一人一单幂等检查，并原子完成库存扣减与订单插入。
            transactionalService.createVoucherOrder(voucherOrder);

            // 只有业务事务正常完成或被事务服务判定为幂等成功后才允许提交 offset。
            acknowledgment.acknowledge();
            log.info(
                    "Kafka秒杀订单消费成功并已确认, orderId={}, userId={}, voucherId={}, topic={}, partition={}, offset={}",
                    message.getOrderId(),
                    message.getUserId(),
                    message.getVoucherId(),
                    record.topic(),
                    record.partition(),
                    record.offset());
        } catch (RuntimeException exception) {
            // 监听方法不确认 offset；继续抛出异常，交由对应容器的有限重试与转发策略处理。
            log.error(
                    "Kafka秒杀订单消费失败，交由错误处理器处理, orderId={}, userId={}, voucherId={}, topic={}, partition={}, offset={}",
                    message == null ? null : message.getOrderId(),
                    message == null ? null : message.getUserId(),
                    message == null ? null : message.getVoucherId(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    exception);
            throw exception;
        }
    }

    /**
     * 校验创建订单所需的全部消息字段，空消息不能进入事务或被确认。
     */
    private void validateMessage(VoucherOrderMessage message) {
        if (message == null
                || message.getOrderId() == null
                || message.getUserId() == null
                || message.getVoucherId() == null
                || message.getCreateTime() == null) {
            throw new IllegalArgumentException("Kafka秒杀订单消息字段不完整");
        }
    }

    /**
     * 将 Kafka DTO 转换为现有事务服务使用的订单实体。
     */
    private VoucherOrder toVoucherOrder(VoucherOrderMessage message) {
        return new VoucherOrder()
                .setId(message.getOrderId())
                .setUserId(message.getUserId())
                .setVoucherId(message.getVoucherId())
                .setCreateTime(message.getCreateTime());
    }
}
