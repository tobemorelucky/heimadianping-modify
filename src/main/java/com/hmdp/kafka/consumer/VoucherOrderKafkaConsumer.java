package com.hmdp.kafka.consumer;

import com.hmdp.kafka.message.VoucherOrderMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 秒杀订单 Kafka Consumer 的 Phase 1 空实现。
 *
 * <p>当前仅验证监听和反序列化，不调用订单业务服务。</p>
 */
@Slf4j
@Component
public class VoucherOrderKafkaConsumer {

    /**
     * 监听秒杀订单 Topic 并打印消息元数据。
     */
    @KafkaListener(
            topics = "${hmdp.kafka.topics.voucher-order}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "voucherOrderKafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, VoucherOrderMessage> record) {
        VoucherOrderMessage message = record.value();
        log.info(
                "收到Kafka秒杀订单消息, key={}, orderId={}, userId={}, voucherId={}, createTime={}, topic={}, partition={}, offset={}",
                record.key(),
                message.getOrderId(),
                message.getUserId(),
                message.getVoucherId(),
                message.getCreateTime(),
                record.topic(),
                record.partition(),
                record.offset());
    }
}
