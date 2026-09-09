package com.hmdp.kafka.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 秒杀订单 Kafka 消息。
 *
 * <p>由秒杀入口在 Redis Lua 准入成功后发送，消费者据此创建数据库订单。</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VoucherOrderMessage {

    /** 订单唯一标识。 */
    private Long orderId;

    /** 下单用户标识，同时作为 Kafka 消息 key。 */
    private Long userId;

    /** 秒杀优惠券标识。 */
    private Long voucherId;

    /** 消息创建时间。 */
    private LocalDateTime createTime;
}
