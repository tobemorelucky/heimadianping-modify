package com.hmdp.kafka.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 秒杀订单 Kafka 消息。
 *
 * <p>Phase 1 仅用于验证 Kafka 基础设施，不改变现有订单处理流程。</p>
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
