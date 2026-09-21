package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/** AI 运营助手使用的近七天订单统计结果。 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AiOrderStatisticsDTO {

    /** 排除取消、退款流程订单后的订单数量。 */
    private Long orderCount;

    /** 按优惠券支付金额汇总的交易金额，单位为元。 */
    private BigDecimal transactionAmount;
}
