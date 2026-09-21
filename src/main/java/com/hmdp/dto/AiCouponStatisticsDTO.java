package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/** AI 运营助手使用的优惠券发放与核销统计结果。 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AiCouponStatisticsDTO {

    /** 以优惠券订单记录计算的累计发放数量。 */
    private Long issuedCount;

    /** 已核销的优惠券数量。 */
    private Long usedCount;

    /** 核销数量占发放数量的百分比，保留两位小数。 */
    private BigDecimal usageRate;
}
