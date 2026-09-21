package com.hmdp.service;

import com.hmdp.dto.AiCouponStatisticsDTO;
import com.hmdp.dto.AiOrderStatisticsDTO;

/** 为 AI 专用只读接口提供商家经营统计。 */
public interface AiBusinessStatisticsService {

    /**
     * 查询店铺最近七天的订单数量和交易金额。
     *
     * @param shopId 店铺 ID
     * @return 店铺不存在时返回 null
     */
    AiOrderStatisticsDTO getRecentSevenDayOrderStatistics(Long shopId);

    /**
     * 查询店铺优惠券累计发放、核销及使用率。
     *
     * @param shopId 店铺 ID
     * @return 店铺不存在时返回 null
     */
    AiCouponStatisticsDTO getCouponStatistics(Long shopId);
}
