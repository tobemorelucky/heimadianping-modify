package com.hmdp.controller;

import com.hmdp.dto.AiCouponStatisticsDTO;
import com.hmdp.dto.Result;
import com.hmdp.service.AiBusinessStatisticsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** AI 运营助手专用的只读优惠券统计接口。 */
@RestController
@RequestMapping("/api/ai/shop")
public class AiCouponStatisticsController {

    private final AiBusinessStatisticsService statisticsService;

    /** 注入只读经营统计服务。 */
    public AiCouponStatisticsController(AiBusinessStatisticsService statisticsService) {
        this.statisticsService = statisticsService;
    }

    /** 返回指定店铺优惠券的累计发放、核销数量和使用率。 */
    @GetMapping("/{id}/coupons")
    public Result getCouponStatistics(@PathVariable("id") Long id) {
        AiCouponStatisticsDTO statistics = statisticsService
                .getCouponStatistics(id);
        return statistics == null
                ? Result.fail("店铺不存在")
                : Result.ok(statistics);
    }
}
