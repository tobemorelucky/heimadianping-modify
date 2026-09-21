package com.hmdp.controller;

import com.hmdp.dto.AiOrderStatisticsDTO;
import com.hmdp.dto.Result;
import com.hmdp.service.AiBusinessStatisticsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** AI 运营助手专用的只读订单统计接口。 */
@RestController
@RequestMapping("/api/ai/shop")
public class AiOrderStatisticsController {

    private final AiBusinessStatisticsService statisticsService;

    /** 注入只读经营统计服务。 */
    public AiOrderStatisticsController(AiBusinessStatisticsService statisticsService) {
        this.statisticsService = statisticsService;
    }

    /** 返回指定店铺最近七天的订单数量和交易金额。 */
    @GetMapping("/{id}/orders")
    public Result getOrderStatistics(@PathVariable("id") Long id) {
        AiOrderStatisticsDTO statistics = statisticsService
                .getRecentSevenDayOrderStatistics(id);
        return statistics == null
                ? Result.fail("店铺不存在")
                : Result.ok(statistics);
    }
}
