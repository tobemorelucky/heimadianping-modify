package com.hmdp.service.impl;

import com.hmdp.dto.AiCouponStatisticsDTO;
import com.hmdp.dto.AiOrderStatisticsDTO;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.AiBusinessStatisticsService;
import com.hmdp.service.IShopService;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;

/** 使用现有业务数据为 AI 提供只读聚合统计。 */
@Service
public class AiBusinessStatisticsServiceImpl implements AiBusinessStatisticsService {

    private static final int STATISTICS_DAYS = 7;

    private final IShopService shopService;
    private final VoucherOrderMapper voucherOrderMapper;

    /** 注入店铺业务服务和订单 Mapper，不向 Python 暴露数据库连接。 */
    public AiBusinessStatisticsServiceImpl(
            IShopService shopService,
            VoucherOrderMapper voucherOrderMapper
    ) {
        this.shopService = shopService;
        this.voucherOrderMapper = voucherOrderMapper;
    }

    @Override
    public AiOrderStatisticsDTO getRecentSevenDayOrderStatistics(Long shopId) {
        if (!shopExists(shopId)) {
            return null;
        }

        // 使用调用时刻作为七天窗口终点，避免 Controller 拼装业务查询条件。
        LocalDateTime endTime = LocalDateTime.now();
        LocalDateTime startTime = endTime.minusDays(STATISTICS_DAYS);
        AiOrderStatisticsDTO statistics = voucherOrderMapper
                .queryOrderStatistics(shopId, startTime, endTime);
        return statistics == null
                ? new AiOrderStatisticsDTO(0L, BigDecimal.ZERO.setScale(2))
                : statistics;
    }

    @Override
    public AiCouponStatisticsDTO getCouponStatistics(Long shopId) {
        if (!shopExists(shopId)) {
            return null;
        }

        AiCouponStatisticsDTO statistics = voucherOrderMapper
                .queryCouponStatistics(shopId);
        if (statistics == null) {
            statistics = new AiCouponStatisticsDTO(0L, 0L, null);
        }

        long issuedCount = statistics.getIssuedCount() == null
                ? 0L : statistics.getIssuedCount();
        long usedCount = statistics.getUsedCount() == null
                ? 0L : statistics.getUsedCount();
        BigDecimal usageRate = issuedCount == 0L
                ? BigDecimal.ZERO.setScale(2)
                : BigDecimal.valueOf(usedCount)
                .multiply(BigDecimal.valueOf(100))
                .divide(BigDecimal.valueOf(issuedCount), 2, RoundingMode.HALF_UP);

        statistics.setIssuedCount(issuedCount);
        statistics.setUsedCount(usedCount);
        statistics.setUsageRate(usageRate);
        return statistics;
    }

    /** 统一处理非法 ID 和不存在店铺，避免执行无意义聚合查询。 */
    private boolean shopExists(Long shopId) {
        return shopId != null && shopId > 0 && shopService.getById(shopId) != null;
    }
}
