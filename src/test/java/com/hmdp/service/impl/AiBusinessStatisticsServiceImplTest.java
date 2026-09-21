package com.hmdp.service.impl;

import com.hmdp.dto.AiCouponStatisticsDTO;
import com.hmdp.dto.AiOrderStatisticsDTO;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IShopService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 验证 AI 经营统计服务的查询边界与使用率计算。 */
class AiBusinessStatisticsServiceImplTest {

    private IShopService shopService;
    private VoucherOrderMapper voucherOrderMapper;
    private AiBusinessStatisticsServiceImpl service;

    /** 每个测试使用隔离的业务服务和 Mapper mock。 */
    @BeforeEach
    void setUp() {
        shopService = mock(IShopService.class);
        voucherOrderMapper = mock(VoucherOrderMapper.class);
        service = new AiBusinessStatisticsServiceImpl(shopService, voucherOrderMapper);
    }

    /** 订单统计必须使用七天窗口并返回数据库聚合结果。 */
    @Test
    void shouldQueryRecentSevenDayOrderStatistics() {
        when(shopService.getById(1L)).thenReturn(new Shop().setId(1L));
        when(voucherOrderMapper.queryOrderStatistics(
                eq(1L), any(LocalDateTime.class), any(LocalDateTime.class)
        )).thenReturn(new AiOrderStatisticsDTO(3L, new BigDecimal("88.50")));

        AiOrderStatisticsDTO result = service
                .getRecentSevenDayOrderStatistics(1L);

        assertEquals(3L, result.getOrderCount());
        assertEquals(new BigDecimal("88.50"), result.getTransactionAmount());
        verify(voucherOrderMapper).queryOrderStatistics(
                eq(1L), any(LocalDateTime.class), any(LocalDateTime.class)
        );
    }

    /** 优惠券使用率以百分比计算并保留两位小数。 */
    @Test
    void shouldCalculateCouponUsageRate() {
        when(shopService.getById(1L)).thenReturn(new Shop().setId(1L));
        when(voucherOrderMapper.queryCouponStatistics(1L))
                .thenReturn(new AiCouponStatisticsDTO(3L, 1L, null));

        AiCouponStatisticsDTO result = service.getCouponStatistics(1L);

        assertEquals(3L, result.getIssuedCount());
        assertEquals(1L, result.getUsedCount());
        assertEquals(new BigDecimal("33.33"), result.getUsageRate());
    }

    /** 店铺不存在时不得访问统计 Mapper。 */
    @Test
    void shouldNotQueryStatisticsWhenShopDoesNotExist() {
        when(shopService.getById(999L)).thenReturn(null);

        assertNull(service.getCouponStatistics(999L));

        verify(voucherOrderMapper, never()).queryCouponStatistics(999L);
    }
}
