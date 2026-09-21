package com.hmdp.controller;

import com.hmdp.dto.AiCouponStatisticsDTO;
import com.hmdp.service.AiBusinessStatisticsService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 验证 AI 优惠券统计接口的路径和响应协议。 */
class AiCouponStatisticsControllerTest {

    private AiBusinessStatisticsService statisticsService;
    private MockMvc mockMvc;

    /** 使用独立 Controller 测试，不连接数据库。 */
    @BeforeEach
    void setUp() {
        statisticsService = mock(AiBusinessStatisticsService.class);
        mockMvc = MockMvcBuilders.standaloneSetup(
                new AiCouponStatisticsController(statisticsService)
        ).build();
    }

    /** 正常返回优惠券发放量、使用量和百分比使用率。 */
    @Test
    void shouldReturnCouponStatistics() throws Exception {
        when(statisticsService.getCouponStatistics(1L))
                .thenReturn(new AiCouponStatisticsDTO(
                        20L, 5L, new BigDecimal("25.00")
                ));

        mockMvc.perform(get("/api/ai/shop/1/coupons"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.issuedCount").value(20))
                .andExpect(jsonPath("$.data.usedCount").value(5))
                .andExpect(jsonPath("$.data.usageRate").value(25.00));

        verify(statisticsService).getCouponStatistics(1L);
    }

    /** 店铺不存在时返回统一失败结果。 */
    @Test
    void shouldReturnFailureWhenShopDoesNotExist() throws Exception {
        when(statisticsService.getCouponStatistics(999L)).thenReturn(null);

        mockMvc.perform(get("/api/ai/shop/999/coupons"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.errorMsg").value("店铺不存在"));
    }
}
