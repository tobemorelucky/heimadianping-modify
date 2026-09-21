package com.hmdp.controller;

import com.hmdp.dto.AiOrderStatisticsDTO;
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

/** 验证 AI 订单统计接口的路径和响应协议。 */
class AiOrderStatisticsControllerTest {

    private AiBusinessStatisticsService statisticsService;
    private MockMvc mockMvc;

    /** 使用独立 Controller 测试，不连接数据库。 */
    @BeforeEach
    void setUp() {
        statisticsService = mock(AiBusinessStatisticsService.class);
        mockMvc = MockMvcBuilders.standaloneSetup(
                new AiOrderStatisticsController(statisticsService)
        ).build();
    }

    /** 正常返回近七天订单数量和元单位交易金额。 */
    @Test
    void shouldReturnRecentOrderStatistics() throws Exception {
        when(statisticsService.getRecentSevenDayOrderStatistics(1L))
                .thenReturn(new AiOrderStatisticsDTO(12L, new BigDecimal("345.60")));

        mockMvc.perform(get("/api/ai/shop/1/orders"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.orderCount").value(12))
                .andExpect(jsonPath("$.data.transactionAmount").value(345.60));

        verify(statisticsService).getRecentSevenDayOrderStatistics(1L);
    }

    /** 店铺不存在时返回统一失败结果。 */
    @Test
    void shouldReturnFailureWhenShopDoesNotExist() throws Exception {
        when(statisticsService.getRecentSevenDayOrderStatistics(999L))
                .thenReturn(null);

        mockMvc.perform(get("/api/ai/shop/999/orders"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.errorMsg").value("店铺不存在"));
    }
}
