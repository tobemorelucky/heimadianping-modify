package com.hmdp.controller;

import com.hmdp.dto.ShopDTO;
import com.hmdp.elasticsearch.service.ShopSearchService;
import com.hmdp.service.IShopService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 验证名称搜索切换后原 HTTP 路径、参数和响应字段保持兼容。 */
class ShopControllerSearchTest {

    private ShopSearchService shopSearchService;
    private MockMvc mockMvc;

    /** 使用独立 Controller 测试，避免启动外部基础设施。 */
    @BeforeEach
    void setUp() {
        ShopController controller = new ShopController();
        controller.shopService = mock(IShopService.class);
        shopSearchService = mock(ShopSearchService.class);
        ReflectionTestUtils.setField(controller, "shopSearchService", shopSearchService);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    /** `/shop/of/name` 仍接收原参数并返回字段兼容的 Result 数据。 */
    @Test
    void shouldKeepShopNameSearchContract() throws Exception {
        ShopDTO shop = new ShopDTO();
        shop.setId(5L);
        shop.setName("海底捞火锅");
        shop.setImages("shop-5.jpg");
        shop.setAvgPrice(88L);
        when(shopSearchService.searchByName("火锅", 2))
                .thenReturn(Collections.singletonList(shop));

        mockMvc.perform(get("/shop/of/name")
                        .param("name", "火锅")
                        .param("current", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data[0].id").value(5))
                .andExpect(jsonPath("$.data[0].name").value("海底捞火锅"))
                .andExpect(jsonPath("$.data[0].images").value("shop-5.jpg"))
                .andExpect(jsonPath("$.data[0].avgPrice").value(88));

        verify(shopSearchService).searchByName("火锅", 2);
    }
}
