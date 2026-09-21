package com.hmdp.controller;

import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.service.IShopService;
import com.hmdp.service.IShopTypeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 验证 AI 店铺接口的路径、字段白名单和缺失数据响应。 */
class AiShopControllerTest {

    private IShopService shopService;
    private IShopTypeService shopTypeService;
    private MockMvc mockMvc;

    /** 使用独立 Controller 测试，不启动数据库或其他中间件。 */
    @BeforeEach
    void setUp() {
        shopService = mock(IShopService.class);
        shopTypeService = mock(IShopTypeService.class);
        mockMvc = MockMvcBuilders.standaloneSetup(
                new AiShopController(shopService, shopTypeService)
        ).build();
    }

    /** 成功响应只包含首期 Tool 约定的四个业务字段。 */
    @Test
    void shouldReturnMinimalShopInfo() throws Exception {
        Shop shop = new Shop()
                .setId(1L)
                .setName("测试店铺")
                .setTypeId(2L)
                .setAddress("测试路 1 号")
                .setImages("must-not-be-exposed.jpg");
        ShopType shopType = new ShopType().setId(2L).setName("美食");
        when(shopService.getById(1L)).thenReturn(shop);
        when(shopTypeService.getById(2L)).thenReturn(shopType);

        mockMvc.perform(get("/api/ai/shop/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.id").value(1))
                .andExpect(jsonPath("$.data.name").value("测试店铺"))
                .andExpect(jsonPath("$.data.type").value("美食"))
                .andExpect(jsonPath("$.data.address").value("测试路 1 号"))
                .andExpect(jsonPath("$.data.images").doesNotExist());

        verify(shopService).getById(1L);
        verify(shopTypeService).getById(2L);
    }

    /** 店铺不存在时保持统一 Result 错误协议。 */
    @Test
    void shouldReturnFailureWhenShopDoesNotExist() throws Exception {
        when(shopService.getById(999L)).thenReturn(null);

        mockMvc.perform(get("/api/ai/shop/999"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.errorMsg").value("店铺不存在"))
                .andExpect(jsonPath("$.data").doesNotExist());

        verify(shopService).getById(999L);
    }
}
