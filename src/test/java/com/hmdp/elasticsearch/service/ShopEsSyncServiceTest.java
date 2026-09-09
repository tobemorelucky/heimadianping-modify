package com.hmdp.elasticsearch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.elasticsearch.document.ShopDocument;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/** 验证商户索引创建、模型转换和 Bulk 首次同步。 */
@ExtendWith(MockitoExtension.class)
class ShopEsSyncServiceTest {

    @Mock
    private ShopMapper shopMapper;

    private MockRestServiceServer server;
    private ShopEsSyncService syncService;

    /** 每个测试使用独立的 REST 客户端和 Mock 服务端。 */
    @BeforeEach
    void setUp() {
        RestTemplate restTemplate = new RestTemplateBuilder()
                .rootUri("http://127.0.0.1:9200")
                .build();
        server = MockRestServiceServer.bindTo(restTemplate).build();
        syncService = new ShopEsSyncService(restTemplate, new ObjectMapper(), shopMapper);
    }

    /** 根 API 返回成功时判定 Elasticsearch 连接可用。 */
    @Test
    void shouldReportElasticsearchAvailable() {
        server.expect(once(), requestTo("http://127.0.0.1:9200/"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"version\":{\"number\":\"8.19.21\"}}", MediaType.APPLICATION_JSON));

        assertTrue(syncService.isElasticsearchAvailable());
        server.verify();
    }

    /** 索引不存在时应使用显式 Mapping 创建 shop_index。 */
    @Test
    void shouldCreateShopIndexWithExplicitMapping() {
        server.expect(once(), requestTo("http://127.0.0.1:9200/shop_index"))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));
        server.expect(once(), requestTo("http://127.0.0.1:9200/shop_index"))
                .andExpect(method(HttpMethod.PUT))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("\"location\"")))
                .andRespond(withSuccess("{\"acknowledged\":true}", MediaType.APPLICATION_JSON));

        assertTrue(syncService.createShopIndex());
        server.verify();
    }

    /** MySQL 店铺必须转换经纬度并以 NDJSON Bulk 写入 Elasticsearch。 */
    @Test
    void shouldBulkSyncAllMysqlShops() {
        Shop first = shop(1L, "海底捞火锅", 1L, 120.157780, 30.310633);
        Shop second = shop(2L, "星聚会KTV", 2L, 120.128958, 30.337252);
        when(shopMapper.selectList(null)).thenReturn(Arrays.asList(first, second));

        server.expect(once(), requestTo("http://127.0.0.1:9200/shop_index"))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withSuccess());
        server.expect(once(), requestTo("http://127.0.0.1:9200/_bulk"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().contentType("application/x-ndjson"))
                .andExpect(content().string(org.hamcrest.Matchers.allOf(
                        org.hamcrest.Matchers.containsString("\"_id\":\"1\""),
                        org.hamcrest.Matchers.containsString("\"lat\":30.310633"),
                        org.hamcrest.Matchers.containsString("\"lon\":120.15778"))))
                .andRespond(withSuccess("{\"errors\":false,\"items\":[]}", MediaType.APPLICATION_JSON));
        server.expect(once(), requestTo("http://127.0.0.1:9200/shop_index/_refresh"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess("{\"_shards\":{\"successful\":1}}", MediaType.APPLICATION_JSON));

        int syncedCount = syncService.syncAllShops();

        assertEquals(2, syncedCount);
        ShopDocument document = syncService.toDocument(first);
        assertEquals(first.getY(), document.getLocation().getLat());
        assertEquals(first.getX(), document.getLocation().getLon());
        server.verify();
    }

    /** 构造同步测试所需的最小店铺数据。 */
    private Shop shop(Long id, String name, Long typeId, Double longitude, Double latitude) {
        return new Shop()
                .setId(id)
                .setName(name)
                .setTypeId(typeId)
                .setArea("测试商圈")
                .setAddress("测试地址")
                .setX(longitude)
                .setY(latitude);
    }
}
