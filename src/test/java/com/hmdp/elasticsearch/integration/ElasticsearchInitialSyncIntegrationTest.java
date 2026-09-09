package com.hmdp.elasticsearch.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.elasticsearch.service.ShopEsSyncService;
import com.hmdp.mapper.ShopMapper;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本地 Elasticsearch 首次同步集成测试。
 *
 * <p>仅在显式开启环境变量时运行，会创建 shop_index 并写入当前 MySQL 店铺数据。</p>
 */
@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfEnvironmentVariable(named = "ES_INTEGRATION_TEST_ENABLED", matches = "true")
class ElasticsearchInitialSyncIntegrationTest {

    @Autowired
    private ShopEsSyncService syncService;

    @Autowired
    private ShopMapper shopMapper;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    @Qualifier("elasticsearchRestTemplate")
    private RestTemplate elasticsearchRestTemplate;

    /** 验证本地 Elasticsearch 根 API 可以正常响应。 */
    @Test
    @Order(1)
    void shouldConnectToElasticsearch() {
        assertTrue(syncService.isElasticsearchAvailable());
    }

    /** 验证 shop_index 可以创建或已存在。 */
    @Test
    @Order(2)
    void shouldCreateShopIndex() {
        syncService.createShopIndex();
        assertTrue(syncService.indexExists());
    }

    /** 验证 MySQL 全部店铺写入 ES，并可通过 _count 立即查询。 */
    @Test
    @Order(3)
    void shouldSyncAllMysqlShopsToElasticsearch() throws Exception {
        int expectedCount = shopMapper.selectCount(null);

        int syncedCount = syncService.syncAllShops();
        ResponseEntity<String> countResponse = elasticsearchRestTemplate.getForEntity(
                "/" + ShopEsSyncService.SHOP_INDEX + "/_count",
                String.class);
        JsonNode responseBody = objectMapper.readTree(countResponse.getBody());

        assertEquals(expectedCount, syncedCount);
        assertTrue(responseBody.path("count").asInt() >= expectedCount);
    }
}
