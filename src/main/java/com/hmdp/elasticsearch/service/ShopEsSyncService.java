package com.hmdp.elasticsearch.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.elasticsearch.document.ShopDocument;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 商户 Elasticsearch 首次全量同步服务。
 *
 * <p>该服务独立于 ShopController 和现有 ShopService 查询链路，只负责创建索引并把
 * MySQL 店铺数据批量写入 Elasticsearch。</p>
 */
@Slf4j
@Service
public class ShopEsSyncService {

    /** 商户搜索索引名。 */
    public static final String SHOP_INDEX = "shop_index";

    /** 单次 Bulk 写入数量，限制请求体和 JVM 临时内存占用。 */
    private static final int BULK_BATCH_SIZE = 500;

    private static final MediaType NDJSON_MEDIA_TYPE =
            MediaType.parseMediaType("application/x-ndjson");

    private final RestTemplate elasticsearchRestTemplate;
    private final ObjectMapper objectMapper;
    private final ShopMapper shopMapper;

    /** 注入 Elasticsearch REST 客户端、JSON 序列化器和店铺 Mapper。 */
    public ShopEsSyncService(
            @Qualifier("elasticsearchRestTemplate") RestTemplate elasticsearchRestTemplate,
            ObjectMapper objectMapper,
            ShopMapper shopMapper) {
        this.elasticsearchRestTemplate = elasticsearchRestTemplate;
        this.objectMapper = objectMapper;
        this.shopMapper = shopMapper;
    }

    /** 调用根 API 验证 Elasticsearch 是否可以正常响应。 */
    public boolean isElasticsearchAvailable() {
        ResponseEntity<String> response = elasticsearchRestTemplate.getForEntity("/", String.class);
        return response.getStatusCode().is2xxSuccessful() && response.getBody() != null;
    }

    /** 通过 HEAD 请求检查 shop_index 是否已经存在。 */
    public boolean indexExists() {
        try {
            ResponseEntity<Void> response = elasticsearchRestTemplate.exchange(
                    "/" + SHOP_INDEX,
                    HttpMethod.HEAD,
                    HttpEntity.EMPTY,
                    Void.class);
            return response.getStatusCode().is2xxSuccessful();
        } catch (HttpClientErrorException exception) {
            if (exception.getStatusCode() == HttpStatus.NOT_FOUND) {
                return false;
            }
            throw exception;
        }
    }

    /**
     * 按预定义 Mapping 创建 shop_index；索引已经存在时按幂等成功处理。
     *
     * @return true 表示本次新建索引，false 表示索引原本已存在
     */
    public boolean createShopIndex() {
        if (indexExists()) {
            log.info("Elasticsearch商户索引已存在, index={}", SHOP_INDEX);
            return false;
        }

        String mapping = loadIndexMapping();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> response = elasticsearchRestTemplate.exchange(
                "/" + SHOP_INDEX,
                HttpMethod.PUT,
                new HttpEntity<>(mapping, headers),
                String.class);
        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new IllegalStateException("创建Elasticsearch商户索引失败: " + response.getStatusCode());
        }
        log.info("Elasticsearch商户索引创建成功, index={}", SHOP_INDEX);
        return true;
    }

    /**
     * 查询 MySQL 全部店铺并通过 Bulk API 首次写入 shop_index。
     *
     * @return 成功提交给 Elasticsearch 的店铺数量
     */
    public int syncAllShops() {
        createShopIndex();
        List<Shop> shops = shopMapper.selectList(null);
        if (shops == null || shops.isEmpty()) {
            log.info("MySQL没有可同步的店铺数据, index={}", SHOP_INDEX);
            return 0;
        }

        int syncedCount = 0;
        for (int fromIndex = 0; fromIndex < shops.size(); fromIndex += BULK_BATCH_SIZE) {
            int toIndex = Math.min(fromIndex + BULK_BATCH_SIZE, shops.size());
            List<Shop> batch = shops.subList(fromIndex, toIndex);
            executeBulk(batch);
            syncedCount += batch.size();
        }

        // 主动刷新只用于首次同步后立即验证；实时 Canal 写入阶段不应逐条刷新。
        elasticsearchRestTemplate.postForEntity(
                "/" + SHOP_INDEX + "/_refresh",
                HttpEntity.EMPTY,
                String.class);
        log.info("MySQL店铺首次同步完成, index={}, syncedCount={}", SHOP_INDEX, syncedCount);
        return syncedCount;
    }

    /** 将数据库 Shop 转换为独立的 Elasticsearch 文档。 */
    ShopDocument toDocument(Shop shop) {
        ShopDocument.Location location = null;
        if (shop.getX() != null && shop.getY() != null) {
            // 数据库 x 为经度、y 为纬度；geo_point 必须写成 lat=y、lon=x。
            location = new ShopDocument.Location(shop.getY(), shop.getX());
        }
        return new ShopDocument(
                shop.getId(),
                shop.getName(),
                shop.getTypeId(),
                shop.getArea(),
                shop.getAddress(),
                null,
                location);
    }

    /** 生成 NDJSON 并执行一批 Bulk index 操作。 */
    private void executeBulk(List<Shop> shops) {
        StringBuilder payload = new StringBuilder();
        for (Shop shop : shops) {
            ShopDocument document = toDocument(shop);
            try {
                payload.append("{\"index\":{\"_index\":\"")
                        .append(SHOP_INDEX)
                        .append("\",\"_id\":\"")
                        .append(document.getId())
                        .append("\"}}\n");
                payload.append(objectMapper.writeValueAsString(document)).append('\n');
            } catch (JsonProcessingException exception) {
                throw new IllegalStateException("序列化商户ES文档失败, shopId=" + shop.getId(), exception);
            }
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(NDJSON_MEDIA_TYPE);
        ResponseEntity<String> response = elasticsearchRestTemplate.exchange(
                "/_bulk",
                HttpMethod.POST,
                new HttpEntity<>(payload.toString(), headers),
                String.class);
        validateBulkResponse(response);
    }

    /** 检查 HTTP 状态和 Bulk 明细错误，避免部分写入失败被误判为同步成功。 */
    private void validateBulkResponse(ResponseEntity<String> response) {
        if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null) {
            throw new IllegalStateException("Elasticsearch Bulk请求失败: " + response.getStatusCode());
        }
        try {
            JsonNode responseBody = objectMapper.readTree(response.getBody());
            if (responseBody.path("errors").asBoolean(false)) {
                throw new IllegalStateException("Elasticsearch Bulk存在失败明细: " + response.getBody());
            }
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("解析Elasticsearch Bulk响应失败", exception);
        }
    }

    /** 从 classpath 加载显式 Mapping，禁止依赖动态字段推断。 */
    private String loadIndexMapping() {
        ClassPathResource resource =
                new ClassPathResource("elasticsearch/shop-index-mapping.json");
        try {
            return StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new IllegalStateException("读取shop_index Mapping失败", exception);
        }
    }
}
