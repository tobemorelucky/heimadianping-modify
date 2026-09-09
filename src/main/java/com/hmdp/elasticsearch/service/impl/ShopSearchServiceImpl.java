package com.hmdp.elasticsearch.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hmdp.dto.ShopDTO;
import com.hmdp.elasticsearch.service.ShopEsSyncService;
import com.hmdp.elasticsearch.service.ShopSearchService;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 商户名称 Elasticsearch 搜索实现。
 *
 * <p>Elasticsearch 负责匹配和排序，MySQL 仅按主键批量补齐完整商户字段。</p>
 */
@Slf4j
@Service
public class ShopSearchServiceImpl implements ShopSearchService {

    private final RestTemplate elasticsearchRestTemplate;
    private final ObjectMapper objectMapper;
    private final ShopMapper shopMapper;

    /** 注入 ES REST 客户端、JSON 工具和 MySQL Mapper。 */
    public ShopSearchServiceImpl(
            @Qualifier("elasticsearchRestTemplate") RestTemplate elasticsearchRestTemplate,
            ObjectMapper objectMapper,
            ShopMapper shopMapper) {
        this.elasticsearchRestTemplate = elasticsearchRestTemplate;
        this.objectMapper = objectMapper;
        this.shopMapper = shopMapper;
    }

    /**
     * 非空关键词使用 ES match 查询；空关键词和 ES 异常保持原 MySQL 查询能力。
     */
    @Override
    public List<ShopDTO> searchByName(String keyword, Integer current) {
        int pageNumber = current == null || current < 1 ? 1 : current;
        if (StrUtil.isBlank(keyword)) {
            // 原接口在关键词为空时不添加 LIKE 条件，继续由 MySQL 分页返回全部商户。
            return queryMysql(keyword, pageNumber);
        }

        try {
            List<Long> orderedShopIds = searchOrderedShopIds(keyword, pageNumber);
            return loadShopsInSearchOrder(orderedShopIds);
        } catch (RestClientException | JsonProcessingException | IllegalStateException exception) {
            // ES 是搜索读模型；故障时记录原因并回退原 LIKE 查询，避免影响其他店铺业务。
            log.error("Elasticsearch商户名称搜索失败，降级到MySQL, keyword={}, current={}",
                    keyword, pageNumber, exception);
            return queryMysql(keyword, pageNumber);
        }
    }

    /** 构造 match 查询并提取 Elasticsearch 返回的有序商户 ID。 */
    private List<Long> searchOrderedShopIds(String keyword, int pageNumber)
            throws JsonProcessingException {
        ObjectNode requestBody = objectMapper.createObjectNode();
        requestBody.put("from", (long) (pageNumber - 1) * SystemConstants.MAX_PAGE_SIZE);
        requestBody.put("size", SystemConstants.MAX_PAGE_SIZE);
        requestBody.putArray("_source").add("id");

        ObjectNode matchOptions = objectMapper.createObjectNode();
        matchOptions.put("query", keyword);
        requestBody.putObject("query").putObject("match").set("name", matchOptions);

        // 默认按相关性排序，ID 用作同分时的稳定排序字段。
        ArrayNode sort = requestBody.putArray("sort");
        sort.addObject().putObject("_score").put("order", "desc");
        sort.addObject().putObject("id").put("order", "asc");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> response = elasticsearchRestTemplate.exchange(
                "/" + ShopEsSyncService.SHOP_INDEX + "/_search",
                HttpMethod.POST,
                new HttpEntity<>(objectMapper.writeValueAsString(requestBody), headers),
                String.class);
        if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null) {
            throw new IllegalStateException("Elasticsearch商户搜索响应无效: " + response.getStatusCode());
        }

        return parseOrderedShopIds(response.getBody());
    }

    /** 解析 hits，使用 LinkedHashSet 在保持 ES 顺序的同时防止重复 ID。 */
    private List<Long> parseOrderedShopIds(String responseBody) throws JsonProcessingException {
        JsonNode hits = objectMapper.readTree(responseBody).path("hits").path("hits");
        if (!hits.isArray()) {
            throw new IllegalStateException("Elasticsearch商户搜索响应缺少hits数组");
        }

        LinkedHashSet<Long> orderedIds = new LinkedHashSet<>();
        for (JsonNode hit : hits) {
            JsonNode sourceId = hit.path("_source").path("id");
            if (sourceId.canConvertToLong()) {
                orderedIds.add(sourceId.asLong());
                continue;
            }
            String documentId = hit.path("_id").asText(null);
            if (StrUtil.isBlank(documentId)) {
                throw new IllegalStateException("Elasticsearch商户搜索命中缺少店铺ID");
            }
            try {
                orderedIds.add(Long.valueOf(documentId));
            } catch (NumberFormatException exception) {
                throw new IllegalStateException("Elasticsearch商户文档ID不是Long: " + documentId,
                        exception);
            }
        }
        return new ArrayList<>(orderedIds);
    }

    /** 批量查询完整 Shop，并严格按照 Elasticsearch 命中顺序组装 DTO。 */
    private List<ShopDTO> loadShopsInSearchOrder(List<Long> orderedShopIds) {
        if (orderedShopIds.isEmpty()) {
            return Collections.emptyList();
        }

        List<Shop> shops = shopMapper.selectBatchIds(orderedShopIds);
        if (shops == null || shops.isEmpty()) {
            log.warn("Elasticsearch命中店铺但MySQL无对应记录, shopIds={}", orderedShopIds);
            return Collections.emptyList();
        }
        Map<Long, Shop> shopById = shops.stream()
                .collect(Collectors.toMap(Shop::getId, Function.identity(), (first, ignored) -> first));

        List<ShopDTO> result = new ArrayList<>(orderedShopIds.size());
        for (Long shopId : orderedShopIds) {
            Shop shop = shopById.get(shopId);
            if (shop == null) {
                // 同步删除存在短暂延迟时跳过脏命中，不返回字段为空的商户。
                log.warn("Elasticsearch商户数据与MySQL不一致, missingShopId={}", shopId);
                continue;
            }
            result.add(toShopDTO(shop));
        }
        return result;
    }

    /** 保留原 MySQL LIKE 分页逻辑，供空关键词和 Elasticsearch 故障降级使用。 */
    private List<ShopDTO> queryMysql(String keyword, int pageNumber) {
        QueryWrapper<Shop> queryWrapper = new QueryWrapper<>();
        queryWrapper.like(StrUtil.isNotBlank(keyword), "name", keyword);
        Page<Shop> page = shopMapper.selectPage(
                new Page<>(pageNumber, SystemConstants.MAX_PAGE_SIZE), queryWrapper);
        Collection<Shop> records = page == null ? Collections.emptyList() : page.getRecords();
        return records.stream().map(this::toShopDTO).collect(Collectors.toList());
    }

    /** 将数据库实体转换为协议字段一致的响应 DTO。 */
    private ShopDTO toShopDTO(Shop shop) {
        return BeanUtil.copyProperties(shop, ShopDTO.class);
    }
}
