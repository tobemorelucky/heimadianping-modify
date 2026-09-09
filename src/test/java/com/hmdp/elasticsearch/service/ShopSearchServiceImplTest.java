package com.hmdp.elasticsearch.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.ShopDTO;
import com.hmdp.elasticsearch.service.impl.ShopSearchServiceImpl;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/** 验证 ES 名称搜索、无结果、异常降级和相关性顺序恢复。 */
@ExtendWith(MockitoExtension.class)
class ShopSearchServiceImplTest {

    @Mock
    private ShopMapper shopMapper;

    private MockRestServiceServer server;
    private ShopSearchService searchService;

    /** 每个用例使用隔离的 Mock Elasticsearch HTTP 服务。 */
    @BeforeEach
    void setUp() {
        RestTemplate restTemplate = new RestTemplateBuilder()
                .rootUri("http://127.0.0.1:9200")
                .build();
        server = MockRestServiceServer.bindTo(restTemplate).build();
        searchService = new ShopSearchServiceImpl(restTemplate, new ObjectMapper(), shopMapper);
    }

    /** 非空关键词必须发送 match 查询并返回完整 ShopDTO。 */
    @Test
    void shouldSearchByMatchQuery() {
        server.expect(once(), requestTo("http://127.0.0.1:9200/shop_index/_search"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().string(allOf(
                        containsString("\"match\""),
                        containsString("\"name\""),
                        containsString("\"query\":\"火锅\""))))
                .andRespond(withSuccess(searchResponse(1L), MediaType.APPLICATION_JSON));
        when(shopMapper.selectBatchIds(anyCollection()))
                .thenReturn(Collections.singletonList(shop(1L, "海底捞火锅")));

        List<ShopDTO> result = searchService.searchByName("火锅", 1);

        assertEquals(1, result.size());
        assertEquals("海底捞火锅", result.get(0).getName());
        assertEquals("shop-1.jpg", result.get(0).getImages());
        server.verify();
    }

    /** ES 无命中时返回空列表，并且不访问 MySQL 批量查询。 */
    @Test
    void shouldReturnEmptyListWhenElasticsearchHasNoHits() {
        server.expect(once(), requestTo("http://127.0.0.1:9200/shop_index/_search"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess(searchResponse(), MediaType.APPLICATION_JSON));

        List<ShopDTO> result = searchService.searchByName("不存在的商户", 1);

        assertTrue(result.isEmpty());
        verify(shopMapper, never()).selectBatchIds(anyCollection());
        server.verify();
    }

    /** 空关键词保持原逻辑，直接分页查询 MySQL 而不访问 Elasticsearch。 */
    @SuppressWarnings("unchecked")
    @Test
    void shouldKeepMysqlPagingForBlankKeyword() {
        Page<Shop> mysqlPage = new Page<>(1, 10);
        mysqlPage.setRecords(Collections.singletonList(shop(4L, "默认商户")));
        when(shopMapper.selectPage(any(Page.class), any(QueryWrapper.class)))
                .thenReturn(mysqlPage);

        List<ShopDTO> result = searchService.searchByName("  ", 1);

        assertEquals(1, result.size());
        assertEquals(4L, result.get(0).getId());
        verify(shopMapper, never()).selectBatchIds(anyCollection());
        server.verify();
    }

    /** ES 异常时必须降级为原 MySQL LIKE 分页查询。 */
    @SuppressWarnings("unchecked")
    @Test
    void shouldFallbackToMysqlWhenElasticsearchFails() {
        server.expect(once(), requestTo("http://127.0.0.1:9200/shop_index/_search"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withServerError());
        Page<Shop> fallbackPage = new Page<>(1, 10);
        fallbackPage.setRecords(Collections.singletonList(shop(3L, "备用火锅店")));
        when(shopMapper.selectPage(any(Page.class), any(QueryWrapper.class)))
                .thenReturn(fallbackPage);

        List<ShopDTO> result = searchService.searchByName("火锅", 1);

        assertEquals(Collections.singletonList(3L),
                Collections.singletonList(result.get(0).getId()));
        server.verify();
    }

    /** MySQL 批量查询顺序不同于 ES hits 时，最终结果仍保持 ES 排序。 */
    @Test
    void shouldRestoreElasticsearchHitOrder() {
        server.expect(once(), requestTo("http://127.0.0.1:9200/shop_index/_search"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().string(allOf(
                        containsString("\"_score\":{\"order\":\"desc\"}"),
                        containsString("\"id\":{\"order\":\"asc\"}"))))
                .andRespond(withSuccess(searchResponse(2L, 1L), MediaType.APPLICATION_JSON));
        when(shopMapper.selectBatchIds(anyCollection()))
                .thenReturn(Arrays.asList(shop(1L, "第一家"), shop(2L, "第二家")));

        List<ShopDTO> result = searchService.searchByName("店", 1);

        assertEquals(Arrays.asList(2L, 1L), Arrays.asList(result.get(0).getId(), result.get(1).getId()));
        server.verify();
    }

    /** 构造只包含测试所需 hits 的 Elasticsearch 响应。 */
    private String searchResponse(Long... ids) {
        StringBuilder response = new StringBuilder("{\"hits\":{\"hits\":[");
        for (int index = 0; index < ids.length; index++) {
            if (index > 0) {
                response.append(',');
            }
            response.append("{\"_id\":\"").append(ids[index])
                    .append("\",\"_source\":{\"id\":").append(ids[index]).append("}}");
        }
        return response.append("]}}").toString();
    }

    /** 构造包含完整响应字段的 MySQL 商户。 */
    private Shop shop(Long id, String name) {
        return new Shop()
                .setId(id)
                .setName(name)
                .setTypeId(1L)
                .setImages("shop-" + id + ".jpg")
                .setArea("测试商圈")
                .setAddress("测试地址")
                .setX(120.1)
                .setY(30.2)
                .setAvgPrice(100L)
                .setSold(20)
                .setComments(10)
                .setScore(45)
                .setOpenHours("10:00-22:00");
    }
}
