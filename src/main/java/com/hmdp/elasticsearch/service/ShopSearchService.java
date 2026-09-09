package com.hmdp.elasticsearch.service;

import com.hmdp.dto.ShopDTO;

import java.util.List;

/** 商户 Elasticsearch 搜索服务。 */
public interface ShopSearchService {

    /**
     * 按商户名称分页搜索。
     *
     * @param keyword 名称关键词，为空时保持原 MySQL 分页逻辑
     * @param current 页码，从 1 开始
     * @return 完整商户响应列表
     */
    List<ShopDTO> searchByName(String keyword, Integer current);
}
