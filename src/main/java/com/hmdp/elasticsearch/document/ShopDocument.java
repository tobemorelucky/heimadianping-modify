package com.hmdp.elasticsearch.document;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 商户 Elasticsearch 文档模型，对应 {@code shop_index}。
 *
 * <p>该模型与 MyBatis 的 Shop 实体隔离，避免搜索字段和数据库字段相互污染。</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ShopDocument {

    /** MySQL 店铺主键，同时作为 Elasticsearch 文档 _id。 */
    private Long id;

    /** 店铺名称，作为全文检索的主要字段。 */
    private String name;

    /** 店铺类型 ID，用于精确过滤。 */
    private Long typeId;

    /** 店铺所在商圈，可参与全文检索和精确过滤。 */
    private String area;

    /** 店铺地址，可参与全文检索。 */
    private String address;

    /** 店铺描述；当前数据库没有权威来源时保持为空。 */
    private String description;

    /** 经纬度位置，供 Elasticsearch geo_point 映射使用。 */
    private Location location;

    /** Elasticsearch geo_point 对应的经纬度对象。 */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Location {

        /** 纬度，对应 Shop.y。 */
        private Double lat;

        /** 经度，对应 Shop.x。 */
        private Double lon;
    }
}
