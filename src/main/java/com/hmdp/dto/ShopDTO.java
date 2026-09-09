package com.hmdp.dto;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 商户查询响应对象。
 *
 * <p>字段与当前名称搜索返回的 {@code Shop} JSON 保持一致，避免搜索数据源切换改变接口协议。</p>
 */
@Data
public class ShopDTO {

    /** 商户主键。 */
    private Long id;

    /** 商户名称。 */
    private String name;

    /** 商户类型 ID。 */
    private Long typeId;

    /** 商户图片，多个地址以逗号分隔。 */
    private String images;

    /** 所属商圈。 */
    private String area;

    /** 商户地址。 */
    private String address;

    /** 经度，与原 Shop.x 字段一致。 */
    private Double x;

    /** 纬度，与原 Shop.y 字段一致。 */
    private Double y;

    /** 人均价格。 */
    private Long avgPrice;

    /** 销量。 */
    private Integer sold;

    /** 评论数量。 */
    private Integer comments;

    /** 评分，沿用数据库放大十倍后的整数值。 */
    private Integer score;

    /** 营业时间。 */
    private String openHours;

    /** 创建时间。 */
    private LocalDateTime createTime;

    /** 更新时间。 */
    private LocalDateTime updateTime;

    /** 距离字段；名称搜索中通常为空，保留以兼容原 Shop 响应。 */
    private Double distance;
}
