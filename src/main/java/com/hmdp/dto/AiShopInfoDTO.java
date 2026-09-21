package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * AI 运营助手可读取的店铺基础信息。
 *
 * <p>该 DTO 只暴露首期 Tool 所需字段，避免将完整 Shop 实体交给 AI 服务。</p>
 */
@Data
@AllArgsConstructor
public class AiShopInfoDTO {

    /** 店铺主键。 */
    private Long id;

    /** 店铺名称。 */
    private String name;

    /** 店铺类型名称。 */
    private String type;

    /** 店铺地址。 */
    private String address;
}
