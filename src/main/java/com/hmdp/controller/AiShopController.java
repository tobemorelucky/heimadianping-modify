package com.hmdp.controller;

import com.hmdp.dto.AiShopInfoDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.service.IShopService;
import com.hmdp.service.IShopTypeService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * AI 运营助手专用的只读店铺查询接口。
 *
 * <p>该 Controller 与现有 ShopController 隔离，只复用业务 Service，
 * 不允许 AI 服务直接访问数据库。</p>
 */
@RestController
@RequestMapping("/api/ai/shop")
public class AiShopController {

    private final IShopService shopService;
    private final IShopTypeService shopTypeService;

    /** 注入现有只读业务服务。 */
    public AiShopController(IShopService shopService, IShopTypeService shopTypeService) {
        this.shopService = shopService;
        this.shopTypeService = shopTypeService;
    }

    /**
     * 查询 AI Tool 所需的最小店铺信息。
     *
     * @param id 店铺 ID
     * @return 仅包含 id、name、type、address 的统一 Result
     */
    @GetMapping("/{id}")
    public Result getShopInfo(@PathVariable("id") Long id) {
        Shop shop = shopService.getById(id);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }

        // 店铺类型单独查询，响应中不暴露内部 typeId。
        ShopType shopType = shop.getTypeId() == null
                ? null
                : shopTypeService.getById(shop.getTypeId());
        String typeName = shopType == null ? null : shopType.getName();

        return Result.ok(new AiShopInfoDTO(
                shop.getId(),
                shop.getName(),
                typeName,
                shop.getAddress()
        ));
    }
}
