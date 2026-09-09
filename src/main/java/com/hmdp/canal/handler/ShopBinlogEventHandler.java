package com.hmdp.canal.handler;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hmdp.canal.service.ShopCacheInvalidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 解析 hmdp.tb_shop 的行级 binlog，并提取店铺主键执行缓存失效。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShopBinlogEventHandler {

    private static final String SHOP_DATABASE = "hmdp";
    private static final String SHOP_TABLE = "tb_shop";
    private static final String ID_COLUMN = "id";

    private final ShopCacheInvalidator shopCacheInvalidator;

    /**
     * 处理一个 Canal batch；任意目标缓存删除失败时抛出异常，由 Client rollback。
     */
    public void handle(List<CanalEntry.Entry> entries) {
        Set<Long> shopIds = new LinkedHashSet<>();
        for (CanalEntry.Entry entry : entries) {
            collectShopIds(entry, shopIds);
        }
        for (Long shopId : shopIds) {
            shopCacheInvalidator.invalidate(shopId);
        }
        if (!shopIds.isEmpty()) {
            log.info("Canal shop binlog batch handled, shopIds={}", shopIds);
        }
    }

    /**
     * 仅处理目标库表的 INSERT、UPDATE、DELETE ROWDATA。
     */
    private void collectShopIds(CanalEntry.Entry entry, Set<Long> shopIds) {
        if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA
                || !SHOP_DATABASE.equalsIgnoreCase(entry.getHeader().getSchemaName())
                || !SHOP_TABLE.equalsIgnoreCase(entry.getHeader().getTableName())) {
            return;
        }

        CanalEntry.RowChange rowChange;
        try {
            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Unable to parse Canal row change", e);
        }

        CanalEntry.EventType eventType = rowChange.getEventType();
        if (eventType != CanalEntry.EventType.INSERT
                && eventType != CanalEntry.EventType.UPDATE
                && eventType != CanalEntry.EventType.DELETE) {
            return;
        }

        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            List<CanalEntry.Column> columns = eventType == CanalEntry.EventType.DELETE
                    ? rowData.getBeforeColumnsList()
                    : rowData.getAfterColumnsList();
            shopIds.add(readShopId(columns, eventType));
        }
    }

    /**
     * DELETE 从 before image 取 id，INSERT/UPDATE 从 after image 取 id。
     */
    private Long readShopId(List<CanalEntry.Column> columns, CanalEntry.EventType eventType) {
        for (CanalEntry.Column column : columns) {
            if (ID_COLUMN.equalsIgnoreCase(column.getName())) {
                try {
                    return Long.valueOf(column.getValue());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "Invalid shop id in Canal " + eventType + " event: " + column.getValue(), e);
                }
            }
        }
        throw new IllegalArgumentException("Missing shop id in Canal " + eventType + " event");
    }
}
