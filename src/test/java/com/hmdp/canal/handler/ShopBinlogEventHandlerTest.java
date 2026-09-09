package com.hmdp.canal.handler;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.hmdp.canal.service.ShopCacheInvalidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * tb_shop binlog 解析单元测试。
 */
class ShopBinlogEventHandlerTest {

    private ShopCacheInvalidator invalidator;
    private ShopBinlogEventHandler handler;

    @BeforeEach
    void setUp() {
        invalidator = mock(ShopCacheInvalidator.class);
        handler = new ShopBinlogEventHandler(invalidator);
    }

    /**
     * INSERT/UPDATE 读取 after image，DELETE 读取 before image。
     */
    @Test
    void shouldHandleInsertUpdateAndDelete() {
        handler.handle(Arrays.asList(
                rowEntry("hmdp", "tb_shop", CanalEntry.EventType.INSERT, 1L),
                rowEntry("hmdp", "tb_shop", CanalEntry.EventType.UPDATE, 2L),
                rowEntry("hmdp", "tb_shop", CanalEntry.EventType.DELETE, 3L)));

        verify(invalidator).invalidate(1L);
        verify(invalidator).invalidate(2L);
        verify(invalidator).invalidate(3L);
    }

    /**
     * 非目标库表事件不会触发缓存删除。
     */
    @Test
    void shouldIgnoreOtherDatabaseOrTable() {
        handler.handle(Arrays.asList(
                rowEntry("other", "tb_shop", CanalEntry.EventType.UPDATE, 1L),
                rowEntry("hmdp", "tb_user", CanalEntry.EventType.UPDATE, 2L)));

        verify(invalidator, never()).invalidate(org.mockito.ArgumentMatchers.anyLong());
    }

    /**
     * 构造带完整行镜像的 Canal ROWDATA 测试事件。
     */
    private CanalEntry.Entry rowEntry(String database,
                                      String table,
                                      CanalEntry.EventType eventType,
                                      Long shopId) {
        CanalEntry.Column idColumn = CanalEntry.Column.newBuilder()
                .setName("id")
                .setValue(shopId.toString())
                .build();
        List<CanalEntry.Column> before = eventType == CanalEntry.EventType.DELETE
                ? Collections.singletonList(idColumn)
                : Collections.emptyList();
        List<CanalEntry.Column> after = eventType == CanalEntry.EventType.DELETE
                ? Collections.emptyList()
                : Collections.singletonList(idColumn);
        CanalEntry.RowData rowData = CanalEntry.RowData.newBuilder()
                .addAllBeforeColumns(before)
                .addAllAfterColumns(after)
                .build();
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.newBuilder()
                .setEventType(eventType)
                .addRowDatas(rowData)
                .build();
        return CanalEntry.Entry.newBuilder()
                .setHeader(CanalEntry.Header.newBuilder()
                        .setSchemaName(database)
                        .setTableName(table)
                        .build())
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setStoreValue(rowChange.toByteString())
                .build();
    }
}
