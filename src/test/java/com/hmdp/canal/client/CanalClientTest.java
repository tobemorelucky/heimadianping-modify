package com.hmdp.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.hmdp.canal.config.CanalProperties;
import com.hmdp.canal.handler.ShopBinlogEventHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Canal batch ACK/rollback 边界单元测试。
 */
class CanalClientTest {

    private CanalConnector connector;
    private ShopBinlogEventHandler handler;
    private CanalClient client;

    @BeforeEach
    void setUp() {
        connector = mock(CanalConnector.class);
        handler = mock(ShopBinlogEventHandler.class);
        CanalProperties properties = new CanalProperties();
        properties.setBatchSize(100);
        properties.setPollTimeoutMillis(1000L);
        client = new CanalClient(connector, properties, handler);
    }

    /**
     * 处理全部成功后才确认 batch。
     */
    @Test
    void shouldAckAfterSuccessfulHandling() {
        Message message = message(11L);
        when(connector.getWithoutAck(anyInt(), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(message);

        client.consumeOnce();

        verify(handler).handle(message.getEntries());
        verify(connector).ack(11L);
        verify(connector, never()).rollback(11L);
    }

    /**
     * 事件解析或 Redis 删除失败时回滚，禁止错误 ACK。
     */
    @Test
    void shouldRollbackWithoutAckWhenHandlingFails() {
        Message message = message(12L);
        when(connector.getWithoutAck(anyInt(), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(message);
        doThrow(new IllegalStateException("redis unavailable"))
                .when(handler).handle(message.getEntries());

        assertThrows(IllegalStateException.class, client::consumeOnce);

        verify(connector).rollback(12L);
        verify(connector, never()).ack(12L);
    }

    /**
     * 空轮询不提交无效 batchId。
     */
    @Test
    void shouldNotAckEmptyMessage() {
        when(connector.getWithoutAck(anyInt(), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(new Message(-1L, Collections.emptyList()));

        client.consumeOnce();

        verify(connector, never()).ack(anyLong());
    }

    private Message message(long batchId) {
        return new Message(batchId, Collections.singletonList(
                CanalEntry.Entry.getDefaultInstance()));
    }
}
