package com.hmdp.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.hmdp.canal.config.CanalProperties;
import com.hmdp.canal.handler.ShopBinlogEventHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Spring 生命周期托管的 Canal 消费客户端。
 *
 * <p>使用 getWithoutAck 拉取。只有事件解析和全部 Redis 缓存删除成功后才 ACK；
 * 失败时 rollback 当前 batch，并通过断线重连重新消费。</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CanalClient implements SmartLifecycle {

    private final CanalConnector canalConnector;
    private final CanalProperties properties;
    private final ShopBinlogEventHandler shopBinlogEventHandler;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "hmdp-canal-consumer");
        thread.setDaemon(true);
        return thread;
    });

    private volatile boolean running;
    private volatile boolean connected;

    /**
     * 启动独立消费线程，避免阻塞 Spring Boot 启动线程。
     */
    @Override
    public synchronized void start() {
        if (running || !properties.isEnabled()) {
            return;
        }
        running = true;
        executor.submit(this::consumeLoop);
        log.info("Canal consumer scheduled, destination={}, subscription={}",
                properties.getDestination(), properties.getSubscription());
    }

    /**
     * 建立连接并持续消费；连接异常采用固定退避，避免快速重试。
     */
    private void consumeLoop() {
        while (running) {
            try {
                canalConnector.connect();
                canalConnector.subscribe(properties.getSubscription());
                // 清理上次连接遗留的未确认 batch，确保其可被重新投递。
                canalConnector.rollback();
                connected = true;
                log.info("Canal consumer connected, host={}:{}, destination={}",
                        properties.getHost(), properties.getPort(), properties.getDestination());

                while (running) {
                    consumeOnce();
                }
            } catch (Exception e) {
                if (running) {
                    log.error("Canal consume failed; batch will not be acknowledged", e);
                    sleepBeforeReconnect();
                }
            } finally {
                connected = false;
                disconnectQuietly();
            }
        }
    }

    /**
     * 拉取并处理一次 batch，供消费循环和单元测试共同验证 ACK 边界。
     */
    void consumeOnce() {
        Message message = canalConnector.getWithoutAck(
                properties.getBatchSize(),
                properties.getPollTimeoutMillis(),
                TimeUnit.MILLISECONDS);
        long batchId = message.getId();
        if (batchId == -1 || message.getEntries().isEmpty()) {
            return;
        }

        try {
            shopBinlogEventHandler.handle(message.getEntries());
            canalConnector.ack(batchId);
            log.debug("Canal batch acknowledged, batchId={}, entryCount={}",
                    batchId, message.getEntries().size());
        } catch (RuntimeException e) {
            rollbackQuietly(batchId);
            throw e;
        }
    }

    /**
     * 消费失败时回滚指定 batch；回滚本身失败也保留原始异常日志。
     */
    private void rollbackQuietly(long batchId) {
        try {
            canalConnector.rollback(batchId);
            log.warn("Canal batch rolled back, batchId={}", batchId);
        } catch (Exception rollbackException) {
            log.error("Canal batch rollback failed, batchId={}", batchId, rollbackException);
        }
    }

    /**
     * 固定退避可防止 Canal Server 或 MySQL 暂不可用时形成重连风暴。
     */
    private void sleepBeforeReconnect() {
        try {
            Thread.sleep(properties.getRetryIntervalMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 断开连接时吞掉关闭异常，避免覆盖真正的消费错误。
     */
    private void disconnectQuietly() {
        try {
            canalConnector.disconnect();
        } catch (Exception e) {
            log.warn("Canal disconnect failed", e);
        }
    }

    /**
     * 停止拉取并中断退避等待，连接由消费线程 finally 关闭。
     */
    @Override
    public synchronized void stop() {
        running = false;
        executor.shutdownNow();
        log.info("Canal consumer stopping");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * 返回当前是否已完成连接和订阅，供运行检查与独立集成测试等待就绪。
     */
    public boolean isConnected() {
        return connected;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
