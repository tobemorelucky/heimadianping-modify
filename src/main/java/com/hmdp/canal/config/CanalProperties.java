package com.hmdp.canal.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Canal Client 连接、订阅和轮询参数。
 */
@Data
@ConfigurationProperties(prefix = "canal")
public class CanalProperties {

    /** 是否启动 Canal 消费线程。 */
    private boolean enabled = true;

    /** Canal Server 地址。 */
    private String host;

    /** Canal Client TCP 端口。 */
    private int port;

    /** Canal destination 名称。 */
    private String destination;

    /** Canal 表订阅正则。 */
    private String subscription;

    /** Canal Server 客户端认证用户名，可为空。 */
    private String username = "";

    /** Canal Server 客户端认证密码，可为空。 */
    private String password = "";

    /** 单次最多拉取的 Entry 数量。 */
    private int batchSize = 100;

    /** 空闲轮询等待时间，避免无数据时忙轮询。 */
    private long pollTimeoutMillis = 1000L;

    /** 连接或消费失败后的重连退避时间。 */
    private long retryIntervalMillis = 3000L;
}
