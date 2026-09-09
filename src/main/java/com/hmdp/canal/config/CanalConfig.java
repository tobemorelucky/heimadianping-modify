package com.hmdp.canal.config;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;

/**
 * Canal 原生 Client 配置。
 */
@Configuration
@EnableConfigurationProperties(CanalProperties.class)
public class CanalConfig {

    /**
     * 根据外部配置创建单节点 Canal 连接器。
     */
    @Bean
    public CanalConnector canalConnector(CanalProperties properties) {
        return CanalConnectors.newSingleConnector(
                new InetSocketAddress(properties.getHost(), properties.getPort()),
                properties.getDestination(),
                properties.getUsername(),
                properties.getPassword());
    }
}
