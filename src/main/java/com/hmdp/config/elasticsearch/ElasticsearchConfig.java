package com.hmdp.config.elasticsearch;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

/** Elasticsearch REST 连接配置。 */
@Configuration
public class ElasticsearchConfig {

    /**
     * 创建 Elasticsearch 专用 REST 客户端。
     *
     * <p>默认连接本机 9200，并允许后续通过系统属性覆盖地址而无需修改业务代码。</p>
     */
    @Bean("elasticsearchRestTemplate")
    public RestTemplate elasticsearchRestTemplate(
            RestTemplateBuilder builder,
            @Value("${hmdp.elasticsearch.url:http://127.0.0.1:9200}") String elasticsearchUrl) {
        return builder
                .rootUri(elasticsearchUrl)
                .setConnectTimeout(Duration.ofSeconds(3))
                .setReadTimeout(Duration.ofSeconds(30))
                .build();
    }
}
