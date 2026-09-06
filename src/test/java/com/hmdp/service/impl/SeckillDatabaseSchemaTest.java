package com.hmdp.service.impl;

import org.junit.jupiter.api.Test;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证秒杀业务唯一索引位于正确的订单表定义中。
 */
class SeckillDatabaseSchemaTest {

    /** 唯一索引必须属于 tb_voucher_order，不能误加到其他表。 */
    @Test
    void shouldDeclareUserVoucherUniqueIndexOnVoucherOrderTable() throws IOException {
        String schema = readClasspathResource("db/hmdp.sql");
        String followTable = tableDefinition(schema, "tb_follow");
        String voucherOrderTable = tableDefinition(schema, "tb_voucher_order");

        assertFalse(followTable.contains("uk_voucher_order_user_voucher"));
        assertTrue(voucherOrderTable.contains(
                "UNIQUE INDEX `uk_voucher_order_user_voucher`(`user_id`, `voucher_id`)"));
    }

    /** 读取测试所需的 classpath 文本资源。 */
    private String readClasspathResource(String path) throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(path)) {
            assertNotNull(inputStream, "缺少测试资源：" + path);
            String content = StreamUtils.copyToString(inputStream, StandardCharsets.UTF_8);
            assertFalse(content.isEmpty(), "测试资源为空：" + path);
            return content;
        }
    }

    /** 截取指定 CREATE TABLE 到下一张表之间的定义文本。 */
    private String tableDefinition(String schema, String tableName) {
        String marker = "CREATE TABLE `" + tableName + "`";
        int start = schema.indexOf(marker);
        assertTrue(start >= 0, "缺少表定义：" + tableName);
        int end = schema.indexOf(";", start);
        assertTrue(end > start, "表定义未结束：" + tableName);
        return schema.substring(start, end + 1);
    }
}
