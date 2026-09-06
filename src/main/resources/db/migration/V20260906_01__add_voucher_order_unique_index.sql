-- 上线前先检查并清理重复的 user_id + voucher_id 数据，否则唯一索引创建会失败。
ALTER TABLE `tb_voucher_order`
    ADD UNIQUE INDEX `uk_voucher_order_user_voucher` (`user_id`, `voucher_id`) USING BTREE;
