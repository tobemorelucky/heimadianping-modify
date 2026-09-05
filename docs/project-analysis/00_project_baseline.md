# 项目基线

## 1. 审计范围

- 审计日期：2026-09-05（Asia/Shanghai）
- 仓库目录：`E:\work\code\java\hm-dianping-total`
- Git 分支：`main`
- 基线提交：`db31cdf1b02b3e710a4c085251cf49df489e942f`
- Maven 坐标：`com.hmdp:hm-dianping:0.0.1-SNAPSHOT`
- 应用名：`hmdp`
- 服务端口：`8081`
- 审计对象说明：仓库中没有名为 `heimadianping-modify` 的子模块；本报告将用户所称项目解析为当前唯一 Maven 项目 `hm-dianping`。
- 变更边界：未修改 Java、配置、SQL、Lua、Nginx 或测试代码，只新增 `docs/project-analysis/` 下的分析文档。

## 2. 技术版本

| 项目 | 当前基线 | 证据与说明 |
| --- | --- | --- |
| Spring Boot | `2.3.12.RELEASE` | `pom.xml` 的 parent |
| Java | `1.8` | `pom.xml` 的 `java.version` |
| Maven | 未锁定 | 无 Maven Wrapper，仓库未声明 Maven 运行时版本 |
| MySQL JDBC Driver | `mysql-connector-java 5.1.47` | `pom.xml`，使用旧驱动类 `com.mysql.jdbc.Driver` |
| MySQL 数据基线 | `5.6.22` | `src/main/resources/db/hmdp.sql` 的导出头；脚本包含 MySQL 5.6 风格零日期默认值 |
| Redis 客户端抽象 | Spring Data Redis `2.6.2` | 替换了 Spring Boot 2.3 默认版本 |
| Lettuce | `6.1.6.RELEASE` | `pom.xml` 显式覆盖 |
| Redisson | `3.13.6` | `pom.xml` |
| Redis 服务端 | 未在仓库锁定 | 秒杀 Stream 至少依赖 Redis 5；附近店铺使用 `GEOSEARCH`，完整功能应以 Redis 6.2+ 验证 |
| MyBatis-Plus | `3.4.3` | `pom.xml` |
| Hutool | `5.7.17` | `pom.xml` |
| Nginx | `1.18.0` 目录随仓库提供 | `nginx-1.18.0/`；不属于 Maven 模块 |

> “Redis 6.2+”是根据当前代码调用 `GEOSEARCH` 得出的兼容性要求，不是仓库显式声明。若仅运行登录、缓存和 Redis Stream 秒杀，最低命令集要求不同；部署基线应统一固定为经过集成测试的版本。

## 3. Maven 直接依赖

| 依赖 | 版本来源 | 用途 |
| --- | --- | --- |
| `spring-boot-starter-web` | Boot parent 管理 | Spring MVC、内嵌 Web 容器、JSON API |
| `spring-boot-starter-data-redis` | Boot parent 管理；排除默认 Redis/Lettuce | Redis 自动配置 |
| `spring-data-redis` | `2.6.2` | StringRedisTemplate、Stream、GEO、Bitmap、ZSet |
| `lettuce-core` | `6.1.6.RELEASE` | Redis 驱动 |
| `commons-pool2` | Boot parent 管理 | Lettuce 连接池 |
| `mysql-connector-java` | `5.1.47` | MySQL JDBC |
| `mybatis-plus-boot-starter` | `3.4.3` | ORM、CRUD、分页 |
| `hutool-all` | `5.7.17` | Bean/JSON/UUID/字符串/文件工具 |
| `redisson` | `3.13.6` | 分布式可重入锁 |
| `lombok` | Boot parent 管理，可选 | 实体和日志样板代码生成 |
| `spring-boot-starter-test` | Boot parent 管理，test scope | JUnit 5 与 Spring 测试 |

当前没有 Kafka、Elasticsearch、Canal 客户端、Spring Security、Flyway/Liquibase、指标采集或 AI SDK 依赖。

## 4. 使用框架与基础设施

- Web：Spring MVC，统一响应 `Result`，全局捕获 `RuntimeException`。
- 持久化：MyBatis-Plus；`VoucherMapper.xml` 有一条优惠券与秒杀券联表查询。
- 数据库：MySQL/InnoDB，共 10 张业务表。
- 缓存与高并发组件：Redis、Lettuce、Redis Lua、Redis Stream、Redisson。
- 身份态：自定义 MVC 拦截器 + Redis Token Hash + ThreadLocal，不使用 Spring Security。
- 文件资源：本地磁盘上传，路径硬编码在 `SystemConstants.IMAGE_UPLOAD_DIR`。
- 反向代理/静态站点：仓库携带 Nginx 1.18.0 目录。
- 测试：3 个测试类，主要是 Redis ID、GEO 数据装载、HyperLogLog、Redisson 与 Bitmap 演示；缺少业务断言型单元/集成测试。

## 5. 当前运行配置

`src/main/resources/application.yaml` 将 MySQL 和 Redis 都指向 `127.0.0.1`：

- MySQL：`jdbc:mysql://127.0.0.1:3306/hmdp`，用户名 `root`，密码以明文写入配置。
- Redis：`127.0.0.1:6379`，未启用密码；Lettuce 连接池最大活跃/空闲连接数均为 10。
- Redisson：没有复用 Spring Redis 配置，而是在 `RedissonConfig` 再次硬编码 `redis://127.0.0.1:6379`。
- 日志：`com.hmdp` 为 DEBUG。

## 6. 环境前置数据

以下数据不会由应用启动自动建立：

1. 导入 `src/main/resources/db/hmdp.sql` 到配置的 `hmdp` 数据库。
2. 为秒杀库存准备 `seckill:stock:{voucherId}`；新增秒杀券接口会写入，但已有数据库记录没有启动回填逻辑。
3. 预先创建 Redis Stream `stream.orders` 的消费者组 `g1`。
4. 通过测试或独立初始化任务写入 `shop:geo:{typeId}`，否则带坐标的附近店铺查询为空。

## 7. 基线风险摘要

| 等级 | 风险 |
| --- | --- |
| P0 | 秒杀消费者读取 `stream.orders`，却对 `s1` 执行 ACK，消息会滞留 Pending List |
| P0 | 秒杀库存扣减与订单插入没有同一数据库事务，订单表也没有 `(user_id, voucher_id)` 唯一约束 |
| P0 | 配置中存在明文数据库密码；Redis 与上传路径硬编码，不支持环境隔离 |
| P1 | 秒杀消费者组、库存、GEO 数据依赖人工初始化，应用启动不自检 |
| P1 | 搜索是 MySQL `LIKE`，无全文检索、相关性、拼写容错和索引同步机制 |
| P1 | 店铺缓存只在本应用更新路径删除，外部写库会长期产生脏缓存 |
| P1 | `LOGIN_USER_TTL=36000` 且单位为分钟，即约 25 天滑动有效期；登出功能未实现 |
| P2 | 依赖栈较旧且存在跨代覆盖（Boot 2.3 + Spring Data Redis 2.6），缺少兼容性锁定与回归测试 |

## 8. 证据入口

- 构建与版本：`pom.xml`
- 运行配置：`src/main/resources/application.yaml`
- 数据基线：`src/main/resources/db/hmdp.sql`
- Redis 常量：`src/main/java/com/hmdp/utils/RedisConstants.java`
- 秒杀脚本：`src/main/resources/seckill.lua`
- 秒杀消费：`src/main/java/com/hmdp/service/impl/VoucherOrderServiceImpl.java`

