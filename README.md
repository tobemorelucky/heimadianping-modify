# hm-dianping-total（黑马点评 · Redis 实战综合项目）

一个仿「大众点评 / 美团」的本地生活服务练习项目，源自黑马程序员《Redis 实战》课程（作者：虎哥）。

本仓库是**综合总仓库（total）版本**：单模块 Spring Boot 后端 + 内置 Nginx 托管的纯 HTML 前端（Vue2 + Element UI + Axios），
将课程各阶段的知识点集中在一个工程中，重点演示 **Redis 在高并发 / 缓存场景下的综合应用**。

> 学习/课程向项目，默认数据库账号为课程示例配置，仅用于本地学习环境。

---

## ✨ 项目亮点

| 模块 | 业务场景 | 用到的 Redis 知识点 |
| --- | --- | --- |
| 短信登录 | 验证码、登录态管理 | String / Hash 存储、Token 机制、Key 过期时间 |
| 商户查询缓存 | 热点店铺详情读写 | 缓存穿透（缓存空值）、缓存击穿（逻辑过期 / 互斥锁）、双写一致性（更新库删缓存） |
| 缓存工具封装 | 通用缓存读写 | `CacheClient` 泛型工具：空值缓存、逻辑过期、互斥锁、异步缓存重建 |
| 全局唯一 ID | 订单号生成 | 时间戳(秒)左移 32 位 \| 自增序列（`INCR` 按天分 key） |
| 附近商铺 | 按距离排序的店铺列表 | Redis GEO（`GEOSEARCH` 按经纬度 + 距离分页） |
| 签到统计 | 用户按月签到、连续签到天数 | BitMap（`SETBIT` / `BITFIELD`） |
| 优惠券秒杀 | 高并发下单、一人一单 | Lua 脚本原子判断（库存 + 防重复下单）、Redis Stream 消费者组异步下单、Redisson / 自研 `SimpleRedisLock` 分布式锁 |
| 好友关注 | 关注、取关、共同关注 | Set（`SINTER` 求交集） |
| 关注推送 | 关注的人发笔记后按时间倒序推送 | ZSet 收件箱（推模式）、Score 滚动分页 |
| 笔记点赞 | 点赞 / 取消、点赞排行榜 | ZSet（按时间排序）、取 Top5 点赞用户 |

---

## 🧱 技术栈

- **JDK 1.8** / Maven
- **Spring Boot 2.3.12.RELEASE**（spring-boot-starter-web / data-redis / test）
- **MyBatis-Plus 3.4.3**（含 `mybatis-plus-boot-starter`）
- **MySQL 5.x**（驱动 `mysql-connector-java 5.1.47`）+ 初始化脚本 `db/hmdp.sql`
- **Redis 5.0+**（需支持 Stream、GEO）客户端：**Lettuce 6.1.6 + commons-pool2**
- **Redisson 3.13.6**（分布式锁）
- **Hutool 5.7.17**（工具库）
- **Lombok**
- 前端：内置 **Nginx 1.18.0** 静态托管，页面基于 Vue2 / Element UI / Axios（`nginx-1.18.0/html/hmdp/`）

---

## 🚀 快速开始

### 1. 环境准备

| 依赖 | 说明 |
| --- | --- |
| JDK 8 | 项目编译运行 |
| Maven 3.6+ | 构建（也可用 IDEA 内置） |
| MySQL | 5.x（如本机为 8.x 需替换驱动与 url 参数） |
| Redis | 5.0 及以上 |

### 2. 初始化数据库

用任意 MySQL 客户端执行建库建表脚本（内置 10 张表 + 演示数据 + 签到等前置数据）：

```sql
-- 先建库（脚本注释头提到源库名 hmdp2，可自行改库名）
CREATE DATABASE IF NOT EXISTS hmdp DEFAULT CHARACTER SET utf8mb4;
USE hmdp;
source src/main/resources/db/hmdp.sql;   -- 或直接用 Navicat/IDEA 导入
```

### 3. 修改配置

编辑 `src/main/resources/application.yaml`，按本机环境调整：

```yaml
server:
  port: 8081
spring:
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/hmdp?useSSL=false&serverTimezone=UTC
    username: root
    password: 123456        # ← 改成你的 MySQL 密码
  redis:
    host: 127.0.0.1
    port: 6379
    # password: xxx          # 本地 Redis 无密码可留空
```

### 4. 启动后端

IDEA 直接运行主类，或命令行：

```bash
mvn spring-boot:run
# 或
mvn clean package -DskipTests && java -jar target/hm-dianping-0.0.1-SNAPSHOT.jar
```

启动后后端监听 **http://localhost:8081**（默认账号登录逻辑：任意手机号 + 控制台打印的验证码）。

### 5. 启动前端

前端为纯静态页面，直接打开本仓库自带的 Nginx：

```bash
cd nginx-1.18.0
start nginx.exe        # Windows：双击 nginx.exe 或执行该命令
```

浏览器访问 **http://localhost:80**（Nginx 默认端口），页面位于 `html/hmdp/`，包含：登录、首页、店铺列表/详情、探店笔记、发布笔记、个人中心等页面。
前端通过 `html/hmdp/js/common.js` 里的 `axios.defaults.baseURL` 指向后端 8081 端口。

> 也可以不用 Nginx，用任意静态服务器托管 `nginx-1.18.0/html/hmdp/` 目录即可。

---

## 📁 项目结构

```
hm-dianping-total
├── pom.xml                              # Maven 配置（Spring Boot 2.3 / MP / Redisson / Hutool）
├── nginx-1.18.0/                        # 内置 Nginx + 前端静态页面（html/hmdp）
└── src/main
    ├── java/com/hmdp
    │   ├── HmDianPingApplication.java   # 启动类（@MapperScan）
    │   ├── config/                      # MvcConfig(拦截器) / RedissonConfig / MybatisConfig / WebExceptionAdvice
    │   ├── controller/                  # 9 个 Controller：user/shop/shop-type/blog/voucher/voucher-order/follow/blog-comments/upload
    │   ├── service/ + impl/             # 业务接口与实现（核心逻辑所在）
    │   ├── mapper/                      # MyBatis-Plus Mapper
    │   ├── entity/                      # 10 张表对应实体（User/Shop/Blog/Voucher/VoucherOrder/...）
    │   ├── dto/                         # Result / LoginFormDTO / UserDTO / ScrollResult
    │   └── utils/                       # 见下表
    └── resources
        ├── application.yaml             # 应用配置（端口 8081）
        ├── seckill.lua                  # 秒杀 Lua 脚本（校验库存 + 一人一单 + 入队）
        ├── unlock.lua                   # 分布式锁释放脚本（校验标识再删除）
        ├── mapper/VoucherMapper.xml     # 自定义 SQL（优惠券 + 秒杀券联查）
        └── db/hmdp.sql                  # 建库脚本 + 数据
```

### 关键工具类（`utils/`）

| 类 | 作用 |
| --- | --- |
| `CacheClient` | 通用缓存封装：`queryWithPassThrough`（空值防穿透）、`queryWithMutex`（互斥锁）、`queryWithLogicalExpire`（逻辑过期，含异步重建） |
| `RedisIdWorker` | 全局唯一 ID：`时间戳 << 32 \| 序列号`，序列号按天 `INCR` |
| `SimpleRedisLock` / `ILock` | 手写分布式锁（SET NX EX + Lua 释放），对比 Redisson |
| `RefreshTokenInterceptor` / `LoginInterceptor` | 双拦截器：先刷新 Token 有效期，再校验登录，用户信息放 `UserHolder`（ThreadLocal） |
| `UserHolder` | ThreadLocal 保存当前登录用户 |
| `RedisConstants` | Redis Key 前缀与 TTL 常量 |
| `RedisData` | 逻辑过期时间包装对象 |

---

## 🔑 Redis Key 设计一览

| Key 模式 | 类型 | 说明 |
| --- | --- | --- |
| `login:code:{phone}` | String | 短信验证码，2 分钟过期 |
| `login:token:{token}` | Hash | 登录用户信息，Token 续期 |
| `cache:shop:{id}` | String(JSON) | 店铺缓存，30 分钟 |
| `lock:shop:{id}` | String | 缓存重建互斥锁（10s） |
| `seckill:stock:{voucherId}` | String | 秒杀库存 |
| `seckill:order:{voucherId}` | Set | 已抢购用户（一人一单判重） |
| `stream.orders` | Stream | 秒杀下单消息队列（消费者组 g1） |
| `lock:order:{userId}` | String | Redisson 分布式锁（防重复下单） |
| `blog:liked:{blogId}` | ZSet | 点赞用户，Score=点赞时间 |
| `feed:{userId}` | ZSet | 关注推送收件箱，Score=发布时间 |
| `shop:geo:{typeId}` | GEO | 店铺坐标，按距离搜索 |
| `sign:{userId}:{yyyyMM}` | BitMap | 按月签到位图 |

---

## 🗄️ 数据库表（`db/hmdp.sql`）

| 表 | 说明 |
| --- | --- |
| `tb_user` | 用户（手机号 + 密码 + 头像 + 昵称） |
| `tb_user_info` | 用户扩展信息（签名/地区/性别等） |
| `tb_shop` | 商户（含经纬度、类型） |
| `tb_shop_type` | 商户分类（美食/ KTV 等） |
| `tb_blog` | 探店笔记（标题、图片、内容、点赞评论数） |
| `tb_blog_comments` | 笔记评论 |
| `tb_follow` | 关注关系 |
| `tb_voucher` | 优惠券（普通券） |
| `tb_seckill_voucher` | 秒杀券（库存、开始/结束时间） |
| `tb_voucher_order` | 优惠券订单 |

---

## 🌐 主要 API 一览

| 模块 | 接口（前缀） | 说明 |
| --- | --- | --- |
| 用户 | `/user/code`、`/user/login`、`/user/logout`、`/user/me`、`/user/sign`、`/user/sign/count` | 发送验证码 / 登录 / 签到（BitMap）/ 连续签到天数 |
| 商户 | `/shop/{id}`、`/shop/of/type`、`/shop/of/name` | 缓存查询、按类型/距离查询（GEO） |
| 分类 | `/shop-type/list` | 商户分类列表 |
| 笔记 | `/blog/hot`、`/blog/{id}`、`/blog/like/{id}`、`/blog/likes/{id}`、`/blog/of/follow`、`/blog/of/user`、`/blog/of/me` | 热门笔记 / 点赞（ZSet）/ 关注推送流（滚动分页） |
| 优惠券 | `/voucher/list/{shopId}`、`/voucher/seckill` | 店铺券列表、新增秒杀券 |
| 秒杀下单 | `/voucher-order/seckill/{id}` | Lua 校验 → Stream 异步下单（返回订单 ID） |
| 关注 | `/follow/{id}/{isFollow}`、`/follow/or/not/{id}`、`/follow/common/{id}` | 关注 / 是否关注 / 共同关注（Set 交集） |
| 上传 | `/upload/blog` | 笔记图片上传 |

> 登录相关拦截：`MvcConfig` 中放行 `/shop/**`、`/voucher/**`、`/shop-type/**`、`/upload/**`、`/blog/hot`、`/user/code`、`/user/login`，其余接口需要登录态。

---

## 💡 秒杀流程（最终版，可对照学习演进）

1. **预热**：发布秒杀券时把库存写入 Redis（`seckill:stock:{id}`）。
2. **原子校验**：执行 `seckill.lua` —— 判断库存 > 0、用户未下单（Set 判重）、`INCRBY` 扣库存、`SADD` 记录用户、`XADD` 写入 `stream.orders` 消息。
3. **异步下单**：`@PostConstruct` 启动单线程消费者，`XREADGROUP` 读取消息 → 校验 → Redisson 锁内写库建单 → `XACK` 确认；异常时扫描 Pending List 兜底重试。
4. `VoucherOrderServiceImpl` 内保留了大量**被注释的演进版本**（synchronized 单机锁 → 手写 `SimpleRedisLock` → Redisson 锁；阻塞队列 → Redis Stream），便于逐版本理解「为什么越做越复杂」。

---

## ⚠️ 注意事项

1. **Redis 版本**：秒杀依赖 Stream（Redis 5.0+），GEO 依赖 Redis 3.2+，建议本地使用 Redis 5.0 及以上。
2. **MySQL 版本**：pom 中使用 MySQL 5.x 驱动（`com.mysql.jdbc.Driver`）；若本机是 MySQL 8.x，请将驱动升级为 `mysql-connector-java 8.x` 并把 url 参数调整（如 `useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true`）。
3. **验证码**：项目不接真实短信服务，验证码通过后端日志（`logging.level.com.hmdp: debug`）打印，登录时填任意**未注册手机号会自动注册**。
4. **秒杀消息队列**：首次启动前若消费报错，可手动创建消费者组：
   ```bash
   redis-cli XGROUP CREATE stream.orders g1 0 MKSTREAM
   ```
5. **附近店铺（GEO）**：`shop:geo:{typeId}` 坐标数据需先写入（可参考课程/自行用 `GEOADD` 初始化），否则按距离查询结果为空。
6. 项目内 `nginx-1.18.0/conf/nginx.conf` 可按需修改前端端口与静态目录；`nginx-1.18.0` 为官方 Windows 发行版，直接拷贝使用。

---

## 📚 学习建议

- 按「缓存 → 分布式锁 → 秒杀 → Feed 流」的顺序阅读 `service/impl/` 下的实现；
- 对比 `VoucherOrderServiceImpl` 中被注释的多个版本，理解单机锁 → 分布式锁 → Lua + 异步队列的演进动机；
- 结合 `utils/CacheClient.java` 理解缓存穿透 / 击穿 / 雪崩三种问题的对应解法；
- 本项目为教学示例，登录鉴权与并发控制均以演示为主，生产环境还需补充更完善的安全与可靠性设计。

---

*README 由仓库结构、源码与配置归纳生成；如与实际运行行为有出入，以代码为准。*
