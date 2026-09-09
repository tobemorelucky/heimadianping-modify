# Canal 本地开发环境部署

## 1. 范围与部署结论

本阶段仅新增 Canal Docker 编排与环境文档，并对现有 MySQL 做只读检查；未修改 Java、application.yaml、SQL、Lua 或 Redis 逻辑。

截至 2026-09-09，`hmdp-canal` 已启动且为 `healthy`，`127.0.0.1:11111` 可达。`hmdp` destination 已连接宿主机 MySQL，成功定位 binlog 并进入 dump，订阅范围为 `hmdp.tb_shop`。

## 2. 版本选择

使用固定镜像 `canal/canal-server:v1.1.8`，避免 `latest` 漂移。1.1.8 是当前 Canal 发布版本，包含 MySQL 8.4 与 `caching_sha2_password` 等兼容改进；项目当前 MySQL 实测为 8.0.30。

## 3. 部署结构

```text
宿主机 MySQL :3306
        ↑ host.docker.internal
        │
hmdp-canal ── 127.0.0.1:11111 ── 后续 Java Canal Client
```

| 资源 | 名称 | 说明 |
| --- | --- | --- |
| 容器 | `hmdp-canal` | Canal Server 1.1.8 |
| 网络 | `hmdp-canal-network` | 独立 bridge 网络，不加入 Kafka、ES、Redis 网络 |
| 配置/位点卷 | `hmdp-canal-data` | 持久化 conf、meta、TSDB |
| 日志卷 | `hmdp-canal-logs` | 持久化服务与实例日志 |
| 对外端口 | `127.0.0.1:11111` | Canal Client TCP，仅绑定本机 |

本阶段未接入 Canal Admin 或独立 metrics 采集，因此不暴露 11110、11112。

## 4. MySQL binlog

### 4.1 必要配置

```ini
[mysqld]
server-id=1
log-bin=mysql-bin
binlog-format=ROW
binlog-row-image=FULL
```

- `server-id` 是复制拓扑中的 MySQL 唯一标识。
- `log-bin` 开启 binary log。
- `ROW` 记录行级变化，Canal 可稳定还原 INSERT、UPDATE、DELETE。
- `FULL` 保留完整行镜像，便于提取主键和变更前后值。

修改配置后需重启 MySQL；生产环境还应评估重启窗口和 binlog 磁盘保留策略。

### 4.2 只读检查

```sql
SELECT VERSION();
SHOW VARIABLES WHERE Variable_name IN
  ('log_bin', 'binlog_format', 'binlog_row_image', 'server_id');
SHOW MASTER STATUS;
```

| 项目 | 本机结果 |
| --- | --- |
| MySQL | 8.0.30 |
| `log_bin` | `ON` |
| `binlog_format` | `ROW` |
| `binlog_row_image` | `FULL` |
| `server_id` | `1` |
| Master Status | 可返回 binlog 文件与 position |

以上检查全部通过，且只执行查询。

## 5. Canal 专用账号

建议使用独立的最小权限账号，不应长期使用应用账号或 root。以下 SQL 仅供数据库管理员后续执行，本阶段未执行：

```sql
CREATE USER 'canal'@'%' IDENTIFIED BY '<strong-password>';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
FLUSH PRIVILEGES;
SHOW GRANTS FOR 'canal'@'%';
```

- `REPLICATION SLAVE`：建立 binlog dump 复制连接。
- `REPLICATION CLIENT`：读取 master/binlog 状态。
- `SELECT`：读取必要的元数据和表结构。

当前只读检查未发现 `canal` 专用账号。本次启动验证为了不执行 SQL，临时从现有 `application.yaml` 读取数据源凭据并仅注入启动进程；凭据未写入 Compose 或文档。正式联调前应由数据库管理员创建专用账号。

## 6. Compose 配置

文件：`docker/canal/docker-compose-canal.yml`。

- destination：`hmdp`。
- MySQL 地址：默认 `host.docker.internal:3306`。
- 表过滤：默认 `hmdp\.tb_shop`，只解析店铺表。
- Canal replica id：默认 `1234`，须与 MySQL `server_id` 和其他复制客户端不同。
- `canal.auto.scan=false`：只加载声明的 `hmdp` destination。
- 账号密码只通过环境变量注入，不提交仓库。

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `CANAL_DB_HOST` | `host.docker.internal` | MySQL 主机 |
| `CANAL_DB_PORT` | `3306` | MySQL 端口 |
| `CANAL_DB_USERNAME` | `canal` | Canal MySQL 账号 |
| `CANAL_DB_PASSWORD` | `canal` | 本地占位值，实际启动必须覆盖 |
| `CANAL_SLAVE_ID` | `1234` | Canal 模拟副本 ID |
| `CANAL_FILTER_REGEX` | `hmdp\.tb_shop` | 订阅表正则 |

## 7. Docker 启停

PowerShell 当前会话先设置专用账号凭据：

```powershell
$env:CANAL_DB_USERNAME = 'canal'
$env:CANAL_DB_PASSWORD = '<strong-password>'
docker compose -f docker/canal/docker-compose-canal.yml up -d
```

查看状态和日志：

```powershell
docker compose -f docker/canal/docker-compose-canal.yml ps
docker logs hmdp-canal --tail 200
docker exec hmdp-canal sh -lc "tail -n 100 /home/admin/canal-server/logs/hmdp/hmdp.log"
```

停止但保留持久化数据：

```powershell
docker compose -f docker/canal/docker-compose-canal.yml down
```

不要随意追加 `-v`；该参数会删除 Canal 消费位点、TSDB 和日志命名卷。

## 8. 配置与运行验证

### 8.1 Compose 静态校验

```powershell
docker compose -f docker/canal/docker-compose-canal.yml config
```

2026-09-09 实测退出码为 `0`，服务、独立网络、端口、命名卷和环境变量均成功展开。沙箱曾提示无法读取用户级 Docker `config.json`，但不影响 Compose 文件解析。

### 8.2 运行状态

```text
NAME         IMAGE                       STATUS
hmdp-canal   canal/canal-server:v1.1.8   Up (healthy)
```

验证结果：

- `Test-NetConnection 127.0.0.1 -Port 11111` 返回 `True`。
- Canal Server 日志出现 `start canal successful`。
- `hmdp` 实例日志出现 `start successful`。
- 已识别宿主机 MySQL Community Server。
- 已成功找到当前 binlog 起点，日志进入 `the next step is binlog dump`。
- 最终表过滤器为 `^hmdp\.tb_shop$`。

以上证明 Canal Server、端口、MySQL 网络连通、数据库鉴权和 binlog 解析链路均已建立。本阶段未写业务数据，因此未制造 UPDATE 验证下游消费；该项留给 Canal Client 实现阶段。

## 9. 常见问题

### 9.1 `Access denied for user`

确认已创建 `'canal'@'%'`、具备复制权限，并在启动 Compose 的同一终端设置账号密码环境变量。不要将密码写入仓库。

### 9.2 容器无法连接 `127.0.0.1:3306`

容器中的 `127.0.0.1` 指向容器自身。Docker Desktop 应通过 `host.docker.internal` 访问宿主机；若 MySQL 不在宿主机，使用 `CANAL_DB_HOST` 指向实际地址。

### 9.3 找不到 binlog 或无法 dump

检查 `log_bin=ON`、`binlog_format=ROW`、复制权限与 binlog 保留时间，并确保 `CANAL_SLAVE_ID` 不与其他 replica 冲突。

### 9.4 容器健康但没有事件

健康检查只证明 11111 可用。还需查看 `logs/hmdp/hmdp.log`，确认数据库连接、binlog 起点以及过滤器为 `^hmdp\.tb_shop$`。

### 9.5 重建后位点异常

先检查 `hmdp-canal-data` 中的 meta/TSDB 和实例日志，不要直接删除 volume；清理位点可能造成重复消费或从错误位置重拉。

## 10. 后续开发入口

下一阶段可新增 Canal Client、店铺事件转换和 Redis 缓存删除服务。实现应遵循 Phase 1 设计：目标缓存删除成功才 ACK，失败 rollback；重复事件依赖 Redis `DEL` 幂等安全重放。

## 11. 参考资料

- Alibaba Canal Releases：https://github.com/alibaba/canal/releases
- Alibaba Canal AdminGuide：https://github.com/alibaba/canal/wiki/AdminGuide
- Alibaba Canal Docker QuickStart：https://github.com/alibaba/canal/wiki/Docker-QuickStart
