# 项目运行时架构

## 一、当前运行单元

HM-DianPing Plus 由 Nginx 前端、两个应用进程和六个 Docker 基础组件组成。

```text
客户端
  ├── HTTP :8080 → Nginx 静态前端
  └── HTTP :8000 → FastAPI AI Assistant

Nginx
  ├── 静态页面 → html/hmdp
  └── /api → Spring Boot :8081

Spring Boot
  ├── MySQL :3306/3307
  ├── Redis :6379
  ├── Kafka :9092
  ├── Elasticsearch :9200
  └── Canal Client :11111

FastAPI
  ├── DeepSeek OpenAI-compatible API
  └── Spring Boot AI API :8081

Kibana :5601 → Elasticsearch
Canal → MySQL ROW binlog
```

### 1.1 Docker 组件

| 组件 | 镜像 | 容器名 | 端口 | 持久化 | 作用 |
| --- | --- | --- | --- | --- | --- |
| MySQL | `mysql:8.0.30` | `hmdp-mysql` | 默认 3306，冲突时脚本使用 3307 | `hmdp-mysql-data` | 权威业务数据；开启 ROW binlog 供 Canal 读取 |
| Redis | `redis:7-alpine` | `hmdp-redis` | 6379 | `hmdp-redis-data` | 缓存、登录态、Lua 秒杀准入、Redis Stream 回退 |
| Kafka | `apache/kafka:3.9.2` | `hmdp-kafka` | 9092 | `hmdp-kafka-data` | 秒杀订单异步消息、Retry 和 DLT |
| Elasticsearch | `elasticsearch:8.19.21` | `hmdp-elasticsearch` | 9200 | `hmdp-elasticsearch-data` | 商户名称检索 |
| Kibana | `kibana:8.19.21` | `hmdp-kibana` | 5601 | `hmdp-kibana-data` | ES 本地管理和查询验证 |
| Canal | `canal-server:1.1.8` | `hmdp-canal` | 11111 | `hmdp-canal-data`、`hmdp-canal-logs` | 订阅 MySQL `tb_shop` binlog，驱动缓存失效 |

MySQL 在统一 Compose 中使用项目现有 `src/main/resources/db/hmdp.sql` 进行首次数据卷初始化。初始化脚本只在命名卷首次创建时运行。MySQL 启用 `server-id`、`log-bin`、`binlog-format=ROW` 和 `binlog-row-image=FULL`，保持 Canal 所需条件。

### 1.2 已有独立 Compose

| 文件 | 原有启动方式 |
| --- | --- |
| `docker/kafka/docker-compose-kafka.yml` | `docker compose -f docker/kafka/docker-compose-kafka.yml up -d` |
| `docker/elasticsearch/docker-compose-es.yml` | `docker compose -f docker/elasticsearch/docker-compose-es.yml up -d` |
| `docker/canal/docker-compose-canal.yml` | 注入 Canal 数据库变量后执行对应 `up -d` |

三个文件继续保留且内容未修改，仍可独立使用。根 Compose 使用 `include` 直接复用这些已验证定义，避免复制镜像、端口、网络、健康检查和数据卷配置后产生漂移。

## 二、当前应用启动方式

### 2.1 Spring Boot

原有方式：

```powershell
mvn spring-boot:run
```

服务监听 `127.0.0.1:8081`。统一脚本仍使用 Maven 启动，不改变应用配置或业务代码；运行日志和 PID 写入被 Git 忽略的 `target/runtime/`。

### 2.2 FastAPI

原有方式：

```powershell
cd ai-assistant
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

统一脚本使用非 reload 方式启动 `127.0.0.1:8000`，适合稳定的本地全栈联调。LLM 和 Spring Boot 地址继续由 `ai-assistant/.env` 读取，脚本不会生成、输出或改写 API Key。

### 2.3 Nginx 前端

项目自带 Windows Nginx 和静态页面，配置监听 `127.0.0.1:8080`，将 `/api` 请求反向代理到 Spring Boot 8081。统一脚本启动前执行 `nginx -t`，启动后等待 8080 端口就绪，并保存 Nginx master PID。

## 三、统一管理设计

### 3.1 根 Compose

根目录 `docker-compose.yml` 是 Docker 配置聚合入口：

- 新增 MySQL、Redis 定义。
- `include` 原有 Kafka、ES/Kibana、Canal Compose。
- 保留所有原命名卷，停止时不使用 `-v`。
- 保留 ES 与 Canal 原独立网络；MySQL、Redis、Kafka 使用统一 project 默认网络，但应用仍通过宿主机端口访问。
- 使用 `MYSQL_ROOT_PASSWORD`、`MYSQL_HOST_PORT` 和原有 Canal 环境变量覆盖本地默认值。

由于旧组件可能已通过三个子 Compose 启动，它们带有不同 Compose project 标签。Docker 不能让新 project 直接接管同名容器，因此 `start-all.ps1` 首次统一启动前会对旧 project 执行不带 `-v` 的 `down`：仅移除旧容器和网络，保留命名卷，然后由 `hmdp-local` 统一 project 使用相同容器名和卷重新创建。

### 3.2 一键启动

`scripts/start-all.ps1` 顺序如下：

1. 迁移旧 Kafka、ES、Canal Compose project，保留数据卷。
2. 启动 MySQL 和 Redis，并等待健康检查通过。
3. 启动 Kafka、Elasticsearch、Kibana、Canal。
4. 启动 Spring Boot，保存 PID，等待 8081 监听。
5. 校验并启动 Nginx 前端，保存 master PID，等待 8080 监听。
6. 检查 AI `.env` 和 Python 虚拟环境；必要时创建虚拟环境并安装依赖。
7. 启动 FastAPI，保存 PID，等待 8000 监听。

如果宿主机已有 MySQL 占用 3306，首次运行自动选择 3307，并通过 `SPRING_DATASOURCE_URL` 与 `CANAL_DB_PORT` 把实际端口传给 Java 和 Canal。选择结果保存在 `target/runtime/mysql-host-port.txt`，保证后续重启端口稳定。

### 3.3 一键停止

`scripts/stop-all.ps1`：

1. 使用 `nginx -s quit` 优雅停止受管 Nginx，超时后仅终止记录的 master PID。
2. 通过 PID 文件停止 FastAPI 进程树。
3. 通过 PID 文件停止 Maven/Spring Boot 进程树。
4. 对统一 Compose 执行 `down --remove-orphans`。
5. 兼容性清理独立子 Compose project。

停止命令不带 `-v`，MySQL、Redis、Kafka、ES、Kibana 和 Canal 数据卷均保留。

### 3.4 状态检查

`scripts/status.ps1` 同时展示：

- 根 Compose 容器状态。
- Nginx Frontend、Spring Boot、AI Assistant PID 文件与进程状态。
- 8080、8081、8000 是否监听以及占用进程 ID。

脚本只终止自身 PID 文件记录的应用进程，不会根据端口强制结束未知进程，避免误杀用户手动启动的 Java 或 Python 服务。

## 四、管理边界

- 根 Compose 只改变本地运行编排，不改变 Kafka Topic、ES Mapping、Canal 订阅、Redis Key、Lua 或 AI Agent。
- 原三个 Compose 文件没有被修改或删除。
- 所有应用日志和 PID 都位于 `target/runtime`，不会污染源码目录或 Git。
- `down` 不删除命名卷；只有用户明确执行 `docker compose down -v` 才会删除持久化数据，不应在日常停机中使用。
- AI `.env` 继续由 Git 忽略；统一脚本不保存任何 LLM 密钥。

## 五、验证记录

- `docker compose -f docker-compose.yml config`：退出码 0，六个组件、网络、端口、健康检查和命名卷均成功展开。
- `start-all.ps1`、`stop-all.ps1`、`status.ps1`：PowerShell AST 语法解析全部通过。
- `status.ps1`：实际执行成功，能够输出 Docker 可用性、受管进程以及 8081/8000 端口状态。
- 首次直接根 Compose 启动成功完成缺失镜像拉取，并识别出旧 project 同名容器冲突；脚本随后增加了保留卷的旧 project 迁移步骤。
- 当前受控执行环境对 Docker Desktop 命名管道的提升权限审核持续超时，因此修正后的完整 `start-all → stop-all` 生命周期未能在本轮自动化环境内复跑。该项属于运行权限限制，不能记录为已通过；需在用户本机普通 PowerShell 中执行启动指南中的验收命令完成最终确认。
