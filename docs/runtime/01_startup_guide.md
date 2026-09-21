# HM-DianPing Plus 一键启动指南

## 一、环境要求

克隆项目后，先安装：

- Windows PowerShell 5.1 或 PowerShell 7
- Docker Desktop，确保 `docker compose version` 可执行
- JDK 8 或兼容 JDK
- Maven 3.6+
- Python 3.10/3.11

首次启动需要下载 MySQL、Redis、Kafka、Elasticsearch、Kibana 和 Canal 镜像。Kibana 镜像较大，耗时取决于网络速度。

## 二、配置 AI 服务

项目无法预置个人 LLM 密钥。首次启动前执行：

```powershell
cd E:\work\code\java\hm-dianping-total
Copy-Item ai-assistant\.env.example ai-assistant\.env
```

编辑 `ai-assistant/.env`：

```dotenv
LLM_BASE_URL=<OpenAI-compatible API 地址>
LLM_API_KEY=<本地 API Key>
LLM_MODEL=<支持 Tool Calling 的模型>
SPRING_BOOT_BASE_URL=http://localhost:8081
```

`.env` 已被 Git 忽略。不要把真实 API Key 写入 Markdown、提交记录或截图。

## 三、一键启动

在项目根目录执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start-all.ps1
```

PowerShell 7 也可以使用：

```powershell
pwsh -File .\scripts\start-all.ps1
```

脚本会自动：

1. 将原有 Kafka、ES、Canal 独立 Compose 容器迁移到统一 project，命名卷不会删除。
2. 启动并等待 MySQL、Redis 健康。
3. 启动 Kafka、Elasticsearch、Kibana、Canal。
4. 启动 Spring Boot。
5. 校验 Nginx 配置并启动静态前端。
6. 检查 Python 虚拟环境；不存在时自动创建并安装 `requirements.txt`。
7. 启动 FastAPI。

启动成功后的地址：

| 服务 | 地址 |
| --- | --- |
| 黑马点评前端 | `http://127.0.0.1:8080` |
| Spring Boot | `http://127.0.0.1:8081` |
| FastAPI 文档 | `http://127.0.0.1:8000/docs` |
| Kibana | `http://127.0.0.1:5601` |
| Elasticsearch | `http://127.0.0.1:9200` |

运行日志：

```text
target/runtime/spring-boot.out.log
target/runtime/spring-boot.error.log
target/runtime/ai-assistant.out.log
target/runtime/ai-assistant.error.log
```

发布笔记上传的图片默认保存到项目内：

```text
nginx-1.18.0/html/hmdp/imgs/blogs/{一级散列目录}/{二级散列目录}/
```

接口仍返回 `/blogs/...`，前端继续拼接为 `/imgs/blogs/...`，无需修改页面协议。部署到其他目录时可使用 `HMDP_IMAGE_UPLOAD_DIR` 环境变量覆盖物理图片根目录。

## 四、查看状态

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\status.ps1
```

输出包括：

- 六个 Docker 组件的容器和健康状态。
- Nginx 前端、Spring Boot、FastAPI 的受管 PID 状态。
- 8080、8081、8000 端口监听状态。

也可以直接查看统一 Compose：

```powershell
docker compose ps
```

## 五、一键停止

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stop-all.ps1
```

脚本会停止 Nginx 前端、两个应用进程和全部项目容器，但保留所有命名卷。下次启动会继续使用 MySQL 数据、Redis AOF、Kafka 日志、ES/Kibana 数据和 Canal 位点。

不要在日常停止时追加 `-v`：

```powershell
# 危险：会删除持久化卷，不是日常停止命令
docker compose down -v
```

## 六、配置覆盖

默认 MySQL root 密码与当前本地 Spring Boot 示例配置一致，为 `123456`。需要覆盖时，在启动脚本所在终端设置：

```powershell
$env:MYSQL_ROOT_PASSWORD = '<local-password>'
$env:CANAL_DB_USERNAME = 'root'
$env:CANAL_DB_PASSWORD = '<local-password>'
.\scripts\start-all.ps1
```

根 `docker-compose.yml`、Spring Boot 和根 Compose 下的 Canal 默认统一使用宿主机 `3307`，因此可直接运行 `docker compose up -d`，不会与常见的本机 MySQL `3306` 冲突。一键脚本仍会记录并同步实际端口；也可以显式指定：

```powershell
$env:MYSQL_HOST_PORT = '3307'
.\scripts\start-all.ps1
```

项目初始化 SQL 源自 MySQL 5.6，并包含秒杀券时间字段的零日期默认值。统一 Compose 仅关闭 MySQL 8 的 `NO_ZERO_DATE`/`NO_ZERO_IN_DATE` 检查，继续保留严格事务、除零错误和完整分组检查。MySQL 健康检查同时验证项目所需的 10 张表，初始化中途失败时不会再误报健康并继续启动后端。

## 七、首次启动验收

### 7.1 Compose 配置

```powershell
docker compose config
```

预期：退出码 0，并包含 `mysql`、`redis`、`kafka`、`elasticsearch`、`kibana`、`canal-server`。

### 7.2 容器与端口

```powershell
.\scripts\status.ps1
Test-NetConnection 127.0.0.1 -Port 8081
Test-NetConnection 127.0.0.1 -Port 8000
Test-NetConnection 127.0.0.1 -Port 8080
```

预期：三个端口的 `TcpTestSucceeded=True`。

### 7.3 HTTP 检查

```powershell
Invoke-RestMethod http://127.0.0.1:8081/api/ai/shop/1
Invoke-RestMethod http://127.0.0.1:9200/_cluster/health
Invoke-RestMethod http://127.0.0.1:8000/docs
Invoke-WebRequest http://127.0.0.1:8080/
```

AI Tool Calling 验证：

```powershell
$body = @{ message = '分析一下店铺1最近经营情况' } | ConvertTo-Json
Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/chat `
  -ContentType 'application/json; charset=utf-8' `
  -Body ([System.Text.Encoding]::UTF8.GetBytes($body))
```

## 八、常见问题

### 8.1 Docker Desktop 未启动

如果出现 Docker named pipe、daemon 或 engine 连接失败，先启动 Docker Desktop，再执行 `docker version`。

### 8.2 旧容器名称冲突

新版 `start-all.ps1` 会自动迁移三个旧 Compose project，并且不删除数据卷。若此前启动过程被强制中断，可手动执行：

```powershell
docker compose -f docker/kafka/docker-compose-kafka.yml down
docker compose -f docker/elasticsearch/docker-compose-es.yml down
docker compose -f docker/canal/docker-compose-canal.yml down
.\scripts\start-all.ps1
```

这些命令不要追加 `-v`。

### 8.3 Spring Boot 启动失败

查看：

```powershell
Get-Content target/runtime/spring-boot.error.log -Tail 200
Get-Content target/runtime/spring-boot.out.log -Tail 200
```

重点检查 MySQL、Redis、Kafka 和 Canal 是否就绪，以及 8081 是否被其他进程占用。

### 8.4 FastAPI 启动失败

查看：

```powershell
Get-Content target/runtime/ai-assistant.error.log -Tail 200
```

确认 `.env` 存在、模型支持 Tool Calling，并检查虚拟环境依赖是否安装成功。

### 8.5 PID 文件过期

`status.ps1` 会把 PID 文件存在但进程消失显示为 `stopped`。执行 `stop-all.ps1` 会清理过期 PID 文件，不会根据端口误杀未知进程。

### 8.6 Canal 提示 PositionNotFound

如果 Canal 日志持续出现 `PositionNotFoundException`，通常是 Canal 命名卷保存的位点来自另一套 MySQL 或已被清理的 binlog。不要删除 MySQL 数据卷；先备份 `hmdp-canal-data` 中 `conf/hmdp/meta.dat` 与 `h2.mv.db`，在 Canal 停止状态下移走这两个可重建元数据文件，再启动 Canal。重置后应通过一次 `tb_shop` 更新与 `cache:shop:{id}` 删除测试确认链路。

### 8.7 店铺详情返回空对象

当前店铺详情使用缓存穿透格式，即 `cache:shop:{id}` 直接保存 `Shop` JSON。课程中的逻辑过期缓存格式会额外包装 `data` 和 `expireTime`，两种格式不能混用。这组会启动完整上下文并写真实 Redis 的课程 Demo 测试已整体标记为手工禁用，标准 `mvn test` 不再污染开发缓存。如果从旧环境带入过这种缓存，只删除对应 `cache:shop:{id}`，下次查询会按现有业务逻辑自动重建。

`RedissonTest` 也是需要连接真实 Redis 的课程手工锁演示，已从标准回归中隔离。需要演示锁时，应在确认 Kafka Topic 没有待处理生产消息后，由 IDE 单独移除/覆盖禁用状态运行，避免完整 Spring 测试上下文临时加入正式消费者组。
