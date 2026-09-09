# Kafka 本地开发环境搭建

## 1. 环境目标

本环境为 heimadianping-modify 的 Kafka 秒杀开发与故障演练提供单节点 Kafka。它使用 KRaft combined mode，由同一个容器同时承担 broker 和 controller，不启动、不依赖 ZooKeeper。

配置文件：`docker/kafka/docker-compose-kafka.yml`

## 2. Kafka 版本选择

镜像固定为：

```text
apache/kafka:3.9.2
```

选择原因：

- 使用 Apache Kafka 官方 JVM 镜像，避免第三方镜像环境变量和维护周期差异。
- `3.9.2` 是 Kafka 3.9 的补丁版本，固定版本标签可避免 `latest` 漂移造成环境不可复现。
- 官方镜像原生支持 KRaft，单节点开发环境不需要 ZooKeeper。
- 当前项目 Spring Kafka 2.5.14 使用 Kafka Client 2.5.1；Kafka 协议支持客户端与 broker 协商共同 API 版本。开发环境暂留在 Kafka 3.x，避免在未升级客户端前直接引入 Kafka 4.x 的兼容变量。
- 本配置仅用于本地开发。单 broker、复制因子 1 不具备生产容灾能力。

参考资料：

- [Apache Kafka 3.9 Docker 文档](https://kafka.apache.org/39/getting-started/docker/)
- [Apache Kafka 官方 Docker 镜像使用指南](https://github.com/apache/kafka/blob/3.9.2/docker/examples/README.md)
- [Apache Kafka 协议兼容性设计](https://kafka.apache.org/39/design/protocol/)

## 3. Docker 启动方式

在项目根目录执行：

```bash
docker compose -f docker/kafka/docker-compose-kafka.yml up -d
```

查看状态：

```bash
docker compose -f docker/kafka/docker-compose-kafka.yml ps
```

查看启动日志：

```bash
docker compose -f docker/kafka/docker-compose-kafka.yml logs -f kafka
```

停止容器但保留数据卷：

```bash
docker compose -f docker/kafka/docker-compose-kafka.yml down
```

数据保存在命名卷 `hmdp-kafka-data`。再次启动后，Topic、消息和 Consumer Group offset 会继续存在。

## 4. Topic 创建命令

Kafka 启动完成后依次创建主 Topic、Retry Topic 和 DLT。三个 Topic 使用相同的 12 个分区，确保失败消息可以保持原分区号转发；本地只有一个 broker，因此复制因子为 1。

### 主 Topic

```bash
docker exec hmdp-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic hmdp.seckill.order.create.v1 \
  --partitions 12 \
  --replication-factor 1
```

### Retry Topic

```bash
docker exec hmdp-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic hmdp.seckill.order.retry.v1 \
  --partitions 12 \
  --replication-factor 1
```

### Dead Letter Topic

```bash
docker exec hmdp-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic hmdp.seckill.order.dlt.v1 \
  --partitions 12 \
  --replication-factor 1
```

检查 Topic：

```bash
docker exec hmdp-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

```bash
docker exec hmdp-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic hmdp.seckill.order.create.v1
```

## 5. 本地 Spring Boot 连接配置

项目现有配置已经支持该环境，无需修改 Java 或 `application.yaml`：

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:127.0.0.1:9092}
```

直接从宿主机启动 Spring Boot 时，默认连接：

```text
127.0.0.1:9092
```

也可以显式设置环境变量：

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
mvn spring-boot:run
```

若未来 Spring Boot 也运行在同一个 Docker 网络中，应连接容器内部监听地址：

```text
hmdp-kafka:19092
```

## 6. 环境验证

仅验证 Compose 语法和变量展开：

```bash
docker compose -f docker/kafka/docker-compose-kafka.yml config
```

Kafka 启动后验证 broker：

```bash
docker exec hmdp-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

## 7. 常见问题

### 7.1 端口 9092 已被占用

症状：Compose 提示无法绑定端口。

处理：停止占用 9092 的进程或容器。若修改宿主机映射端口，还必须同步调整 `KAFKA_ADVERTISED_LISTENERS` 和 `KAFKA_BOOTSTRAP_SERVERS`，不能只改 `ports`。

### 7.2 Spring Boot 能连接 bootstrap-server，但随后持续断线

通常是 `advertised.listeners` 地址对客户端不可达。宿主机应用应收到 `localhost:9092`，Docker 网络内应用应收到 `hmdp-kafka:19092`。本 Compose 已分别配置两个 listener。

### 7.3 Topic 转发报目标分区不存在

主 Topic、Retry Topic 和 DLT 必须使用相同分区数。本项目 Consumer 会把失败消息转发到与源记录相同的分区，因此三个 Topic 均创建为 12 分区。

### 7.4 重启后出现 Cluster ID 或元数据不一致

不要在保留 `hmdp-kafka-data` 的同时修改 `CLUSTER_ID`。确实需要全新环境时，可执行以下命令删除容器和数据卷：

```bash
docker compose -f docker/kafka/docker-compose-kafka.yml down -v
```

该操作会永久删除本地 Kafka Topic、消息和 offset，执行前必须确认无需保留数据。

### 7.5 单节点环境出现副本因子错误

本地只有一个 broker，Topic 和 Kafka 内部状态 Topic 的复制因子必须为 1。生产环境不能照搬该配置，应部署多 broker/controller 并提高复制因子和最小同步副本数。

### 7.6 Docker 启动时出现目录权限错误

官方镜像要求 Docker 20.10.4 或更高版本。优先升级 Docker Desktop；本配置使用命名卷而非 Windows 主机目录绑定，也可减少路径权限和换行差异。

