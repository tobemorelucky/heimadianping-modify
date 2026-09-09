# Elasticsearch 本地环境部署说明

## 1. 环境目标

本地环境为后续商户搜索索引、初始化同步和查询联调提供：

- Elasticsearch 单节点服务；
- Kibana 管理和 Dev Tools 界面；
- 独立持久化数据卷；
- 独立 Docker 网络；
- 与已有 Kafka、MySQL、Redis 隔离的容器名称和端口。

Compose 文件：`docker/elasticsearch/docker-compose-es.yml`。

## 2. 版本选择

本项目固定使用：

```text
Elasticsearch 8.19.21
Kibana 8.19.21
```

选择原因：

1. 用户要求使用 Elasticsearch 8.x；`8.19.21` 是 2026-09-08 Elastic 官方镜像仓库中可用的最新 8.x 补丁版本。
2. 固定完整版本号可以避免 `latest` 标签漂移导致不同开发机行为不一致。
3. Elasticsearch 与 Kibana 必须保持相同版本，减少协议和 Saved Objects 兼容风险。
4. 使用 Elastic 官方镜像仓库，不使用来源不明的二次封装镜像。
5. 8.x 能满足商户全文检索、`geo_point`、相关性排序和后续 Java 客户端接入需要，同时避免提前跨到 9.x 主版本。

官方参考：

- [Elastic Docker 镜像仓库](https://www.docker.elastic.co/r/elasticsearch)
- [Kibana 8.19 Docker 安装说明](https://www.elastic.co/guide/en/kibana/8.19/docker.html)

## 3. Compose 配置说明

### 3.1 Elasticsearch

| 配置 | 值 | 说明 |
| --- | --- | --- |
| 容器名 | `hmdp-elasticsearch` | 与 Kafka、MySQL、Redis 容器不重名。 |
| 集群名 | `hmdp-es-local` | 标识本地开发集群。 |
| 运行模式 | `discovery.type=single-node` | 本地单节点，不进行生产集群发现。 |
| 宿主机端口 | `127.0.0.1:9200` | REST API，仅绑定本机。 |
| JVM 堆 | `512m / 512m` | 控制本地资源占用，避免过度挤压 Kafka、MySQL 和 Redis。 |
| 数据卷 | `hmdp-elasticsearch-data` | 持久化索引和集群元数据。 |
| 安全功能 | 本地关闭 | 简化开发联调；禁止原样用于生产。 |

### 3.2 Kibana

| 配置 | 值 | 说明 |
| --- | --- | --- |
| 容器名 | `hmdp-kibana` | 独立的 Kibana 服务。 |
| 宿主机端口 | `127.0.0.1:5601` | Kibana Web UI，仅绑定本机。 |
| ES 地址 | `http://hmdp-elasticsearch:9200` | 通过独立 Docker 网络访问 ES，不使用宿主机回环地址。 |
| 数据卷 | `hmdp-kibana-data` | 持久化 Kibana 本地数据和 Saved Objects 相关状态。 |
| 启动依赖 | ES healthcheck 通过 | 避免 Kibana 在 ES 尚未就绪时反复初始化。 |

### 3.3 网络隔离

两个服务只加入命名网络 `hmdp-es-network`。该网络没有复用 Kafka Compose 的网络，ES Compose 也不声明 Kafka、MySQL 或 Redis 服务，因此启动和停止 ES 环境不会操作已有容器。

端口分配：

```text
Elasticsearch REST API: http://localhost:9200
Kibana Web UI:          http://localhost:5601
Kafka:                  localhost:9092（不变）
MySQL:                  localhost:3306（不变）
Redis:                  localhost:6379（不变）
```

## 4. Docker 启动与停止

所有命令在项目根目录执行。

### 4.1 解析配置

```powershell
docker compose -f docker/elasticsearch/docker-compose-es.yml config
```

只验证配置是否合法，不启动容器。

### 4.2 启动服务

```powershell
docker compose -f docker/elasticsearch/docker-compose-es.yml up -d
```

首次启动需要下载 Elasticsearch 和 Kibana 镜像，耗时取决于网络速度。

### 4.3 查看状态

```powershell
docker compose -f docker/elasticsearch/docker-compose-es.yml ps
```

### 4.4 查看日志

```powershell
docker compose -f docker/elasticsearch/docker-compose-es.yml logs -f elasticsearch
```

```powershell
docker compose -f docker/elasticsearch/docker-compose-es.yml logs -f kibana
```

### 4.5 停止并保留数据

```powershell
docker compose -f docker/elasticsearch/docker-compose-es.yml down
```

命名卷默认保留。不要随意添加 `-v`；`down -v` 会删除 ES 索引和 Kibana 本地数据。

## 5. Kibana 访问方式

容器健康后在浏览器访问：

```text
http://localhost:5601
```

本地 Compose 已关闭 Elastic Security，因此不需要输入 `elastic` 用户密码或 enrollment token。进入 Kibana 后可打开：

```text
Management -> Dev Tools -> Console
```

后续可以在 Console 中执行索引创建、Mapping、文档查询和 `_cluster/health` 请求。

注意：关闭认证只适用于受信任的本机开发环境。生产环境必须启用认证、TLS、权限控制和安全凭据管理。

## 6. 健康检查

Compose 内置 ES healthcheck：

```text
GET /_cluster/health?wait_for_status=yellow&timeout=5s
```

单节点环境允许 `yellow`：它通常表示 primary shard 可用但副本无法分配到同一个节点。项目测试索引可设置 `number_of_replicas=0` 以获得 `green`。

### 6.1 PowerShell 检查

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:9200/"
```

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:9200/_cluster/health?pretty"
```

期望结果：

- 根接口返回节点名 `hmdp-elasticsearch` 和 8.19.21 版本信息；
- `_cluster/health` 返回 `cluster_name=hmdp-es-local`；
- `status` 为 `green` 或 `yellow`，不能为 `red`；
- `number_of_nodes=1`。

### 6.2 curl 检查

```powershell
curl.exe -fsS "http://localhost:9200/_cluster/health?pretty"
```

### 6.3 Kibana 状态

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:5601/api/status"
```

也可以直接访问 `http://localhost:5601`，页面正常加载即表示 Web UI 可达。

## 7. ES 基础验证脚本说明

以下命令只用于环境冒烟测试，测试索引名为 `hmdp_es_smoke_test`，不会创建正式 `shop_index`。

### 7.1 查看 Cluster Health

PowerShell：

```powershell
$clusterHealth = Invoke-RestMethod -Method Get -Uri "http://localhost:9200/_cluster/health"
$clusterHealth | Format-List cluster_name,status,number_of_nodes,active_primary_shards
```

Kibana Dev Tools：

```http
GET /_cluster/health
```

### 7.2 创建测试 Index

PowerShell：

```powershell
$testIndexBody = @{
    settings = @{
        number_of_shards = 1
        number_of_replicas = 0
    }
    mappings = @{
        properties = @{
            name = @{ type = "keyword" }
        }
    }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
    -Method Put `
    -Uri "http://localhost:9200/hmdp_es_smoke_test" `
    -ContentType "application/json" `
    -Body $testIndexBody
```

Kibana Dev Tools：

```http
PUT /hmdp_es_smoke_test
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "name": {
        "type": "keyword"
      }
    }
  }
}
```

期望返回 `acknowledged: true`。

### 7.3 查询 Index

查看索引：

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:9200/hmdp_es_smoke_test?pretty"
```

查看索引列表：

```powershell
curl.exe -fsS "http://localhost:9200/_cat/indices/hmdp_es_smoke_test?v"
```

Kibana Dev Tools：

```http
GET /hmdp_es_smoke_test
GET /_cat/indices/hmdp_es_smoke_test?v
```

### 7.4 可选清理测试 Index

确认不再需要测试数据后执行：

```powershell
Invoke-RestMethod -Method Delete -Uri "http://localhost:9200/hmdp_es_smoke_test"
```

该命令只删除明确命名的冒烟测试索引，不要对通配符或正式 `shop_index` 执行删除。

## 8. 常见问题

### 8.1 容器启动后立即退出

先查看 ES 日志。常见原因包括 Docker Desktop 内存不足、数据卷权限异常或宿主机虚拟化环境未启动。建议为 Docker Desktop 保留至少 4 GB 总内存；本 Compose 中 ES JVM 堆限制为 512 MB，但 Kibana、容器本身和已有服务仍需要额外内存。

### 8.2 9200 端口被占用

检查是否已有 ES 或其他服务监听 9200。不要为了启动本环境而停止 Kafka/MySQL/Redis；应关闭重复 ES 实例，或仅调整本 Compose 的宿主机端口并同步修改访问文档。

### 8.3 Kibana 一直显示未就绪

先确认 `hmdp-elasticsearch` healthcheck 已通过，再检查 Kibana 日志以及 `ELASTICSEARCH_HOSTS` 是否仍指向 `http://hmdp-elasticsearch:9200`。容器内不能使用 `localhost:9200` 访问另一个容器。

### 8.4 Cluster Health 为 yellow

单节点无法把副本分片分配到另一个节点，`yellow` 不代表 primary shard 不可用。开发测试索引设置 `number_of_replicas=0` 即可。`red` 才表示存在不可用 primary shard，需要排查。

### 8.5 中文 IK analyzer 不存在

当前环境部署阶段使用官方原始镜像，没有安装 IK 插件。创建包含 `ik_max_word` 或 `ik_smart` 的正式 Mapping 前，需要在后续索引阶段提供与 ES 8.19.21 完全匹配的插件镜像或改用已确认可用的 analyzer。不能直接用不匹配版本的插件。

## 9. 本阶段验证结果

已执行：

```powershell
docker compose -f docker/elasticsearch/docker-compose-es.yml config
```

结果：退出码为 `0`，Compose 成功解析以下内容：

- Elasticsearch 与 Kibana 均固定为 8.19.21；
- 容器名称分别为 `hmdp-elasticsearch`、`hmdp-kibana`；
- 9200 和 5601 仅绑定 `127.0.0.1`；
- 数据卷为 `hmdp-elasticsearch-data`、`hmdp-kibana-data`；
- 独立网络为 `hmdp-es-network`；
- Kibana 等待 Elasticsearch 健康后启动。

命令输出了无法读取用户级 Docker `config.json` 的权限警告，但不影响 Compose 文件解析结果。本阶段按要求没有执行 `docker compose up`、没有下载镜像，也没有运行 ES API 冒烟测试。

