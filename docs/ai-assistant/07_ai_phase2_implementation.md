# AI Assistant Phase 2 实现记录

## 1. 实现结果

Phase 2 已实现第一个真实业务 Tool `get_shop_info`，并将 LangGraph 从单 LLM 节点升级为 Agent–Tool 循环。Python 只通过 HTTP 调用 Spring Boot，Spring Boot 再通过既有 Service 查询业务数据。

## 2. 新增文件

### Java

| 文件 | 作用 |
|---|---|
| `src/main/java/com/hmdp/controller/AiShopController.java` | 提供 `GET /api/ai/shop/{id}` 只读接口 |
| `src/main/java/com/hmdp/dto/AiShopInfoDTO.java` | 限定 AI 可见的四个店铺字段 |
| `src/test/java/com/hmdp/controller/AiShopControllerTest.java` | 验证接口协议、字段白名单和不存在场景 |

### Python

| 文件 | 作用 |
|---|---|
| `ai-assistant/app/tools/shop_tools.py` | 实现 Spring Boot HTTP Tool `get_shop_info` |
| `ai-assistant/tests/__init__.py` | 测试包声明 |
| `ai-assistant/tests/test_shop_tool.py` | Mock Spring Boot 的 Tool 单元测试 |
| `ai-assistant/tests/test_agent_graph.py` | Agent → ToolNode → Agent 循环测试 |

## 3. 修改文件

| 文件 | 修改内容 |
|---|---|
| `src/main/java/com/hmdp/config/MvcConfig.java` | 仅将公开基础信息路径 `/api/ai/shop/**` 排除登录拦截 |
| `ai-assistant/app/config.py` | 新增 `SPRING_BOOT_BASE_URL` 和按需校验 |
| `ai-assistant/app/agent/state.py` | 增加本轮 `messages` reducer，不做持久化 Memory |
| `ai-assistant/app/agent/graph.py` | 引入 `ToolNode`、`tools_condition` 和 Agent 循环 |
| `ai-assistant/app/tools/__init__.py` | 导出白名单 Tool |
| `ai-assistant/app/main.py` | 初始化本次请求的空消息列表 |
| `ai-assistant/.env.example` | 增加本地 Spring Boot 示例地址 |
| `ai-assistant/requirements.txt` | 显式声明 `httpx` 依赖 |
| `ai-assistant/README.md` | 更新 Phase 2 流程、配置与测试命令 |
| `docs/development-log.md` | 记录实现和验证结果 |

没有修改 `ShopController`、Kafka、Elasticsearch、Canal、Redis Lua 或秒杀代码。

## 4. Java 接口实现

`AiShopController` 使用构造器注入：

- `IShopService.getById(id)` 查询店铺。
- `IShopTypeService.getById(typeId)` 查询可读类型名。
- 转换为 `AiShopInfoDTO` 后使用统一 `Result.ok` 返回。

DTO 只包含 `id`、`name`、`type`、`address`。店铺不存在返回 `Result.fail("店铺不存在")`。

真实启动时首次访问返回 401，定位到原 `LoginInterceptor` 默认保护所有非排除路径。由于这些字段本来已通过公开 `/shop/{id}` 提供，最终只在 `MvcConfig` 排除 `/api/ai/shop/**`。修改后真实请求返回 200；未排除其他 `/api/ai/**` 路径。

## 5. Python Tool 实现

`get_shop_info(shop_id)`：

- 校验正整数 ID。
- 从环境读取 `SPRING_BOOT_BASE_URL`。
- 使用固定路径调用 Spring Boot，超时为 5 秒。
- 检查 HTTP 和 `Result.success`。
- 返回前再次过滤四个字段。
- 将网络、业务和格式异常转换为 ToolException。

Python 项目没有新增数据库或中间件驱动。

## 6. LangGraph 改造

```text
START
  ↓
Agent Node
  ↓ tool_calls
ToolNode(get_shop_info)
  ↓ ToolMessage
Agent Node
  ↓ 普通回答
END
```

模型通过 `bind_tools` 只获得 `get_shop_info`。`tools_condition` 根据 AIMessage 是否含 Tool Call 决定路由；ToolNode 执行成功后回到 Agent。没有 Checkpointer，消息仅存在于单次请求状态。

## 7. 测试结果

### 7.1 Python

执行：

```powershell
cd ai-assistant
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

结果：2 个测试，全部通过。

- Tool 测试确认请求 `http://spring-boot.test/api/ai/shop/1`，并过滤额外 `images` 字段。
- Graph 测试确认 Agent 调用 2 次、ToolNode 实际执行、最终状态含 ToolMessage 和自然语言回答。

### 7.2 Java 专项测试

```powershell
mvn -Dtest=AiShopControllerTest test
```

结果：2 个测试，0 失败，0 错误，`BUILD SUCCESS`。

### 7.3 Java 完整回归

首次直接运行 `mvn test` 时，本机 Redis 未启动，原有 `HmDianPingApplicationTests` 和 `RedissonTest` 共出现 5 个上下文错误。启动一次性 Redis 并使用既有方式禁用 Canal Client 后重跑：

```powershell
mvn "-Dcanal.enabled=false" test
```

结果：45 个测试，0 失败，0 错误，3 个环境测试跳过，`BUILD SUCCESS`。一次性 Redis 在测试后已停止并自动删除。

### 7.4 真实 Java–Python Tool 联调

环境：真实 Spring Boot、MySQL 和一次性 Redis；Canal Client 禁用，Kafka 不影响店铺查询。

验证结果：

```text
GET http://127.0.0.1:8081/api/ai/shop/1
success=true
id=1
name=103茶餐厅
```

Python Tool 实际返回：

```json
{
  "id": 1,
  "name": "103茶餐厅",
  "type": "美食",
  "address": "金华路锦昌文华苑29号"
}
```

再使用确定性假模型发出真实 `get_shop_info(shop_id=1)` Tool Call，LangGraph 运行两次 Agent Node，中间 ToolNode 实际访问 Spring Boot，最终 answer 包含上述真实 Tool Result。该验证覆盖：

```text
LangGraph Agent
  → ToolNode
  → Python HTTP Tool
  → Spring Boot Controller
  → 既有 Service / MySQL
  → ToolMessage
  → Agent
```

验证结束后 Spring Boot 和一次性 Redis 均已停止。

### 7.5 真实 DeepSeek 联调状态

当前进程和仓库没有 `LLM_BASE_URL`、`LLM_API_KEY`、`LLM_MODEL` 或真实 `.env`，因此无法验证 DeepSeek 是否能根据“查询店铺1的信息”自主产生 Tool Call 并生成最终中文回答。

这不是代码或 Java/Python HTTP 链路故障。配置有效凭据后，启动两个服务并调用原 `/chat` 即可完成最后一步：

```json
{
  "message": "查询店铺1的信息"
}
```

## 8. 当前边界

- 只有一个只读 Tool，不实现订单、优惠券和热门分析。
- 没有 Agent Memory、RAG 或向量库。
- 公开 AI 店铺接口只适合非敏感基础信息。
- 没有服务间认证；增加私有经营数据前必须补齐认证授权。
- 模型真实 Tool Calling 能力仍需有效 DeepSeek 配置验证。
