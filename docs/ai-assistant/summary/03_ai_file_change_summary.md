# AI 运营助手文件变更总结

## 一、Java 新增文件

### Controller

| 文件 | 作用 |
| --- | --- |
| `src/main/java/com/hmdp/controller/AiShopController.java` | 提供店铺基础信息只读接口 `/api/ai/shop/{id}`。 |
| `src/main/java/com/hmdp/controller/AiOrderStatisticsController.java` | 提供最近 7 天订单统计接口 `/api/ai/shop/{id}/orders`。 |
| `src/main/java/com/hmdp/controller/AiCouponStatisticsController.java` | 提供累计优惠券统计接口 `/api/ai/shop/{id}/coupons`。 |

### DTO

| 文件 | 作用 |
| --- | --- |
| `src/main/java/com/hmdp/dto/AiShopInfoDTO.java` | 限定 AI 可读取的店铺基础字段。 |
| `src/main/java/com/hmdp/dto/AiOrderStatisticsDTO.java` | 承载订单数量和交易金额。 |
| `src/main/java/com/hmdp/dto/AiCouponStatisticsDTO.java` | 承载优惠券发放量、使用量和使用率。 |

### Service

| 文件 | 作用 |
| --- | --- |
| `src/main/java/com/hmdp/service/AiBusinessStatisticsService.java` | 定义 AI 经营统计只读服务。 |
| `src/main/java/com/hmdp/service/impl/AiBusinessStatisticsServiceImpl.java` | 校验店铺、确定统计口径并计算优惠券使用率。 |

## 二、Java 修改文件

| 文件 | 作用 |
| --- | --- |
| `src/main/java/com/hmdp/config/MvcConfig.java` | 将 AI 专用查询路径纳入无需用户登录的接口范围。 |
| `src/main/java/com/hmdp/mapper/VoucherOrderMapper.java` | 增加最近 7 天订单和累计优惠券数据的只读聚合 SQL。 |

未修改已有 `ShopController`，也未修改 Kafka、Elasticsearch、Canal、Redis、Lua 和秒杀业务代码。

## 三、Python 新增文件

### FastAPI 与配置

| 文件 | 作用 |
| --- | --- |
| `ai-assistant/app/__init__.py` | 声明 Python 应用包。 |
| `ai-assistant/app/main.py` | 创建 FastAPI 应用并提供 `POST /chat`。 |
| `ai-assistant/app/config.py` | 从环境变量和本地 `.env` 读取 LLM、Spring Boot 地址配置。 |

### LangGraph

| 文件 | 作用 |
| --- | --- |
| `ai-assistant/app/agent/__init__.py` | 声明 Agent 包。 |
| `ai-assistant/app/agent/state.py` | 定义请求级 `AgentState` 和消息累加规则。 |
| `ai-assistant/app/agent/graph.py` | 构建 `Agent → ToolNode → Agent` 状态图，绑定三个只读工具和系统指令。 |

### Tool

| 文件 | 作用 |
| --- | --- |
| `ai-assistant/app/tools/__init__.py` | 汇总并导出允许注册的业务工具。 |
| `ai-assistant/app/tools/shop_tools.py` | 实现 `get_shop_info`，通过 HTTP 获取店铺基础信息。 |
| `ai-assistant/app/tools/order_tools.py` | 实现 `get_order_statistics`，通过 HTTP 获取最近 7 天订单指标。 |
| `ai-assistant/app/tools/coupon_tools.py` | 实现 `get_coupon_statistics`，通过 HTTP 获取累计优惠券指标。 |

## 四、配置与依赖文件

| 文件 | 作用 |
| --- | --- |
| `ai-assistant/.env.example` | 声明 `LLM_BASE_URL`、`LLM_API_KEY`、`LLM_MODEL`、`SPRING_BOOT_BASE_URL` 配置模板，不包含真实密钥。 |
| `ai-assistant/.gitignore` | 忽略 `.env`、虚拟环境、缓存和本地产物。 |
| `ai-assistant/requirements.txt` | 声明 FastAPI、Uvicorn、LangGraph、LangChain、OpenAI-compatible Client、HTTPX 和 dotenv 依赖。 |
| `ai-assistant/README.md` | 记录 Python 环境安装、配置、启动和调用方式。 |

本地 `ai-assistant/.env` 只用于运行验证，已被 Git 忽略，不属于应提交文件。

## 五、测试文件

### Java

| 文件 | 作用 |
| --- | --- |
| `src/test/java/com/hmdp/controller/AiShopControllerTest.java` | 验证店铺 AI 接口及字段限制。 |
| `src/test/java/com/hmdp/controller/AiOrderStatisticsControllerTest.java` | 验证订单统计接口的成功和店铺不存在场景。 |
| `src/test/java/com/hmdp/controller/AiCouponStatisticsControllerTest.java` | 验证优惠券统计接口及使用率字段。 |
| `src/test/java/com/hmdp/service/impl/AiBusinessStatisticsServiceImplTest.java` | 验证统计查询边界、店铺校验和使用率计算。 |

### Python

| 文件 | 作用 |
| --- | --- |
| `ai-assistant/tests/__init__.py` | 声明测试包。 |
| `ai-assistant/tests/test_shop_tool.py` | Mock Spring Boot，验证店铺 Tool 的 HTTP 路径和字段白名单。 |
| `ai-assistant/tests/test_order_tool.py` | 验证订单 Tool 的调用与结果转换。 |
| `ai-assistant/tests/test_coupon_tool.py` | 验证优惠券 Tool 的调用与结果转换。 |
| `ai-assistant/tests/test_agent_graph.py` | 验证 Agent 在一轮内发起多个 Tool Call 并完成最终回答。 |

## 六、文档文件

| 文件 | 作用 |
| --- | --- |
| `docs/ai-assistant/01_ai_architecture_design.md` | 初始业务场景、Agent 架构和安全边界设计。 |
| `docs/ai-assistant/02_ai_interview_summary.md` | 初始 Agent、RAG、LangGraph 面试问题整理。 |
| `docs/ai-assistant/03_ai_feature_scope.md` | 明确实现范围与不实现范围。 |
| `docs/ai-assistant/04_ai_environment_setup.md` | Python 环境、依赖和 `.env` 配置说明。 |
| `docs/ai-assistant/05_ai_phase1_implementation.md` | FastAPI、LangGraph、LLM 基础链路记录。 |
| `docs/ai-assistant/06_ai_tool_calling_design.md` | Java/Python Tool Calling 架构设计。 |
| `docs/ai-assistant/07_ai_phase2_implementation.md` | 首个店铺信息 Tool 的实现记录。 |
| `docs/ai-assistant/08_ai_phase2_real_llm_test.md` | 首轮真实 LLM 联调及当时的外部配置阻塞记录。 |
| `docs/ai-assistant/09_ai_phase2_real_llm_test.md` | 修正本地配置后的真实 DeepSeek 店铺 Tool 联调结果。 |
| `docs/ai-assistant/10_ai_phase3_business_tools_design.md` | 订单和优惠券统计 Tool 的架构、口径与安全设计。 |
| `docs/ai-assistant/11_ai_phase3_business_tools_test.md` | 自动化测试、真实接口和 DeepSeek 多 Tool 联调结果。 |
| `docs/ai-assistant/summary/01_ai_final_architecture.md` | AI 模块最终架构归档。 |
| `docs/ai-assistant/summary/02_ai_interview_summary.md` | 秋招面试问答归档。 |
| `docs/ai-assistant/summary/03_ai_file_change_summary.md` | AI 模块全部文件与职责归档。 |
| `docs/development-log.md` | 按阶段记录设计、实现、测试和归档状态。 |

## 七、测试基线

- Java AI 专项测试：通过。
- Python Tool 与 Agent 测试：4 个通过。
- 完整 Maven：52 个测试，0 失败，0 错误，3 个外部环境集成测试跳过。
- 真实 DeepSeek：店铺信息 Tool Calling 和订单/优惠券多 Tool Calling 均已通过。
- API Key 未进入 Git 跟踪文件或 Markdown。

## 八、删除文件

AI Assistant 各阶段没有删除业务文件。
