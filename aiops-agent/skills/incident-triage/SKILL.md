+++
skill_id = "incident-triage"
name = "Incident Triage"
description = "通用只读故障分诊流程，用于确认影响、收集证据、建立并验证假设。"
category = "general"
trigger_conditions = ["故障", "异常", "失败", "超时", "服务不可用", "incident"]
version = "1.0"
+++

# Incident Triage

本 Skill 只向 Planner 提供调查顺序建议，不是 Evidence，也不能授权任何操作。

1. 明确故障发生时间、受影响组件和用户影响范围。
2. 从已注册的只读工具中选择最小必要工具，收集基础证据。
3. 基于 Evidence 建立至少一个可验证假设，不把告警描述当成已确认事实。
4. 优先寻找能够证伪当前假设的观测；证据不足时明确标记为不确定。
5. 输出可追溯结论，引用 Evidence ID，并将后续操作留给人工决定。

禁止建议 Planner 调用未注册工具，禁止把本 Skill 的文字作为根因证据。
