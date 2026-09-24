import { describe, expect, it, vi, beforeEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import Dashboard from './Dashboard.vue'
import IncidentDetail from './IncidentDetail.vue'
import Proposal from './Proposal.vue'
import { getIncident, getMonitoringSummary, listIncidents } from '../api/client.js'

vi.mock('../api/client.js', () => ({
  getIncident: vi.fn(),
  getMonitoringSummary: vi.fn(),
  listIncidents: vi.fn(),
  isAgentOffline: error => !error?.response || error.response.status >= 500
}))

const routerLink = { template: '<a><slot /></a>' }

beforeEach(() => {
  vi.useRealTimers()
  vi.clearAllMocks()
})

describe('AIOps Console', () => {
  it('shows Healthy when there are no incidents', async () => {
    getMonitoringSummary.mockResolvedValue({ health: 'healthy', active_incident_count: 0, last_inspection_at: null })
    listIncidents.mockResolvedValue([])
    const wrapper = mount(Dashboard, { global: { stubs: { RouterLink: routerLink } } })
    await flushPromises()
    expect(wrapper.text()).toContain('系统健康')
    expect(wrapper.text()).toContain('当前没有故障事件')
    expect(wrapper.text()).toContain('尚无持续巡检记录')
  })

  it('shows Attention for an active fault even when diagnosis is complete', async () => {
    getMonitoringSummary.mockResolvedValue({ health: 'healthy', active_incident_count: 1, last_inspection_at: null })
    listIncidents.mockResolvedValue([{
      incident_id: 'inc_active', title: 'Kafka consumer down', source: 'alert',
      status: 'ACTIVE', diagnosis_status: 'COMPLETED', created_at: '2026-09-19T04:00:00Z'
    }])
    const wrapper = mount(Dashboard, { global: { stubs: { RouterLink: routerLink } } })
    await flushPromises()
    expect(wrapper.text()).toContain('需关注')
    expect(wrapper.text()).toContain('存在未恢复故障事件')
    expect(wrapper.text()).not.toContain('当前无活跃故障事件')
  })

  it('shows Healthy when the fault is recovered', async () => {
    getMonitoringSummary.mockResolvedValue({ health: 'healthy', active_incident_count: 0, last_inspection_at: null })
    listIncidents.mockResolvedValue([{
      incident_id: 'inc_recovered', title: 'Kafka consumer down', source: 'alert',
      status: 'RECOVERED', diagnosis_status: 'COMPLETED', created_at: '2026-09-19T04:00:00Z'
    }])
    const wrapper = mount(Dashboard, { global: { stubs: { RouterLink: routerLink } } })
    await flushPromises()
    expect(wrapper.text()).toContain('系统健康')
    expect(wrapper.text()).toContain('RECOVERED')
  })

  it('shows Agent Offline instead of stale Dashboard data when the API is down', async () => {
    getMonitoringSummary.mockRejectedValue(new Error('connection refused'))
    listIncidents.mockRejectedValue(new Error('connection refused'))
    const wrapper = mount(Dashboard, { global: { stubs: { RouterLink: routerLink } } })
    await flushPromises()
    expect(wrapper.text()).toContain('诊断服务离线')
    expect(wrapper.text()).not.toContain('系统健康')
    expect(wrapper.find('button').exists()).toBe(true)
  })

  it('recovers from Agent Offline after retry', async () => {
    getMonitoringSummary.mockRejectedValueOnce(new Error('connection refused'))
      .mockResolvedValueOnce({ health: 'healthy', active_incident_count: 0, last_inspection_at: null })
    listIncidents.mockRejectedValueOnce(new Error('connection refused')).mockResolvedValueOnce([])
    const wrapper = mount(Dashboard, { global: { stubs: { RouterLink: routerLink } } })
    await flushPromises()
    expect(wrapper.text()).toContain('诊断服务离线')
    await wrapper.find('.offline-panel button').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('系统健康')
    expect(wrapper.text()).not.toContain('诊断服务离线')
  })

  it('renders Kafka timeline, evidence, skill, and confidence changes', async () => {
    getIncident.mockResolvedValue({
      incident_id: 'inc_kafka_demo', title: '秒杀订单延迟', description: 'Kafka lag',
      status: 'ACTIVE', diagnosis_status: 'COMPLETED', severity: 'high', runtime_status: 'awaiting_human',
      created_at: '2026-09-19T04:00:00Z', trigger_signal_ids: ['sig_demo'],
      traces: [
        { event_id: 't1', event_type: 'skill_selected', created_at: '2026-09-19T04:00:00Z', summary: 'Selected Kafka skill', payload: { skill_id: 'kafka-consumer-diagnosis', version: '1.0' } },
        { event_id: 't2', event_type: 'skill_loaded', created_at: '2026-09-19T04:00:01Z', summary: 'Loaded Kafka skill', payload: { skill_id: 'kafka-consumer-diagnosis' } },
        { event_id: 't3', event_type: 'tool_called', created_at: '2026-09-19T04:00:02Z', summary: 'Called Kafka tool', payload: { tool_name: 'get_kafka_status' } },
        { event_id: 't4', event_type: 'evidence_created', created_at: '2026-09-19T04:00:03Z', summary: 'Kafka lag 50000', payload: { evidence_id: 'evi_kafka' } }
      ],
      evidence: [{ evidence_id: 'evi_kafka_status_001', kind: 'kafka_consumer_status', source: 'hmdp.kafka', source_tool: 'get_kafka_status', status: 'success', summary: 'No active consumers', facts: ['member_count: 0', 'total_lag: 50000'], collected_at: '2026-09-19T04:00:03Z' }],
      hypotheses: [{ hypothesis_id: 'hyp_kafka_001', description: 'Kafka consumer unavailable', status: 'SUPPORTED', confidence: 0.96, supporting_evidence_refs: ['evi_kafka'], contradicting_evidence_refs: [], history: [{ status: 'UNKNOWN', confidence: 0 }, { status: 'SUPPORTED', confidence: 0.96 }] }],
      report: { status: 'confirmed', root_cause: 'Kafka consumer unavailable', conclusion: 'Consumer group has no members', confidence: 0.96 },
      proposals: []
    })
    const wrapper = mount(IncidentDetail, { props: { id: 'inc_kafka_demo' }, global: { stubs: { RouterLink: routerLink } } })
    await flushPromises()
    expect(wrapper.text()).toContain('kafka-consumer-diagnosis')
    expect(wrapper.text()).toContain('故障状态：进行中 ACTIVE')
    expect(wrapper.text()).toContain('诊断状态：诊断完成 COMPLETED')
    expect(wrapper.text()).toContain('get_kafka_status')
    expect(wrapper.text()).toContain('消费者成员数（member_count）：0')
    expect(wrapper.text()).toContain('UNKNOWN 0%')
    expect(wrapper.text()).toContain('SUPPORTED 96%')
    await wrapper.find('button[aria-pressed="false"]').trigger('click')
    expect(wrapper.text()).toContain('智能体运行回放')
    expect(wrapper.text()).toContain('0 / 4 个事件')
  })

  it('renders the recorded MySQL path with Kafka contradiction and final support', async () => {
    getIncident.mockResolvedValue({
      incident_id: 'inc_mysql_real', title: 'Consumer MySQL persistence failure',
      description: '真实 Recorded Observation 回放', demo_scenario: 'MySQL Persistence Failure · Real',
      demo_fault_id: 'MYSQL_PERSISTENCE_TIMEOUT_001', replay_only: true,
      status: 'ACTIVE', diagnosis_status: 'COMPLETED', severity: 'high', runtime_status: 'awaiting_human',
      created_at: '2026-09-23T08:10:34Z', trigger_signal_ids: ['sig_mysql'],
      traces: [
        { event_id: 't1', event_type: 'skill_selected', created_at: '2026-09-23T08:10:34Z', summary: 'Selected skill', payload: { skill_id: 'kafka-consumer-diagnosis', version: '1.0' } },
        { event_id: 't2', event_type: 'tool_called', created_at: '2026-09-23T08:10:35Z', summary: 'Business', payload: { tool_name: 'get_business_metrics' } },
        { event_id: 't3', event_type: 'tool_called', created_at: '2026-09-23T08:10:36Z', summary: 'Kafka', payload: { tool_name: 'get_kafka_status' } },
        { event_id: 't4', event_type: 'tool_called', created_at: '2026-09-23T08:10:37Z', summary: 'MySQL', payload: { tool_name: 'get_mysql_health' } }
      ],
      evidence: [
        { evidence_id: 'e1', kind: 'business_metrics', source_tool: 'get_business_metrics', status: 'success', summary: 'orders created=0', facts: ['kafka_message_sent_count: 2', 'order_created_success_count: 0'], collected_at: '2026-09-23T08:10:35Z' },
        { evidence_id: 'e2', kind: 'kafka_consumer_status', source_tool: 'get_kafka_status', status: 'success', summary: 'Kafka healthy', facts: ['member_count: 1', 'total_lag: 0'], collected_at: '2026-09-23T08:10:36Z' },
        { evidence_id: 'e3', kind: 'mysql_health', source_tool: 'get_mysql_health', status: 'partial', summary: 'CONNECTION_TIMEOUT', facts: ['health_state: DEGRADED'], collected_at: '2026-09-23T08:10:37Z' }
      ],
      hypotheses: [
        { hypothesis_id: 'h1', description: 'Kafka Consumer Failure', status: 'CONTRADICTED', confidence: 0.1, history: [{ status: 'UNKNOWN', confidence: 0 }, { status: 'CONTRADICTED', confidence: 0.1 }] },
        { hypothesis_id: 'h2', description: 'MySQL Persistence Failure', status: 'SUPPORTED', confidence: 0.93, history: [{ status: 'UNKNOWN', confidence: 0 }, { status: 'SUPPORTED', confidence: 0.93 }] }
      ],
      report: { status: 'confirmed', root_cause: 'MySQL Persistence Failure', conclusion: 'Kafka healthy; MySQL timed out', confidence: 0.93 },
      proposals: []
    })

    const wrapper = mount(IncidentDetail, { props: { id: 'inc_mysql_real' }, global: { stubs: { RouterLink: routerLink } } })
    await flushPromises()

    expect(wrapper.text()).toContain('只读记录 RECORDED')
    expect(wrapper.text()).toContain('get_business_metrics')
    expect(wrapper.text()).toContain('get_mysql_health')
    expect(wrapper.text()).toContain('CONTRADICTED 10%')
    expect(wrapper.text()).toContain('SUPPORTED 93%')
    expect(wrapper.text()).toContain('MySQL 持久化故障')
  })

  it('shows Agent Offline on Incident Detail when the API is unreachable', async () => {
    getIncident.mockRejectedValue(new Error('connection refused'))
    const wrapper = mount(IncidentDetail, { props: { id: 'inc_kafka_demo' }, global: { stubs: { RouterLink: routerLink } } })
    await flushPromises()
    expect(wrapper.text()).toContain('诊断服务离线')
    expect(wrapper.text()).not.toContain('根因尚未确认')
  })

  it('distinguishes a missing Incident from an offline Agent', async () => {
    getIncident.mockRejectedValue({ response: { status: 404 } })
    const wrapper = mount(IncidentDetail, { props: { id: 'inc_missing' }, global: { stubs: { RouterLink: routerLink } } })
    await flushPromises()
    expect(wrapper.text()).toContain('此故障事件不存在')
    expect(wrapper.text()).not.toContain('诊断服务离线')
  })

  it('displays a proposal without an execution control', async () => {
    getIncident.mockResolvedValue({
      proposals: [{ proposal_id: 'proposal_demo', action_name: 'restart_consumer', reason: 'Kafka consumer unavailable', evidence_refs: ['evi_kafka_status_001'], risk_level: 'low', approval_required: true, permission_result: 'REQUIRE_APPROVAL' }]
    })
    const wrapper = mount(Proposal, { props: { id: 'inc_kafka_demo' }, global: { stubs: { RouterLink: routerLink } } })
    await flushPromises()
    expect(wrapper.text()).toContain('restart_consumer')
    expect(wrapper.text()).toContain('REQUIRE_APPROVAL')
    expect(wrapper.find('button').exists()).toBe(false)
  })

  it('shows Agent Offline on Proposal when the API is unreachable', async () => {
    getIncident.mockRejectedValue(new Error('connection refused'))
    const wrapper = mount(Proposal, { props: { id: 'inc_kafka_demo' }, global: { stubs: { RouterLink: routerLink } } })
    await flushPromises()
    expect(wrapper.text()).toContain('诊断服务离线')
  })

  it('refreshes Dashboard every five seconds and stops after leaving', async () => {
    vi.useFakeTimers()
    getMonitoringSummary.mockResolvedValue({ health: 'healthy', active_incident_count: 0, last_inspection_at: null })
    listIncidents.mockResolvedValue([])
    const wrapper = mount(Dashboard, { global: { stubs: { RouterLink: routerLink } } })
    await flushPromises()
    expect(getMonitoringSummary).toHaveBeenCalledTimes(1)
    expect(wrapper.text()).toContain('实时监控中 · 每 5 秒刷新')

    await vi.advanceTimersByTimeAsync(5000)
    await flushPromises()
    expect(getMonitoringSummary).toHaveBeenCalledTimes(2)
    wrapper.unmount()

    await vi.advanceTimersByTimeAsync(5000)
    expect(getMonitoringSummary).toHaveBeenCalledTimes(2)
  })

  it('refreshes Incident detail every three seconds without overlapping requests', async () => {
    vi.useFakeTimers()
    let resolveIncident
    getIncident.mockReturnValue(new Promise(resolve => { resolveIncident = resolve }))
    const wrapper = mount(IncidentDetail, { props: { id: 'inc_live' }, global: { stubs: { RouterLink: routerLink } } })
    await vi.advanceTimersByTimeAsync(9000)
    expect(getIncident).toHaveBeenCalledTimes(1)

    resolveIncident({
      incident_id: 'inc_live', title: '实时诊断', status: 'ACTIVE', diagnosis_status: 'RUNNING',
      severity: 'medium', runtime_status: 'investigating', created_at: '2026-09-23T08:00:00Z',
      trigger_signal_ids: [], traces: [], evidence: [], hypotheses: [], proposals: [], report: null
    })
    await flushPromises()
    await vi.advanceTimersByTimeAsync(3000)
    await flushPromises()
    expect(getIncident).toHaveBeenCalledTimes(2)
    expect(wrapper.text()).toContain('实时监控中 · 每 3 秒刷新')
    wrapper.unmount()

    await vi.advanceTimersByTimeAsync(3000)
    expect(getIncident).toHaveBeenCalledTimes(2)
  })
})
