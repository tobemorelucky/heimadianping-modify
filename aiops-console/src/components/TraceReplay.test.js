import { describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import TraceReplay from './TraceReplay.vue'

const stamp = '2026-09-19T04:00:00Z'
const incident = {
  traces: [
    { event_id: 't1', event_type: 'skill_selected', created_at: stamp, summary: 'Selected Kafka skill', payload: { skill_id: 'kafka-consumer-diagnosis', version: '1.0' } },
    { event_id: 't2', event_type: 'skill_loaded', created_at: stamp, summary: 'Loaded Kafka skill', payload: { skill_id: 'kafka-consumer-diagnosis' } },
    { event_id: 't3', event_type: 'tool_called', created_at: stamp, summary: 'Called Kafka status', payload: { tool_name: 'get_kafka_status' } },
    { event_id: 't4', event_type: 'evidence_created', created_at: stamp, summary: 'Evidence created', payload: { evidence_id: 'evi_kafka_001' } },
    { event_id: 't5', event_type: 'hypothesis_created', created_at: stamp, summary: 'Kafka consumer unavailable', payload: { hypothesis_id: 'hyp_001', status: 'UNKNOWN', confidence: 0 } },
    { event_id: 't6', event_type: 'hypothesis_updated', created_at: stamp, summary: 'Kafka has no active members', payload: { hypothesis_id: 'hyp_001', hypothesis: 'Kafka consumer unavailable', status: 'SUPPORTED', confidence: 0.96 } },
    { event_id: 't7', event_type: 'reflection_completed', created_at: stamp, summary: 'Reflection selected report', payload: { decision: 'report' } },
    { event_id: 't8', event_type: 'report_generated', created_at: stamp, summary: 'Diagnosis finished', payload: {} }
  ],
  evidence: [{ evidence_id: 'evi_kafka_001', kind: 'kafka_consumer_status', source: 'hmdp.kafka', source_tool: 'get_kafka_status', status: 'success', summary: 'No active consumers', facts: ['member_count: 0'], collected_at: stamp }],
  report: { root_cause: 'Kafka consumer unavailable', conclusion: 'No active members and lag' }
}

describe('TraceReplay', () => {
  it('reveals only facts available up to the selected Trace event', async () => {
    const wrapper = mount(TraceReplay, { props: { incident } })
    expect(wrapper.text()).toContain('0 / 8 个事件')
    expect(wrapper.text()).not.toContain('No active consumers')
    expect(wrapper.text()).toContain('诊断报告尚未生成')

    for (let index = 0; index < 4; index += 1) {
      await wrapper.find('[aria-label="下一步"]').trigger('click')
    }
    expect(wrapper.text()).toContain('kafka-consumer-diagnosis')
    expect(wrapper.text()).toContain('get_kafka_status')
    expect(wrapper.text()).toContain('消费者组没有活跃成员')
    expect(wrapper.text()).not.toContain('SUPPORTED · 96%')

    for (let index = 0; index < 2; index += 1) {
      await wrapper.find('[aria-label="下一步"]').trigger('click')
    }
    expect(wrapper.text()).toContain('SUPPORTED · 96%')
    expect(wrapper.text()).toContain('诊断报告尚未生成')

    for (let index = 0; index < 2; index += 1) {
      await wrapper.find('[aria-label="下一步"]').trigger('click')
    }
    expect(wrapper.text()).toContain('消费者组无活跃成员且消息持续积压')
    expect(wrapper.find('[aria-label="下一步"]').attributes('disabled')).toBeDefined()
    await wrapper.find('[aria-label="上一步"]').trigger('click')
    expect(wrapper.text()).toContain('诊断报告尚未生成')
    wrapper.unmount()
  })

  it('auto-plays to the end without calling tools', async () => {
    vi.useFakeTimers()
    try {
      const wrapper = mount(TraceReplay, { props: { incident } })
      await wrapper.find('.replay-primary').trigger('click')
      await vi.advanceTimersByTimeAsync(700 * incident.traces.length)
      expect(wrapper.text()).toContain('8 / 8 个事件')
      expect(wrapper.text()).toContain('消费者组无活跃成员且消息持续积压')
      wrapper.unmount()
    } finally {
      vi.useRealTimers()
    }
  })

  it('marks missing historical evidence instead of presenting it as no evidence', async () => {
    const wrapper = mount(TraceReplay, { props: { incident: { ...incident, evidence: [] } } })
    for (let index = 0; index < 4; index += 1) {
      await wrapper.find('[aria-label="下一步"]').trigger('click')
    }
    expect(wrapper.text()).toContain('1 条证据引用缺少卡片')
    wrapper.unmount()
  })
})
