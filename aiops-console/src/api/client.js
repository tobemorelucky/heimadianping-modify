import axios from 'axios'

const client = axios.create({
  baseURL: import.meta.env.VITE_AIOPS_API_BASE_URL || '/api',
  timeout: 10000
})

export function isAgentOffline(error) {
  const status = error?.response?.status
  return status == null || status >= 500
}

export async function listIncidents() {
  const { data } = await client.get('/incidents')
  return data
}

export async function getIncident(id) {
  const { data } = await client.get(`/incidents/${encodeURIComponent(id)}`)
  return data
}

export async function getMonitoringSummary() {
  const { data } = await client.get('/monitoring/summary')
  return data
}
