import { useEffect, useMemo, useState } from 'react'
import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LineElement,
  LinearScale,
  PointElement,
  Title,
  Tooltip,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
)

interface ScoresBucket {
  bucket: string
  count: number
}

interface TimelinePoint {
  date: string
  submissions: number
}

interface PassRateRow {
  task: string
  avg_score: number
  attempts: number
}

interface DashboardProps {
  token: string
}

type LoadState =
  | { status: 'loading' }
  | {
      status: 'success'
      scores: ScoresBucket[]
      timeline: TimelinePoint[]
      passRates: PassRateRow[]
    }
  | { status: 'error'; message: string }

const DEFAULT_LABS = ['lab-01', 'lab-02', 'lab-03', 'lab-04', 'lab-05']

async function fetchAnalytics<T>(path: string, token: string): Promise<T> {
  const response = await fetch(path, {
    headers: { Authorization: `Bearer ${token}` },
  })

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${path}`)
  }

  return (await response.json()) as T
}

function Dashboard({ token }: DashboardProps) {
  const [lab, setLab] = useState('lab-01')
  const [state, setState] = useState<LoadState>({ status: 'loading' })

  useEffect(() => {
    let isCancelled = false

    async function load(): Promise<void> {
      setState({ status: 'loading' })
      try {
        const [scores, timeline, passRates] = await Promise.all([
          fetchAnalytics<ScoresBucket[]>(`/analytics/scores?lab=${lab}`, token),
          fetchAnalytics<TimelinePoint[]>(`/analytics/timeline?lab=${lab}`, token),
          fetchAnalytics<PassRateRow[]>(`/analytics/pass-rates?lab=${lab}`, token),
        ])

        if (!isCancelled) {
          setState({ status: 'success', scores, timeline, passRates })
        }
      } catch (error) {
        if (!isCancelled) {
          const message =
            error instanceof Error ? error.message : 'Unknown dashboard error'
          setState({ status: 'error', message })
        }
      }
    }

    load()

    return () => {
      isCancelled = true
    }
  }, [lab, token])

  const scoreChartData = useMemo(() => {
    if (state.status !== 'success') {
      return { labels: [], datasets: [] }
    }

    return {
      labels: state.scores.map((row) => row.bucket),
      datasets: [
        {
          label: 'Submissions',
          data: state.scores.map((row) => row.count),
          backgroundColor: '#1f77b4',
          borderRadius: 6,
        },
      ],
    }
  }, [state])

  const timelineChartData = useMemo(() => {
    if (state.status !== 'success') {
      return { labels: [], datasets: [] }
    }

    return {
      labels: state.timeline.map((point) => point.date),
      datasets: [
        {
          label: 'Submissions per day',
          data: state.timeline.map((point) => point.submissions),
          borderColor: '#d62828',
          backgroundColor: 'rgba(214, 40, 40, 0.15)',
          tension: 0.25,
          fill: true,
          pointRadius: 4,
        },
      ],
    }
  }, [state])

  return (
    <section className="dashboard">
      <div className="dashboard-controls">
        <label htmlFor="lab-select">Lab:</label>
        <select
          id="lab-select"
          value={lab}
          onChange={(e) => setLab(e.target.value)}
        >
          {DEFAULT_LABS.map((value) => (
            <option key={value} value={value}>
              {value}
            </option>
          ))}
        </select>
      </div>

      {state.status === 'loading' && <p>Loading dashboard...</p>}
      {state.status === 'error' && <p>Error: {state.message}</p>}

      {state.status === 'success' && (
        <>
          <div className="dashboard-grid">
            <article className="card">
              <h2>Score Distribution</h2>
              <Bar data={scoreChartData} />
            </article>

            <article className="card">
              <h2>Submission Timeline</h2>
              <Line data={timelineChartData} />
            </article>
          </div>

          <article className="card">
            <h2>Pass Rates by Task</h2>
            <table>
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Avg score</th>
                  <th>Attempts</th>
                </tr>
              </thead>
              <tbody>
                {state.passRates.map((row) => (
                  <tr key={row.task}>
                    <td>{row.task}</td>
                    <td>{row.avg_score.toFixed(1)}</td>
                    <td>{row.attempts}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </article>
        </>
      )}
    </section>
  )
}

export default Dashboard
