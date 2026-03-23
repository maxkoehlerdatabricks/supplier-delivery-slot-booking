import { useState, useEffect } from 'react'
import StatusBadge from '../components/StatusBadge'

interface DashboardStats {
  total_bookings: number
  requested: number
  confirmed: number
  checked_in: number
  completed: number
  cancelled: number
  today_slots?: {
    dock_name: string
    dock_id: string
    used: number
    total: number
  }[]
  recent_bookings?: {
    id: number
    booking_id: string
    slot_date: string
    time_window: string
    dock_name: string
    vendor_id: string
    po_number: string
    status: string
    created_at: string
  }[]
}

interface StatCardProps {
  label: string
  value: number
  color: string
  borderColor: string
}

function StatCard({ label, value, color, borderColor }: StatCardProps) {
  return (
    <div className={`bg-mercedes-dark rounded-xl border ${borderColor} p-5 transition-all duration-300 hover:scale-[1.02]`}>
      <p className="text-xs font-medium text-mercedes-silver uppercase tracking-wider mb-2">{label}</p>
      <p className={`text-3xl font-bold ${color}`}>{value.toLocaleString()}</p>
    </div>
  )
}

export default function Dashboard() {
  const [stats, setStats] = useState<DashboardStats | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetchStats()
    const interval = setInterval(fetchStats, 30000) // Refresh every 30s
    return () => clearInterval(interval)
  }, [])

  const fetchStats = () => {
    fetch('/api/bookings/stats/summary')
      .then(res => {
        if (!res.ok) throw new Error('Failed to fetch dashboard stats')
        return res.json()
      })
      .then(data => {
        setStats(data)
        setError(null)
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="flex flex-col items-center gap-3">
          <div className="w-10 h-10 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
          <p className="text-mercedes-silver text-sm">Loading dashboard...</p>
        </div>
      </div>
    )
  }

  if (error && !stats) {
    return (
      <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-6 py-8 text-center">
        <p className="text-red-400 text-sm mb-3">{error}</p>
        <button
          onClick={() => { setLoading(true); fetchStats() }}
          className="px-4 py-2 bg-red-600/30 hover:bg-red-600/50 text-red-300 text-sm rounded-lg transition-all"
        >
          Retry
        </button>
      </div>
    )
  }

  const s = stats!

  return (
    <div className="animate-fade-in space-y-6">
      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">Dashboard</h2>
          <p className="text-mercedes-silver text-sm mt-1">Real-time overview of delivery operations</p>
        </div>
        <button
          onClick={() => { setLoading(true); fetchStats() }}
          className="px-4 py-2 text-sm text-mercedes-silver hover:text-white border border-mercedes-gray/30 hover:border-mercedes-gray/60 rounded-lg transition-all duration-200"
        >
          Refresh
        </button>
      </div>

      {/* Stat Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard label="Total Bookings" value={s.total_bookings} color="text-white" borderColor="border-mercedes-gray/30" />
        <StatCard label="Requested" value={s.requested} color="text-yellow-400" borderColor="border-yellow-500/20" />
        <StatCard label="Confirmed" value={s.confirmed} color="text-blue-400" borderColor="border-blue-500/20" />
        <StatCard label="Completed" value={s.completed} color="text-green-400" borderColor="border-green-500/20" />
      </div>

      {/* Middle Section: Slot Utilization */}
      {s.today_slots && s.today_slots.length > 0 && (
        <div className="bg-mercedes-dark rounded-xl border border-mercedes-gray/30 overflow-hidden">
          <div className="px-5 py-4 border-b border-mercedes-gray/30">
            <h3 className="text-white font-semibold">Today's Slot Utilization</h3>
            <p className="text-mercedes-silver text-xs mt-0.5">Current capacity usage per dock</p>
          </div>
          <div className="p-5 space-y-3">
            {s.today_slots.map(dock => {
              const pct = dock.total > 0 ? Math.round((dock.used / dock.total) * 100) : 0
              let barColor = 'bg-green-500'
              if (pct > 80) barColor = 'bg-red-500'
              else if (pct > 50) barColor = 'bg-yellow-500'

              return (
                <div key={dock.dock_id}>
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-sm text-mercedes-light font-medium">{dock.dock_name}</span>
                    <span className="text-xs text-mercedes-silver">
                      {dock.used}/{dock.total} ({pct}%)
                    </span>
                  </div>
                  <div className="w-full h-2 bg-mercedes-black rounded-full overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all duration-500 ${barColor}`}
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Bottom: Recent Activity */}
      <div className="bg-mercedes-dark rounded-xl border border-mercedes-gray/30 overflow-hidden">
        <div className="px-5 py-4 border-b border-mercedes-gray/30">
          <h3 className="text-white font-semibold">Recent Activity</h3>
          <p className="text-mercedes-silver text-xs mt-0.5">Latest booking activity across all docks</p>
        </div>
        {s.recent_bookings && s.recent_bookings.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-mercedes-gray/30 text-left">
                  <th className="px-4 py-3 text-[10px] font-medium text-mercedes-silver uppercase tracking-wider">Booking</th>
                  <th className="px-4 py-3 text-[10px] font-medium text-mercedes-silver uppercase tracking-wider">Date</th>
                  <th className="px-4 py-3 text-[10px] font-medium text-mercedes-silver uppercase tracking-wider">Time</th>
                  <th className="px-4 py-3 text-[10px] font-medium text-mercedes-silver uppercase tracking-wider">Dock</th>
                  <th className="px-4 py-3 text-[10px] font-medium text-mercedes-silver uppercase tracking-wider">Vendor</th>
                  <th className="px-4 py-3 text-[10px] font-medium text-mercedes-silver uppercase tracking-wider">PO</th>
                  <th className="px-4 py-3 text-[10px] font-medium text-mercedes-silver uppercase tracking-wider">Status</th>
                </tr>
              </thead>
              <tbody>
                {s.recent_bookings.map((b, idx) => (
                  <tr
                    key={b.id}
                    className={`border-b border-mercedes-gray/20 hover:bg-mercedes-gray/20 transition-colors ${
                      idx % 2 === 0 ? 'bg-mercedes-black/20' : ''
                    }`}
                  >
                    <td className="px-4 py-3 font-mono text-xs text-blue-400">{b.booking_id || `#${b.id}`}</td>
                    <td className="px-4 py-3 text-mercedes-light">{b.slot_date}</td>
                    <td className="px-4 py-3 text-mercedes-light">{b.time_window}</td>
                    <td className="px-4 py-3 text-mercedes-light">{b.dock_name || '-'}</td>
                    <td className="px-4 py-3 text-mercedes-silver">{b.vendor_id}</td>
                    <td className="px-4 py-3 font-mono text-xs text-mercedes-light">{b.po_number}</td>
                    <td className="px-4 py-3"><StatusBadge status={b.status} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="p-8 text-center">
            <p className="text-mercedes-silver text-sm">No recent activity to display</p>
          </div>
        )}
      </div>
    </div>
  )
}
