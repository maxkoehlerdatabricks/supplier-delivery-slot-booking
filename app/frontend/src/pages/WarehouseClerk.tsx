import { useState } from 'react'
import StatusBadge from '../components/StatusBadge'
import PODetail from '../components/PODetail'

interface POItem {
  ebeln: string
  ebelp: string
  matnr: string
  maktx: string
  menge: number
  meins: string
  netpr: number
  waers: string
  werks: string
}

interface POHeader {
  ebeln: string
  lifnr: string
  lifnr_name?: string
  ekorg: string
  bsart: string
  bedat: string
  status: string
  total_value?: number
  waers?: string
  items?: POItem[]
}

interface Booking {
  id: number
  booking_id: string
  slot_date: string
  time_window: string
  dock_name: string
  vendor_id: string
  po_number: string
  truck_plate?: string
  driver_name?: string
  status: string
  created_at: string
}

type SearchTab = 'po' | 'booking'

const STATUS_TRANSITIONS: Record<string, string[]> = {
  requested: ['confirmed', 'cancelled'],
  confirmed: ['checked_in', 'cancelled'],
  checked_in: ['completed', 'cancelled'],
  completed: [],
  cancelled: [],
}

const TRANSITION_STYLES: Record<string, string> = {
  confirmed: 'bg-blue-600 hover:bg-blue-700 text-white',
  checked_in: 'bg-purple-600 hover:bg-purple-700 text-white',
  completed: 'bg-green-600 hover:bg-green-700 text-white',
  cancelled: 'bg-red-600/80 hover:bg-red-700 text-white',
}

const TRANSITION_LABELS: Record<string, string> = {
  confirmed: 'Confirm',
  checked_in: 'Check In',
  completed: 'Complete',
  cancelled: 'Cancel',
}

export default function WarehouseClerk() {
  const [activeTab, setActiveTab] = useState<SearchTab>('po')
  const [searchInput, setSearchInput] = useState('')

  const [po, setPo] = useState<POHeader | null>(null)
  const [poLoading, setPoLoading] = useState(false)
  const [poError, setPoError] = useState<string | null>(null)

  const [bookings, setBookings] = useState<Booking[]>([])
  const [bookingsLoading, setBookingsLoading] = useState(false)
  const [bookingsError, setBookingsError] = useState<string | null>(null)

  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null)
  const [updatingBookingId, setUpdatingBookingId] = useState<number | null>(null)

  const showToast = (message: string, type: 'success' | 'error') => {
    setToast({ message, type })
    setTimeout(() => setToast(null), 4000)
  }

  const searchPO = async (poNumber: string) => {
    if (!poNumber.trim()) return
    setPoLoading(true)
    setPoError(null)
    setPo(null)
    setBookings([])
    setBookingsError(null)
    try {
      const res = await fetch(`/api/pos/${poNumber.trim()}`)
      if (!res.ok) throw new Error(res.status === 404 ? 'PO not found' : 'Failed to fetch PO')
      const data = await res.json()
      setPo(data)
      // Also fetch bookings linked to this PO
      fetchBookingsByPO(poNumber.trim())
    } catch (err: unknown) {
      setPoError(err instanceof Error ? err.message : 'Failed to fetch PO')
    } finally {
      setPoLoading(false)
    }
  }

  const fetchBookingsByPO = async (poNumber: string) => {
    setBookingsLoading(true)
    setBookingsError(null)
    try {
      const res = await fetch(`/api/bookings?po_number=${encodeURIComponent(poNumber)}`)
      if (!res.ok) throw new Error('Failed to fetch bookings')
      const data = await res.json()
      setBookings(Array.isArray(data) ? data : data.bookings || [])
    } catch (err: unknown) {
      setBookingsError(err instanceof Error ? err.message : 'Failed to fetch bookings')
    } finally {
      setBookingsLoading(false)
    }
  }

  const searchBooking = async (query: string) => {
    if (!query.trim()) return
    setBookingsLoading(true)
    setBookingsError(null)
    setBookings([])
    setPo(null)
    setPoError(null)
    try {
      // Try fetching as a specific booking ID first
      const res = await fetch(`/api/bookings/${encodeURIComponent(query.trim())}`)
      if (res.ok) {
        const data = await res.json()
        setBookings([data])
        return
      }
      // Fallback: search by vendor_id
      const res2 = await fetch(`/api/bookings?vendor_id=${encodeURIComponent(query.trim())}`)
      if (!res2.ok) throw new Error('No bookings found')
      const data2 = await res2.json()
      const result = Array.isArray(data2) ? data2 : data2.bookings || []
      if (result.length === 0) throw new Error('No bookings found for this query')
      setBookings(result)
    } catch (err: unknown) {
      setBookingsError(err instanceof Error ? err.message : 'Failed to fetch bookings')
    } finally {
      setBookingsLoading(false)
    }
  }

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    if (activeTab === 'po') {
      searchPO(searchInput)
    } else {
      searchBooking(searchInput)
    }
  }

  const updateBookingStatus = async (bookingId: number, newStatus: string) => {
    setUpdatingBookingId(bookingId)
    try {
      const res = await fetch(`/api/bookings/${bookingId}/status`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: newStatus }),
      })
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}))
        throw new Error(errData.detail || errData.message || 'Status update failed')
      }
      // Update local state
      setBookings(prev =>
        prev.map(b => (b.id === bookingId ? { ...b, status: newStatus } : b))
      )
      showToast(`Booking status updated to "${newStatus.replace('_', ' ')}"`, 'success')
    } catch (err: unknown) {
      showToast(err instanceof Error ? err.message : 'Status update failed', 'error')
    } finally {
      setUpdatingBookingId(null)
    }
  }

  return (
    <div className="animate-fade-in space-y-6">
      {/* Page Header */}
      <div>
        <h2 className="text-2xl font-bold text-white">Warehouse Clerk</h2>
        <p className="text-mercedes-silver text-sm mt-1">Search and manage deliveries and purchase orders</p>
      </div>

      {/* Toast Notification */}
      {toast && (
        <div
          className={`fixed top-20 right-6 z-50 px-5 py-3 rounded-lg border shadow-lg animate-fade-in ${
            toast.type === 'success'
              ? 'bg-green-900/90 border-green-500/30 text-green-300'
              : 'bg-red-900/90 border-red-500/30 text-red-300'
          }`}
        >
          <p className="text-sm font-medium">{toast.message}</p>
        </div>
      )}

      {/* Search Section */}
      <div className="bg-mercedes-dark rounded-xl border border-mercedes-gray/30 overflow-hidden">
        <div className="px-5 py-4 border-b border-mercedes-gray/30">
          {/* Tabs */}
          <div className="flex gap-1">
            {[
              { key: 'po' as SearchTab, label: 'By PO Number' },
              { key: 'booking' as SearchTab, label: 'By Booking' },
            ].map(tab => (
              <button
                key={tab.key}
                onClick={() => { setActiveTab(tab.key); setSearchInput(''); setPo(null); setBookings([]); setPoError(null); setBookingsError(null) }}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${
                  activeTab === tab.key
                    ? 'bg-blue-600/20 text-blue-400 border border-blue-500/30'
                    : 'text-mercedes-silver hover:text-white hover:bg-mercedes-gray/50'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>
        </div>
        <form onSubmit={handleSearch} className="p-5">
          <div className="flex gap-3">
            <div className="relative flex-1">
              <svg
                className="absolute left-3.5 top-1/2 -translate-y-1/2 w-4 h-4 text-mercedes-silver"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                strokeWidth={2}
              >
                <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <input
                type="text"
                value={searchInput}
                onChange={e => setSearchInput(e.target.value)}
                placeholder={activeTab === 'po' ? 'Enter PO number (e.g. 4500000123)' : 'Enter booking ID or vendor ID'}
                className="w-full bg-mercedes-black border border-mercedes-gray/40 rounded-lg pl-10 pr-4 py-2.5 text-mercedes-light text-sm placeholder:text-mercedes-gray focus:outline-none focus:border-blue-500/60 focus:ring-1 focus:ring-blue-500/30 transition-all duration-200"
              />
            </div>
            <button
              type="submit"
              disabled={!searchInput.trim()}
              className="px-6 py-2.5 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-600/40 disabled:cursor-not-allowed text-white text-sm font-medium rounded-lg transition-all duration-200"
            >
              Search
            </button>
          </div>
        </form>
      </div>

      {/* Results: Two-column layout */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Left: PO Details */}
        <div>
          <PODetail po={po} loading={poLoading} error={poError} />
        </div>

        {/* Right: Bookings */}
        <div>
          <div className="bg-mercedes-dark rounded-xl border border-mercedes-gray/30 overflow-hidden">
            <div className="px-5 py-4 border-b border-mercedes-gray/30">
              <h3 className="text-white font-semibold">Linked Bookings</h3>
              <p className="text-mercedes-silver text-xs mt-0.5">
                {bookings.length > 0 ? `${bookings.length} booking(s) found` : 'Search to view bookings'}
              </p>
            </div>

            {bookingsLoading ? (
              <div className="p-8 flex items-center justify-center">
                <div className="w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
              </div>
            ) : bookingsError ? (
              <div className="p-5">
                <p className="text-red-400 text-sm">{bookingsError}</p>
              </div>
            ) : bookings.length === 0 ? (
              <div className="p-8 text-center">
                <div className="text-mercedes-gray text-4xl mb-3">&#128666;</div>
                <p className="text-mercedes-silver text-sm">No bookings to display</p>
              </div>
            ) : (
              <div className="divide-y divide-mercedes-gray/20">
                {bookings.map(booking => {
                  const transitions = STATUS_TRANSITIONS[booking.status] || []
                  const isUpdating = updatingBookingId === booking.id
                  return (
                    <div key={booking.id} className="p-5 animate-fade-in">
                      <div className="flex items-start justify-between mb-3">
                        <div>
                          <p className="text-sm font-medium text-white font-mono">
                            {booking.booking_id || `#${booking.id}`}
                          </p>
                          <p className="text-xs text-mercedes-silver mt-0.5">
                            {booking.slot_date} &middot; {booking.time_window}
                          </p>
                        </div>
                        <StatusBadge status={booking.status} />
                      </div>
                      <div className="grid grid-cols-2 gap-2 text-xs mb-3">
                        <div>
                          <span className="text-mercedes-silver">Dock: </span>
                          <span className="text-mercedes-light">{booking.dock_name || '-'}</span>
                        </div>
                        <div>
                          <span className="text-mercedes-silver">Vendor: </span>
                          <span className="text-mercedes-light">{booking.vendor_id}</span>
                        </div>
                        <div>
                          <span className="text-mercedes-silver">PO: </span>
                          <span className="text-mercedes-light">{booking.po_number}</span>
                        </div>
                        {booking.truck_plate && (
                          <div>
                            <span className="text-mercedes-silver">Truck: </span>
                            <span className="text-mercedes-light">{booking.truck_plate}</span>
                          </div>
                        )}
                        {booking.driver_name && (
                          <div>
                            <span className="text-mercedes-silver">Driver: </span>
                            <span className="text-mercedes-light">{booking.driver_name}</span>
                          </div>
                        )}
                      </div>
                      {transitions.length > 0 && (
                        <div className="flex gap-2 mt-2">
                          {transitions.map(newStatus => (
                            <button
                              key={newStatus}
                              onClick={() => updateBookingStatus(booking.id, newStatus)}
                              disabled={isUpdating}
                              className={`px-3 py-1.5 rounded-md text-xs font-medium transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed ${
                                TRANSITION_STYLES[newStatus] || 'bg-mercedes-gray text-white'
                              }`}
                            >
                              {isUpdating ? (
                                <span className="flex items-center gap-1.5">
                                  <div className="w-3 h-3 border border-white/30 border-t-white rounded-full animate-spin" />
                                  Updating...
                                </span>
                              ) : (
                                TRANSITION_LABELS[newStatus] || newStatus
                              )}
                            </button>
                          ))}
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
