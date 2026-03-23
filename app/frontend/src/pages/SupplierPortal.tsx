import { useState, useEffect } from 'react'
import SlotCalendar from '../components/SlotCalendar'
import BookingForm from '../components/BookingForm'

interface Slot {
  id: number
  slot_date: string
  time_window: string
  dock_id: string
  dock_name: string
  current_bookings: number
  max_capacity: number
  is_available: boolean
}

interface BookingResult {
  id: number
  booking_id: string
  slot_date: string
  time_window: string
  dock_name: string
  status: string
}

const PLANT_ID = '1100'

export default function SupplierPortal() {
  // Step state: 1=select date, 2=select slot, 3=fill form, 4=success
  const [step, setStep] = useState(1)
  const [availableDates, setAvailableDates] = useState<string[]>([])
  const [selectedDate, setSelectedDate] = useState<string | null>(null)
  const [slots, setSlots] = useState<Slot[]>([])
  const [selectedSlot, setSelectedSlot] = useState<Slot | null>(null)
  const [bookingResult, setBookingResult] = useState<BookingResult | null>(null)

  const [loadingDates, setLoadingDates] = useState(true)
  const [loadingSlots, setLoadingSlots] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Fetch available dates on mount
  useEffect(() => {
    setLoadingDates(true)
    setError(null)
    fetch(`/api/slots/dates?plant_id=${PLANT_ID}`)
      .then(res => {
        if (!res.ok) throw new Error('Failed to fetch available dates')
        return res.json()
      })
      .then(data => {
        const dates: string[] = Array.isArray(data) ? data : data.dates || []
        setAvailableDates(dates)
      })
      .catch(err => setError(err.message))
      .finally(() => setLoadingDates(false))
  }, [])

  // Fetch slots when date is selected
  useEffect(() => {
    if (!selectedDate) return
    setLoadingSlots(true)
    setSlots([])
    setSelectedSlot(null)
    setError(null)
    fetch(`/api/slots?plant_id=${PLANT_ID}&slot_date=${selectedDate}`)
      .then(res => {
        if (!res.ok) throw new Error('Failed to fetch slots')
        return res.json()
      })
      .then(data => {
        const slotList: Slot[] = Array.isArray(data) ? data : data.slots || []
        setSlots(slotList)
        setStep(2)
      })
      .catch(err => setError(err.message))
      .finally(() => setLoadingSlots(false))
  }, [selectedDate])

  const handleSlotSelect = (slot: Slot) => {
    setSelectedSlot(slot)
    setStep(3)
  }

  const handleBookingSubmit = async (formData: {
    slot_id: number
    vendor_id: string
    po_number: string
    truck_plate: string
    driver_name: string
  }) => {
    setSubmitting(true)
    setError(null)
    try {
      const res = await fetch('/api/bookings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formData),
      })
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}))
        throw new Error(errData.detail || errData.message || 'Booking failed')
      }
      const result = await res.json()
      setBookingResult(result)
      setStep(4)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Booking failed')
    } finally {
      setSubmitting(false)
    }
  }

  const handleReset = () => {
    setStep(1)
    setSelectedDate(null)
    setSlots([])
    setSelectedSlot(null)
    setBookingResult(null)
    setError(null)
  }

  // Format date for display
  const formatDate = (dateStr: string) => {
    const d = new Date(dateStr + 'T00:00:00')
    return d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' })
  }

  return (
    <div className="animate-fade-in space-y-6">
      {/* Page Header */}
      <div>
        <h2 className="text-2xl font-bold text-white">Supplier Portal</h2>
        <p className="text-mercedes-silver text-sm mt-1">Book a delivery slot for your purchase order</p>
      </div>

      {/* Progress Steps */}
      <div className="flex items-center gap-2">
        {['Select Date', 'Choose Slot', 'Booking Details', 'Confirmation'].map((label, idx) => {
          const stepNum = idx + 1
          const isActive = step === stepNum
          const isDone = step > stepNum
          return (
            <div key={label} className="flex items-center gap-2">
              {idx > 0 && (
                <div className={`w-8 h-px ${isDone ? 'bg-blue-500' : 'bg-mercedes-gray'}`} />
              )}
              <div className="flex items-center gap-2">
                <div
                  className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-medium transition-all duration-300 ${
                    isDone
                      ? 'bg-blue-600 text-white'
                      : isActive
                      ? 'bg-blue-600/30 text-blue-400 border border-blue-500/50'
                      : 'bg-mercedes-gray/50 text-mercedes-silver'
                  }`}
                >
                  {isDone ? '\u2713' : stepNum}
                </div>
                <span
                  className={`text-xs font-medium hidden sm:inline ${
                    isActive ? 'text-white' : 'text-mercedes-silver'
                  }`}
                >
                  {label}
                </span>
              </div>
            </div>
          )
        })}
      </div>

      {/* Error Banner */}
      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-3 flex items-center justify-between">
          <p className="text-red-400 text-sm">{error}</p>
          <button onClick={() => setError(null)} className="text-red-400 hover:text-red-300 text-sm">&times;</button>
        </div>
      )}

      {/* Step 1: Date Selection */}
      {step <= 2 && (
        <div className="bg-mercedes-dark rounded-xl border border-mercedes-gray/30 overflow-hidden">
          <div className="px-5 py-4 border-b border-mercedes-gray/30">
            <h3 className="text-white font-semibold">Select Delivery Date</h3>
            <p className="text-mercedes-silver text-xs mt-1">Choose from available delivery dates</p>
          </div>
          <div className="p-5">
            {loadingDates ? (
              <div className="flex items-center justify-center py-8">
                <div className="w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
              </div>
            ) : availableDates.length === 0 ? (
              <p className="text-mercedes-silver text-sm text-center py-4">No available dates found. Please try again later.</p>
            ) : (
              <div className="grid grid-cols-3 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-7 gap-2">
                {availableDates.map(date => (
                  <button
                    key={date}
                    onClick={() => setSelectedDate(date)}
                    className={`p-3 rounded-lg text-center transition-all duration-200 border ${
                      selectedDate === date
                        ? 'bg-blue-600/25 border-blue-500/50 text-blue-300'
                        : 'bg-mercedes-black/40 border-mercedes-gray/30 text-mercedes-light hover:border-mercedes-silver/40 hover:bg-mercedes-gray/30'
                    }`}
                  >
                    <div className="text-xs font-medium">{formatDate(date)}</div>
                    <div className="text-[10px] text-mercedes-silver mt-0.5">{date}</div>
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Step 2: Slot Calendar Grid */}
      {step >= 2 && step < 4 && selectedDate && (
        <SlotCalendar
          slots={slots}
          selectedSlotId={selectedSlot?.id ?? null}
          onSelect={handleSlotSelect}
          loading={loadingSlots}
        />
      )}

      {/* Step 3: Booking Form */}
      {step === 3 && selectedSlot && (
        <BookingForm
          selectedSlot={{
            id: selectedSlot.id,
            slot_date: selectedSlot.slot_date,
            time_window: selectedSlot.time_window,
            dock_name: selectedSlot.dock_name,
          }}
          onSubmit={handleBookingSubmit}
          onCancel={() => {
            setSelectedSlot(null)
            setStep(2)
          }}
          submitting={submitting}
        />
      )}

      {/* Step 4: Success */}
      {step === 4 && bookingResult && (
        <div className="bg-mercedes-dark rounded-xl border border-green-500/30 overflow-hidden animate-fade-in">
          <div className="p-8 text-center">
            <div className="w-16 h-16 mx-auto mb-4 rounded-full bg-green-500/20 flex items-center justify-center">
              <svg className="w-8 h-8 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
              </svg>
            </div>
            <h3 className="text-xl font-bold text-white mb-2">Booking Confirmed!</h3>
            <p className="text-mercedes-silver text-sm mb-6">Your delivery slot has been successfully booked.</p>
            <div className="inline-block bg-mercedes-black/50 rounded-lg px-6 py-4 text-left space-y-2 mb-6">
              <div className="flex gap-4">
                <span className="text-xs text-mercedes-silver w-24">Booking ID</span>
                <span className="text-sm text-blue-400 font-mono font-medium">{bookingResult.booking_id || bookingResult.id}</span>
              </div>
              {bookingResult.slot_date && (
                <div className="flex gap-4">
                  <span className="text-xs text-mercedes-silver w-24">Date</span>
                  <span className="text-sm text-mercedes-light">{bookingResult.slot_date}</span>
                </div>
              )}
              {bookingResult.time_window && (
                <div className="flex gap-4">
                  <span className="text-xs text-mercedes-silver w-24">Time Window</span>
                  <span className="text-sm text-mercedes-light">{bookingResult.time_window}</span>
                </div>
              )}
              {bookingResult.dock_name && (
                <div className="flex gap-4">
                  <span className="text-xs text-mercedes-silver w-24">Dock</span>
                  <span className="text-sm text-mercedes-light">{bookingResult.dock_name}</span>
                </div>
              )}
              <div className="flex gap-4">
                <span className="text-xs text-mercedes-silver w-24">Status</span>
                <span className="text-sm text-yellow-400">{bookingResult.status || 'requested'}</span>
              </div>
            </div>
            <div>
              <button
                onClick={handleReset}
                className="px-6 py-2.5 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-all duration-200"
              >
                Book Another Slot
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
