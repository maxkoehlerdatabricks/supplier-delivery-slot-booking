import { useState, useEffect } from 'react'

interface SelectedSlot {
  id: number
  slot_date: string
  time_window: string
  dock_name: string
}

interface BookingFormData {
  slot_id: number
  vendor_id: string
  po_number: string
  truck_plate: string
  driver_name: string
}

interface BookingFormProps {
  selectedSlot: SelectedSlot
  onSubmit: (data: BookingFormData) => Promise<void>
  onCancel: () => void
  submitting: boolean
}

interface POEntry {
  EBELN: string
  LIFNR: string
  BEDAT: string
  BSART: string
}

export default function BookingForm({ selectedSlot, onSubmit, onCancel, submitting }: BookingFormProps) {
  const [poList, setPoList] = useState<POEntry[]>([])
  const [poNumber, setPoNumber] = useState('')
  const [vendorId, setVendorId] = useState('')
  const [truckPlate, setTruckPlate] = useState('')
  const [driverName, setDriverName] = useState('')
  const [loadingPOs, setLoadingPOs] = useState(true)

  useEffect(() => {
    fetch('/api/pos/numbers')
      .then(res => res.ok ? res.json() : [])
      .then(data => setPoList(Array.isArray(data) ? data : []))
      .catch(() => setPoList([]))
      .finally(() => setLoadingPOs(false))
  }, [])

  const handlePoChange = (ebeln: string) => {
    setPoNumber(ebeln)
    const po = poList.find(p => p.EBELN === ebeln)
    if (po) {
      setVendorId(po.LIFNR)
    } else {
      setVendorId('')
    }
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (!vendorId.trim() || !poNumber.trim()) return
    onSubmit({
      slot_id: selectedSlot.id,
      vendor_id: vendorId.trim(),
      po_number: poNumber.trim(),
      truck_plate: truckPlate.trim() || '',
      driver_name: driverName.trim() || '',
    })
  }

  const inputClass =
    'w-full bg-mercedes-black border border-mercedes-gray/40 rounded-lg px-4 py-2.5 text-mercedes-light text-sm ' +
    'placeholder:text-mercedes-gray focus:outline-none focus:border-blue-500/60 focus:ring-1 focus:ring-blue-500/30 transition-all duration-200'

  const selectClass =
    'w-full bg-mercedes-black border border-mercedes-gray/40 rounded-lg px-4 py-2.5 text-mercedes-light text-sm ' +
    'focus:outline-none focus:border-blue-500/60 focus:ring-1 focus:ring-blue-500/30 transition-all duration-200 appearance-none'

  const labelClass = 'block text-xs font-medium text-mercedes-silver mb-1.5'

  return (
    <div className="bg-mercedes-dark rounded-xl border border-mercedes-gray/30 overflow-hidden animate-fade-in">
      <div className="px-5 py-4 border-b border-mercedes-gray/30">
        <h3 className="text-white font-semibold">Complete Your Booking</h3>
        <p className="text-mercedes-silver text-xs mt-1">
          {selectedSlot.slot_date} &middot; {selectedSlot.time_window} &middot; {selectedSlot.dock_name}
        </p>
      </div>
      <form onSubmit={handleSubmit} className="p-5 space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className={labelClass}>
              Purchase Order <span className="text-red-400">*</span>
            </label>
            <div className="relative">
              <select
                className={selectClass}
                value={poNumber}
                onChange={e => handlePoChange(e.target.value)}
                required
                disabled={submitting || loadingPOs}
              >
                <option value="">
                  {loadingPOs ? 'Loading POs...' : 'Select a Purchase Order'}
                </option>
                {poList.map(po => (
                  <option key={po.EBELN} value={po.EBELN}>
                    {po.EBELN} — {po.LIFNR} ({po.BEDAT})
                  </option>
                ))}
              </select>
              <div className="pointer-events-none absolute inset-y-0 right-3 flex items-center">
                <svg className="h-4 w-4 text-mercedes-silver" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                </svg>
              </div>
            </div>
          </div>
          <div>
            <label className={labelClass}>
              Vendor ID <span className="text-red-400">*</span>
            </label>
            <input
              type="text"
              className={inputClass + ' bg-mercedes-black/60'}
              value={vendorId}
              readOnly
              placeholder="Auto-filled from PO"
              required
            />
          </div>
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className={labelClass}>Truck License Plate</label>
            <input
              type="text"
              className={inputClass}
              placeholder="Optional"
              value={truckPlate}
              onChange={e => setTruckPlate(e.target.value)}
              disabled={submitting}
            />
          </div>
          <div>
            <label className={labelClass}>Driver Name</label>
            <input
              type="text"
              className={inputClass}
              placeholder="Optional"
              value={driverName}
              onChange={e => setDriverName(e.target.value)}
              disabled={submitting}
            />
          </div>
        </div>
        <div className="flex items-center gap-3 pt-2">
          <button
            type="submit"
            disabled={submitting || !vendorId.trim() || !poNumber.trim()}
            className="px-6 py-2.5 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-600/40 disabled:cursor-not-allowed text-white text-sm font-medium rounded-lg transition-all duration-200 flex items-center gap-2"
          >
            {submitting ? (
              <>
                <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                Submitting...
              </>
            ) : (
              'Book Delivery Slot'
            )}
          </button>
          <button
            type="button"
            onClick={onCancel}
            disabled={submitting}
            className="px-5 py-2.5 text-mercedes-silver hover:text-white text-sm font-medium rounded-lg border border-mercedes-gray/30 hover:border-mercedes-gray/60 transition-all duration-200"
          >
            Cancel
          </button>
        </div>
      </form>
    </div>
  )
}
