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

interface SlotCalendarProps {
  slots: Slot[]
  selectedSlotId: number | null
  onSelect: (slot: Slot) => void
  loading: boolean
}

export default function SlotCalendar({ slots, selectedSlotId, onSelect, loading }: SlotCalendarProps) {
  if (loading) {
    return (
      <div className="bg-mercedes-dark rounded-xl border border-mercedes-gray/30 p-8 flex items-center justify-center">
        <div className="flex flex-col items-center gap-3">
          <div className="w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
          <p className="text-mercedes-silver text-sm">Loading available slots...</p>
        </div>
      </div>
    )
  }

  if (slots.length === 0) {
    return (
      <div className="bg-mercedes-dark rounded-xl border border-mercedes-gray/30 p-8 text-center">
        <p className="text-mercedes-silver">No slots found for this date. Please select another date.</p>
      </div>
    )
  }

  // Extract unique time windows and docks
  const timeWindows = [...new Set(slots.map(s => s.time_window))].sort()
  const docks = [...new Set(slots.map(s => s.dock_id))].sort()
  const dockNames: Record<string, string> = {}
  slots.forEach(s => { dockNames[s.dock_id] = s.dock_name || s.dock_id })

  // Build lookup: timeWindow -> dockId -> slot
  const slotMap: Record<string, Record<string, Slot>> = {}
  slots.forEach(s => {
    if (!slotMap[s.time_window]) slotMap[s.time_window] = {}
    slotMap[s.time_window][s.dock_id] = s
  })

  return (
    <div className="bg-mercedes-dark rounded-xl border border-mercedes-gray/30 overflow-hidden">
      <div className="px-5 py-4 border-b border-mercedes-gray/30">
        <h3 className="text-white font-semibold">Available Delivery Slots</h3>
        <p className="text-mercedes-silver text-xs mt-1">Select a time window and dock for your delivery</p>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="border-b border-mercedes-gray/30">
              <th className="px-4 py-3 text-left text-xs font-medium text-mercedes-silver uppercase tracking-wider">
                Time Window
              </th>
              {docks.map(dock => (
                <th key={dock} className="px-4 py-3 text-center text-xs font-medium text-mercedes-silver uppercase tracking-wider">
                  {dockNames[dock]}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {timeWindows.map((tw, idx) => (
              <tr key={tw} className={idx % 2 === 0 ? 'bg-mercedes-black/30' : ''}>
                <td className="px-4 py-3 text-sm text-mercedes-light font-medium whitespace-nowrap">
                  {tw}
                </td>
                {docks.map(dock => {
                  const slot = slotMap[tw]?.[dock]
                  if (!slot) {
                    return (
                      <td key={dock} className="px-4 py-3 text-center">
                        <span className="text-mercedes-gray text-xs">N/A</span>
                      </td>
                    )
                  }
                  const isSelected = selectedSlotId === slot.id
                  const isFull = !slot.is_available
                  const utilization = slot.max_capacity > 0 ? slot.current_bookings / slot.max_capacity : 0

                  let cellStyle = 'border border-mercedes-gray/30 rounded-lg px-3 py-2 text-center transition-all duration-200 cursor-pointer '
                  if (isSelected) {
                    cellStyle += 'bg-blue-600/30 border-blue-500/60 ring-1 ring-blue-500/40'
                  } else if (isFull) {
                    cellStyle += 'bg-red-900/20 border-red-500/20 cursor-not-allowed opacity-50'
                  } else if (utilization > 0.7) {
                    cellStyle += 'bg-yellow-900/15 border-yellow-500/20 hover:border-yellow-500/40 hover:bg-yellow-900/25'
                  } else {
                    cellStyle += 'bg-green-900/15 border-green-500/20 hover:border-green-500/40 hover:bg-green-900/25'
                  }

                  return (
                    <td key={dock} className="px-3 py-2">
                      <div
                        className={cellStyle}
                        onClick={() => !isFull && onSelect(slot)}
                        title={isFull ? 'This slot is fully booked' : `${slot.max_capacity - slot.current_bookings} of ${slot.max_capacity} available`}
                      >
                        <div className="text-xs font-medium">
                          {isFull ? (
                            <span className="text-red-400">Full</span>
                          ) : isSelected ? (
                            <span className="text-blue-300">Selected</span>
                          ) : (
                            <span className={utilization > 0.7 ? 'text-yellow-400' : 'text-green-400'}>
                              {slot.max_capacity - slot.current_bookings} avail
                            </span>
                          )}
                        </div>
                        <div className="text-[10px] text-mercedes-silver mt-0.5">
                          {slot.current_bookings}/{slot.max_capacity}
                        </div>
                      </div>
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="px-5 py-3 border-t border-mercedes-gray/30 flex gap-4 text-[10px] text-mercedes-silver">
        <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-sm bg-green-900/40 border border-green-500/30" /> Available</span>
        <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-sm bg-yellow-900/40 border border-yellow-500/30" /> Limited</span>
        <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-sm bg-red-900/40 border border-red-500/30" /> Full</span>
        <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-sm bg-blue-600/40 border border-blue-500/30" /> Selected</span>
      </div>
    </div>
  )
}
