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

interface PODetailProps {
  po: POHeader | null
  loading: boolean
  error: string | null
}

export default function PODetail({ po, loading, error }: PODetailProps) {
  if (loading) {
    return (
      <div className="bg-mercedes-dark rounded-xl border border-mercedes-gray/30 p-8 flex items-center justify-center">
        <div className="flex flex-col items-center gap-3">
          <div className="w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
          <p className="text-mercedes-silver text-sm">Loading PO details...</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-mercedes-dark rounded-xl border border-red-500/30 p-6">
        <p className="text-red-400 text-sm">{error}</p>
      </div>
    )
  }

  if (!po) {
    return (
      <div className="bg-mercedes-dark rounded-xl border border-mercedes-gray/30 p-8 text-center">
        <div className="text-mercedes-gray text-4xl mb-3">&#128196;</div>
        <p className="text-mercedes-silver text-sm">Enter a PO number to view details</p>
      </div>
    )
  }

  return (
    <div className="bg-mercedes-dark rounded-xl border border-mercedes-gray/30 overflow-hidden animate-fade-in">
      {/* PO Header */}
      <div className="px-5 py-4 border-b border-mercedes-gray/30">
        <div className="flex items-center justify-between">
          <h3 className="text-white font-semibold">PO {po.ebeln}</h3>
          <span className="px-2.5 py-1 rounded-full text-xs font-medium bg-blue-500/20 text-blue-400 border border-blue-500/30">
            {po.status || 'Active'}
          </span>
        </div>
      </div>

      {/* Header Fields */}
      <div className="p-5 grid grid-cols-2 gap-4 border-b border-mercedes-gray/30">
        <div>
          <p className="text-[10px] uppercase tracking-wider text-mercedes-silver mb-0.5">Vendor</p>
          <p className="text-sm text-mercedes-light font-medium">{po.lifnr}</p>
          {po.lifnr_name && <p className="text-xs text-mercedes-silver">{po.lifnr_name}</p>}
        </div>
        <div>
          <p className="text-[10px] uppercase tracking-wider text-mercedes-silver mb-0.5">Order Date</p>
          <p className="text-sm text-mercedes-light font-medium">{po.bedat}</p>
        </div>
        <div>
          <p className="text-[10px] uppercase tracking-wider text-mercedes-silver mb-0.5">Order Type</p>
          <p className="text-sm text-mercedes-light font-medium">{po.bsart}</p>
        </div>
        <div>
          <p className="text-[10px] uppercase tracking-wider text-mercedes-silver mb-0.5">Purch. Org</p>
          <p className="text-sm text-mercedes-light font-medium">{po.ekorg}</p>
        </div>
        {po.total_value != null && (
          <div className="col-span-2">
            <p className="text-[10px] uppercase tracking-wider text-mercedes-silver mb-0.5">Total Value</p>
            <p className="text-sm text-mercedes-light font-medium">
              {po.total_value.toLocaleString(undefined, { minimumFractionDigits: 2 })} {po.waers || 'EUR'}
            </p>
          </div>
        )}
      </div>

      {/* Items Table */}
      {po.items && po.items.length > 0 && (
        <div>
          <div className="px-5 py-3 border-b border-mercedes-gray/30">
            <h4 className="text-xs font-medium text-mercedes-silver uppercase tracking-wider">
              Line Items ({po.items.length})
            </h4>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-mercedes-gray/30 text-left">
                  <th className="px-4 py-2.5 text-[10px] font-medium text-mercedes-silver uppercase tracking-wider">Item</th>
                  <th className="px-4 py-2.5 text-[10px] font-medium text-mercedes-silver uppercase tracking-wider">Material</th>
                  <th className="px-4 py-2.5 text-[10px] font-medium text-mercedes-silver uppercase tracking-wider">Description</th>
                  <th className="px-4 py-2.5 text-[10px] font-medium text-mercedes-silver uppercase tracking-wider text-right">Qty</th>
                  <th className="px-4 py-2.5 text-[10px] font-medium text-mercedes-silver uppercase tracking-wider">UoM</th>
                  <th className="px-4 py-2.5 text-[10px] font-medium text-mercedes-silver uppercase tracking-wider text-right">Net Price</th>
                </tr>
              </thead>
              <tbody>
                {po.items.map((item, idx) => (
                  <tr
                    key={`${item.ebeln}-${item.ebelp}`}
                    className={`border-b border-mercedes-gray/20 ${idx % 2 === 0 ? 'bg-mercedes-black/20' : ''}`}
                  >
                    <td className="px-4 py-2.5 text-mercedes-silver">{item.ebelp}</td>
                    <td className="px-4 py-2.5 font-mono text-xs text-mercedes-light">{item.matnr}</td>
                    <td className="px-4 py-2.5 text-mercedes-light">{item.maktx}</td>
                    <td className="px-4 py-2.5 text-right text-mercedes-light">{item.menge}</td>
                    <td className="px-4 py-2.5 text-mercedes-silver">{item.meins}</td>
                    <td className="px-4 py-2.5 text-right text-mercedes-light">
                      {item.netpr?.toLocaleString(undefined, { minimumFractionDigits: 2 })} {item.waers || ''}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
