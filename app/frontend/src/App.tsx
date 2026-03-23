import { Routes, Route, NavLink } from 'react-router-dom'
import SupplierPortal from './pages/SupplierPortal'
import WarehouseClerk from './pages/WarehouseClerk'
import Dashboard from './pages/Dashboard'

function App() {
  return (
    <div className="min-h-screen bg-mercedes-black">
      {/* Navigation Bar */}
      <nav className="bg-mercedes-dark border-b border-mercedes-gray/50 sticky top-0 z-50 backdrop-blur-sm bg-opacity-90">
        <div className="max-w-7xl mx-auto px-6">
          <div className="flex items-center justify-between h-16">
            {/* Logo / Brand */}
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 rounded-full bg-gradient-to-br from-blue-500 to-blue-700 flex items-center justify-center text-white text-sm font-bold">D</div>
              <div>
                <h1 className="text-white font-semibold text-lg leading-tight">Delivery Slot Booking</h1>
                <p className="text-mercedes-silver text-xs">LiDAR Sensor Manufacturing Plant</p>
              </div>
            </div>
            {/* Nav Links */}
            <div className="flex gap-1">
              {[
                { to: '/', label: 'Supplier Portal' },
                { to: '/clerk', label: 'Warehouse Clerk' },
                { to: '/dashboard', label: 'Dashboard' },
              ].map(({ to, label }) => (
                <NavLink
                  key={to}
                  to={to}
                  end={to === '/'}
                  className={({ isActive }) =>
                    `px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${
                      isActive
                        ? 'bg-blue-600/20 text-blue-400 border border-blue-500/30'
                        : 'text-mercedes-silver hover:text-white hover:bg-mercedes-gray/50'
                    }`
                  }
                >
                  {label}
                </NavLink>
              ))}
            </div>
          </div>
        </div>
      </nav>

      {/* Page Content */}
      <main className="max-w-7xl mx-auto px-6 py-8">
        <Routes>
          <Route path="/" element={<SupplierPortal />} />
          <Route path="/clerk" element={<WarehouseClerk />} />
          <Route path="/dashboard" element={<Dashboard />} />
        </Routes>
      </main>
    </div>
  )
}

export default App
