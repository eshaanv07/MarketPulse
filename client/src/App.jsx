
import { useEffect, useState } from "react";
import { io } from "socket.io-client";
import {
  Activity,
  Wifi,
  Database,
  TrendingUp,
  TrendingDown,
  Radio,
  Server
} from "lucide-react";

import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid
} from "recharts";

import "./App.css";

const SOCKET_URL = "http://localhost:5000";

const ASSETS = [
  { symbol: "BTCUSDT", name: "Bitcoin", short: "BTC" },
  { symbol: "ETHUSDT", name: "Ethereum", short: "ETH" },
  { symbol: "SOLUSDT", name: "Solana", short: "SOL" },
  { symbol: "BNBUSDT", name: "BNB", short: "BNB" },
  { symbol: "XRPUSDT", name: "XRP", short: "XRP" }
];

function App() {
  const [market, setMarket] = useState({});
  const [trades, setTrades] = useState([]);
  const [history, setHistory] = useState({});
  const [connected, setConnected] = useState(false);
  const [clients, setClients] = useState(0);
  const [alerts, setAlerts] = useState([]);
  const [selectedSymbol, setSelectedSymbol] = useState("BTCUSDT");
  const [systemStatusOpen, setSystemStatusOpen] = useState(false);
  const [systemInfo, setSystemInfo] = useState(null);
  const [systemStatusLoading, setSystemStatusLoading] = useState(false);

  const selectedAsset = ASSETS.find(asset => asset.symbol === selectedSymbol) || ASSETS[0];

  useEffect(() => {
    const socket = io(SOCKET_URL);

    socket.on("connect", () => {
      setConnected(true);
    });

    socket.on("disconnect", () => {
      setConnected(false);
    });

    socket.on("market:snapshot", (snapshot) => {
      setMarket(snapshot);

      setHistory(previous => {
        const next = { ...previous };

        Object.values(snapshot).forEach(asset => {
          if (!asset.price) return;

          const old = next[asset.symbol] || [];

          next[asset.symbol] = [
            ...old,
            {
              time: Date.now(),
              price: asset.price
            }
          ].slice(-60);
        });

        return next;
      });
    });

    socket.on("market:trades", (batch) => {
      setTrades(previous =>
        [...batch.reverse(), ...previous].slice(0, 50)
      );
    });
    // Receive market intelligence alerts
    socket.on("market:alerts", (newAlerts) => {
      setAlerts(previous => [
        ...newAlerts.slice().reverse(),
        ...previous
      ].slice(0, 30));

    });

    return () => {
      socket.disconnect();
    };
  }, []);

  useEffect(() => {
    if (!systemStatusOpen) return undefined;

    let cancelled = false;
    setSystemStatusLoading(true);

    fetch(`${SOCKET_URL}/health`)
      .then(response => {
        if (!response.ok) throw new Error(`Health check failed (${response.status})`);
        return response.json();
      })
      .then(data => {
        if (!cancelled) setSystemInfo(data);
      })
      .catch(error => {
        if (!cancelled) setSystemInfo({ error: error.message });
      })
      .finally(() => {
        if (!cancelled) setSystemStatusLoading(false);
      });

    return () => { cancelled = true; };
  }, [systemStatusOpen]);

  const formatPrice = value => {
    if (!Number.isFinite(value) || value === 0) return "—";

    return value.toLocaleString("en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: value < 1 ? 6 : 2
    });
  };

  const latestTrade = trades[0];

  return (
    <div className="dashboard">

      <header className="topbar">
        <div className="brand">
          <Activity size={27} />
          <span>MARKET<span className="accent">PULSE</span></span>
        </div>

        <div className="topbar-actions" style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <button
            type="button"
            className="system-status-trigger"
            onClick={() => setSystemStatusOpen(true)}
            style={{ border: "1px solid #334155", borderRadius: 8, padding: "9px 13px", background: "#172333", color: "#e5edf5", cursor: "pointer" }}
          >
            <Server size={16} style={{ verticalAlign: "middle", marginRight: 7 }} />
            System status
          </button>
        </div>
      </header>

      <main className="main-content">

        <section className="welcome">
          <div>
            <p className="eyebrow">MARKET INTELLIGENCE TERMINAL</p>
            <h1>Market Overview</h1>
            <p className="subtitle">
              Real-time cryptocurrency market monitoring
            </p>
          </div>

          <div className="exchange">
            <Radio size={17} />
            BINANCE SPOT
          </div>
        </section>

        {/* MARKET INTELLIGENCE ALERTS */}
          <section className="panel alerts-panel">

             <div className="panel-header">

              <div>
                <h2>Market Intelligence Alerts</h2>
                <p>Automated price and volume anomaly detection</p>
              </div>

              <span className="live-tag">
                {alerts.length} ALERTS
              </span>

             </div>

              <div className="alerts-list">
                {alerts.length === 0 ? (
                  <div className="empty-alerts">
                    <Activity size={28} />

                    <strong>No anomalies detected yet</strong>

                    <span>
                      MarketPulse is continuously monitoring
                      price movements and trading volumes.
                    </span>
                  </div>

                ) : (
                  alerts.map((alert, index) => (

                    <div
                     className={`alert-card ${alert.severity?.toLowerCase()}`}
                     key={alert.id || `${alert.symbol}-${alert.type}-${index}`}
                    >
                      <div className="alert-icon">
                        {alert.type === "PRICE_MOVEMENT"
                          ? <TrendingUp size={21} />
                          : <Activity size={21} />
                        }
                      </div>

                       <div className="alert-content">

                        <div className="alert-heading">

                          <strong>{alert.symbol}</strong>

                          <span className="alert-type">
                            {alert.type === "PRICE_MOVEMENT"
                             ? "PRICE MOVEMENT"
                              : "VOLUME SPIKE"}
                          </span>

                          <span className={`severity ${alert.severity?.toLowerCase()}`}>
                            {alert.severity}
                          </span>

                        </div>
                        
                        <p>{alert.message}</p>

                        <div className="alert-details">

                          <span>
                            Observed: {alert.observedValue}
                          </span>

                          <span>
                            Baseline: {alert.baselineValue}
                          </span>

                          {alert.changePct !== null &&
                           alert.changePct !== undefined && (
                            <span>
                              Change: {alert.changePct}%
                            </span>
                          )}

                        </div>

                        <span className="alert-time">
                          {new Date(alert.timestamp).toLocaleString()}
                        </span>

                       </div>

                      </div>
                    ))
                  )}

              </div>
              
          </section>

        {/* CRYPTOCURRENCY ASSET CARDS */}
        <section className="asset-grid">

          {ASSETS.map(asset => {
            const data = market[asset.symbol];

            return (
              <div className="asset-card" key={asset.symbol}>

                <div className="asset-heading">
                  <div>
                    <h3>{asset.name}</h3>
                    <span>{asset.short}/USDT</span>
                  </div>

                  <div className="asset-icon">
                    {asset.short}
                  </div>
                </div>

                <div className="asset-price">
                  ${formatPrice(data?.price)}
                </div>

                <div className="asset-footer">
                  <span>
                    {data?.trades10s ?? 0} trades / 10 sec
                  </span>
                  <span className="positive">
                    LIVE
                  </span>
                </div>

              </div>
            );
          })}

        </section>

        <section className="content-grid">

          <div className="panel chart-panel">

            <div className="panel-header">
              <div>
                <h2>Market Price Activity</h2>
                <p>Recent live price snapshots</p>
              </div>

              <span className="live-tag">LIVE</span>
            </div>

            <div className="chart-asset-selector" role="group" aria-label="Select cryptocurrency chart" style={{ display: "flex", flexWrap: "wrap", gap: 8, padding: "0 0 16px" }}>
              {ASSETS.map(asset => (
                <button
                  key={asset.symbol}
                  type="button"
                  onClick={() => setSelectedSymbol(asset.symbol)}
                  aria-pressed={selectedSymbol === asset.symbol}
                  style={{
                    border: selectedSymbol === asset.symbol ? "1px solid #00d6a0" : "1px solid #334155",
                    background: selectedSymbol === asset.symbol ? "rgba(0,214,160,0.14)" : "#172333",
                    color: selectedSymbol === asset.symbol ? "#00d6a0" : "#d5deea",
                    borderRadius: 7,
                    padding: "8px 13px",
                    cursor: "pointer",
                    fontWeight: 600
                  }}
                >
                  {asset.short}
                </button>
              ))}
            </div>

            <div className="chart-container">

              <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                  data={history[selectedSymbol] || []}
                >
                  <defs>
                    <linearGradient
                      id="priceGradient"
                      x1="0"
                      y1="0"
                      x2="0"
                      y2="1"
                    >
                      <stop
                        offset="0%"
                        stopColor="#00d6a0"
                        stopOpacity={0.3}
                      />

                      <stop
                        offset="100%"
                        stopColor="#00d6a0"
                        stopOpacity={0}
                      />
                    </linearGradient>
                  </defs>

                  <CartesianGrid
                    stroke="#242d3a"
                    strokeDasharray="3 3"
                  />

                  <XAxis
                    dataKey="time"
                    hide
                  />

                  <YAxis
                    domain={["auto", "auto"]}
                    stroke="#718096"
                    tick={{ fontSize: 11 }}
                    width={75}
                  />

                  <Tooltip
                    contentStyle={{
                      background: "#141c28",
                      border: "1px solid #293747",
                      borderRadius: "8px"
                    }}
                    formatter={value => [
                      `$${formatPrice(value)}`,
                      `${selectedAsset.short} Price`
                    ]}
                    labelFormatter={() => "Market update"}
                  />

                  <Area
                    type="monotone"
                    dataKey="price"
                    stroke="#00d6a0"
                    strokeWidth={2}
                    fill="url(#priceGradient)"
                    isAnimationActive={false}
                  />
                </AreaChart>
              </ResponsiveContainer>

            </div>

            <div className="chart-caption">
              {selectedAsset.short}/USDT · Binance Spot · Live snapshots
            </div>

          </div>

          <div className="panel analytics-panel">

            <div className="panel-header">
              <div>
                <h2>Market Activity</h2>
                <p>Rolling 10-second statistics</p>
              </div>
            </div>

            {ASSETS.map(asset => {
              const data = market[asset.symbol];

              return (
                <div className="activity-row" key={asset.symbol}>
                  <div>
                    <strong>{asset.short}</strong>
                    <span>{asset.name}</span>
                  </div>

                  <div className="activity-value">
                    <strong>
                      {data?.trades10s ?? 0}
                    </strong>
                    <span>Trades</span>
                  </div>

                  <div className="activity-value">
                    <strong>
                      {Number(data?.volume10s || 0).toFixed(4)}
                    </strong>
                    <span>Volume</span>
                  </div>
                </div>
              );
            })}

          </div>

        </section>

        <section className="content-grid bottom-grid">

          <div className="panel trades-panel">

            <div className="panel-header">
              <div>
                <h2>Live Trade Feed</h2>
                <p>Latest Binance trade events</p>
              </div>

              <span className="live-tag">STREAMING</span>
            </div>

            <div className="table-wrapper">

              <table>
                <thead>
                  <tr>
                    <th>Asset</th>
                    <th>Price</th>
                    <th>Quantity</th>
                    <th>Trade time</th>
                  </tr>
                </thead>

                <tbody>

                  {trades.slice(0, 15).map((trade, index) => (
                    <tr key={`${trade.symbol}-${trade.tradeId}-${index}`}>
                      <td>{trade.symbol}</td>
                      <td>${formatPrice(trade.price)}</td>
                      <td>{trade.quantity}</td>
                      <td>
                        {new Date(trade.tradeTime).toLocaleTimeString()}
                      </td>
                    </tr>
                  ))}

                  {trades.length === 0 && (
                    <tr>
                      <td colSpan="4" className="empty">
                        Waiting for live trade events...
                      </td>
                    </tr>
                  )}

                </tbody>
              </table>

            </div>

          </div>


        </section>

      </main>

      {systemStatusOpen && (
        <div
          className="system-status-backdrop"
          role="presentation"
          onClick={() => setSystemStatusOpen(false)}
          style={{ position: "fixed", inset: 0, zIndex: 1000, background: "rgba(0,0,0,0.68)", display: "grid", placeItems: "center", padding: 20 }}
        >
          <section
            className="panel system-status-modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="system-status-title"
            onClick={event => event.stopPropagation()}
            style={{ width: "min(520px, 100%)", padding: 24, background: "#101b2a", border: "1px solid #334155", borderRadius: 14, boxShadow: "0 20px 60px rgba(0,0,0,0.45)" }}
          >
            <div className="panel-header" style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 18 }}>
              <div>
                <h2 id="system-status-title">System status</h2>
                <p>Live backend and database health</p>
              </div>
              <button type="button" onClick={() => setSystemStatusOpen(false)} aria-label="Close system status" style={{ background: "transparent", color: "#e5edf5", border: 0, fontSize: 24, cursor: "pointer" }}>×</button>
            </div>

            {systemStatusLoading ? (
              <p>Checking system health…</p>
            ) : systemInfo?.error ? (
              <p role="alert">Unable to retrieve system health: {systemInfo.error}</p>
            ) : systemInfo ? (
              <>
                <div className="system-row"><Server size={18} /><span>Node.js backend</span><strong>{systemInfo.status === "running" ? "ONLINE" : "OFFLINE"}</strong></div>
                <div className="system-row"><Wifi size={18} /><span>Socket.IO</span><strong>{connected ? "CONNECTED" : "DISCONNECTED"}</strong></div>
                <div className="system-row"><Database size={18} /><span>MongoDB</span><strong>{String(systemInfo.database || "unknown").toUpperCase()}</strong></div>
                <div className="system-row"><Activity size={18} /><span>Connected clients</span><strong>{systemInfo.connectedClients ?? "—"}</strong></div>
                <div className="system-note" style={{ marginTop: 14 }}>Last checked: {systemInfo.timestamp ? new Date(systemInfo.timestamp).toLocaleString() : "—"}</div>
              </>
            ) : (
              <p>System health information is not available.</p>
            )}

            <div style={{ display: "flex", justifyContent: "flex-end", gap: 10, marginTop: 22 }}>
              <button type="button" onClick={() => { setSystemInfo(null); setSystemStatusOpen(false); setTimeout(() => setSystemStatusOpen(true), 0); }} style={{ border: "1px solid #334155", borderRadius: 7, padding: "8px 12px", background: "#172333", color: "#e5edf5", cursor: "pointer" }}>Refresh</button>
              <button type="button" onClick={() => setSystemStatusOpen(false)} style={{ border: 0, borderRadius: 7, padding: "8px 14px", background: "#00a982", color: "#061b17", cursor: "pointer", fontWeight: 700 }}>Close</button>
            </div>
          </section>
        </div>
      )}

      <footer>
        MARKETPULSE © 2026 · REAL-TIME MARKET INTELLIGENCE
        <span>DATA SOURCE: BINANCE</span>
      </footer>

    </div>
  );
}

export default App;
