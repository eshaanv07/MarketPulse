
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

        <div className="top-status">
          <span className={connected ? "live-dot" : "offline-dot"} />
          {connected ? "LIVE MARKET DATA" : "CONNECTING"}
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

            <div className="chart-container">

              <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                  data={history.BTCUSDT || []}
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
                      "BTC Price"
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
              BTC/USDT · Binance Spot · Live snapshots
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

          <div className="panel system-panel">

            <div className="panel-header">
              <div>
                <h2>System Status</h2>
                <p>Backend connectivity</p>
              </div>
            </div>

            <div className="system-row">
              <Server size={18} />
              <span>Node.js Backend</span>
              <strong className="positive">
                {connected ? "ONLINE" : "OFFLINE"}
              </strong>
            </div>

            <div className="system-row">
              <Wifi size={18} />
              <span>Socket.IO</span>
              <strong className="positive">
                {connected ? "CONNECTED" : "DISCONNECTED"}
              </strong>
            </div>

            <div className="system-row">
              <Database size={18} />
              <span>MongoDB</span>
              <strong>VERIFY VIA API</strong>
            </div>

            <div className="system-row">
              <Activity size={18} />
              <span>Latest trade</span>
              <strong>
                {latestTrade?.symbol || "WAITING"}
              </strong>
            </div>

            <div className="system-note">
              Market data is received from the MarketPulse
              backend and distributed through Socket.IO.
            </div>

          </div>

        </section>

      </main>

      <footer>
        MARKETPULSE © 2026 · REAL-TIME MARKET INTELLIGENCE
        <span>DATA SOURCE: BINANCE</span>
      </footer>

    </div>
  );
}

export default App;
