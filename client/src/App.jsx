import { useEffect, useMemo, useState } from "react";
import { io } from "socket.io-client";
import {
  Activity, Wifi, Database, TrendingUp, TrendingDown, Radio, Server,
  LayoutDashboard, ChartNoAxesCombined, Bell, Layers3, X, RefreshCw,
  ChevronLeft, ChevronRight, Clock3, ArrowUpRight, ArrowDownRight
} from "lucide-react";
import {
  ResponsiveContainer, AreaChart, Area, LineChart, Line, XAxis, YAxis,
  Tooltip, CartesianGrid
} from "recharts";
import "./App.css";

const SOCKET_URL = "http://localhost:5000";
const ASSETS = [
  { symbol: "BTCUSDT", name: "Bitcoin", short: "BTC", color: "#f7931a" },
  { symbol: "ETHUSDT", name: "Ethereum", short: "ETH", color: "#8b9cff" },
  { symbol: "SOLUSDT", name: "Solana", short: "SOL", color: "#a78bfa" },
  { symbol: "XRPUSDT", name: "XRP", short: "XRP", color: "#cbd5e1" }
];

function AssetLogo({ symbol, size = 30 }) {
  const common = { width: size, height: size, viewBox: "0 0 40 40", role: "img", "aria-label": `${symbol} logo` };
  if (symbol === "BTCUSDT") return <svg {...common}><circle cx="20" cy="20" r="20" fill="#f7931a"/><text x="20" y="28" textAnchor="middle" fontSize="25" fontWeight="800" fill="#fff">₿</text></svg>;
  if (symbol === "ETHUSDT") return <svg {...common}><circle cx="20" cy="20" r="20" fill="#26365d"/><path d="M20 4 9 21l11 6 11-6L20 4Z" fill="#c5d0ff"/><path d="m20 4 11 17-11 6V4Z" fill="#8297f5"/><path d="m9 23 11 7 11-7-11 13L9 23Z" fill="#8b9cff"/></svg>;
  if (symbol === "SOLUSDT") return <svg {...common}><circle cx="20" cy="20" r="20" fill="#101322"/><path d="M10 10h21l-4 4H6l4-4Zm0 8h21l-4 4H6l4-4Zm0 8h21l-4 4H6l4-4Z" fill="url(#solLogoGradient)"/><defs><linearGradient id="solLogoGradient" x1="6" y1="10" x2="31" y2="30"><stop stopColor="#00ffa3"/><stop offset="1" stopColor="#dc1fff"/></linearGradient></defs></svg>;
  return <svg {...common}><circle cx="20" cy="20" r="20" fill="#252d38"/><path d="M7 11h26L20 21 7 11Zm0 18h26L20 19 7 29Z" fill="none" stroke="#e6edf7" strokeWidth="3" strokeLinejoin="round"/></svg>;
}

function AlertMetric({ label, value, tone = "neutral" }) {
  return <div className={`alert-metric ${tone}`}><span>{label}</span><strong>{value}</strong></div>;
}

function MiniSparkline({ points }) {
  const values = (points || [])
    .map(point => Number(point?.price))
    .filter(price => Number.isFinite(price) && price > 0)
    .slice(-36);

  if (values.length < 2) {
    return <div className="sparkline-waiting" aria-label="Waiting for live price samples">LIVE</div>;
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || Math.max(Math.abs(max) * 0.0001, 0.000001);
  const coords = values.map((value, index) => {
    const x = 2 + (index / (values.length - 1)) * 96;
    const y = 31 - ((value - min) / range) * 25;
    return `${x},${y}`;
  }).join(" ");

  return (
    <svg className="mini-sparkline-svg" viewBox="0 0 100 36" preserveAspectRatio="none" aria-label="Live price trend">
      <polyline points={coords} fill="none" stroke="#00e5a8" strokeWidth="3"
        strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke"/>
    </svg>
  );
}


function appendPricePoints(previous, samples) {
  const latestBySymbol = new Map();
  const batch = Array.isArray(samples) ? samples : (samples ? Object.values(samples) : []);

  batch.forEach(sample => {
    const symbol = sample?.symbol || sample?.s;
    const price = Number(sample?.price ?? sample?.p ?? sample?.lastPrice ?? sample?.close);
    const rawTime = sample?.time ?? sample?.timestamp ?? sample?.tradeTime ?? sample?.T ?? sample?.E;
    let time = rawTime == null ? Date.now() : (typeof rawTime === "number" ? rawTime : new Date(rawTime).getTime());
    // Binance timestamps are milliseconds; tolerate seconds-based timestamps too.
    if (Number.isFinite(time) && time > 0 && time < 100000000000) time *= 1000;

    if (!symbol || !Number.isFinite(price) || price <= 0 || !Number.isFinite(time)) return;
    const current = latestBySymbol.get(symbol);
    if (!current || time >= current.time) latestBySymbol.set(symbol, { time, price });
  });

  const next = { ...previous };
  latestBySymbol.forEach((point, symbol) => {
    const oldPoints = next[symbol] || [];
    const last = oldPoints[oldPoints.length - 1];

    // Ignore duplicate timestamps, but don't discard valid updates just because
    // several Binance batches arrive within the same second.
    if (last && point.time <= last.time) return;

    next[symbol] = [...oldPoints, point].slice(-90);
  });
  return next;
}

function App() {
  const [market, setMarket] = useState({});
  const [trades, setTrades] = useState([]);
  const [history, setHistory] = useState({});
  const [connected, setConnected] = useState(false);
  const [alerts, setAlerts] = useState([]);
  const [selectedSymbol, setSelectedSymbol] = useState("BTCUSDT");
  const [activePage, setActivePage] = useState("dashboard");
  const [systemStatusOpen, setSystemStatusOpen] = useState(false);
  const [systemInfo, setSystemInfo] = useState(null);
  const [systemStatusLoading, setSystemStatusLoading] = useState(false);
  const [historySymbol, setHistorySymbol] = useState("BTCUSDT");
  const [historyRange, setHistoryRange] = useState("3h");
  const [historyInterval, setHistoryInterval] = useState("1m");
  const [historicalCandles, setHistoricalCandles] = useState([]);
  const [historicalAlerts, setHistoricalAlerts] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState("");
  const [alertPage, setAlertPage] = useState(1);
  const [alertSymbolFilter, setAlertSymbolFilter] = useState("ALL");
  const [alertTypeFilter, setAlertTypeFilter] = useState("ALL");
  const [alertSeverityFilter, setAlertSeverityFilter] = useState("ALL");

  const selectedAsset = ASSETS.find(asset => asset.symbol === selectedSymbol) || ASSETS[0];
  const formatPrice = value => {
    if (!Number.isFinite(Number(value)) || Number(value) === 0) return "—";
    return Number(value).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: Number(value) < 1 ? 6 : 2 });
  };
  const formatNumber = value => Number(value || 0).toLocaleString("en-US", { maximumFractionDigits: 4 });
  const latestAlerts = useMemo(() => alerts.slice(0, 3), [alerts]);
  const filteredLiveAlerts = useMemo(() => alerts.filter(alert =>
    (alertSymbolFilter === "ALL" || alert.symbol === alertSymbolFilter) &&
    (alertTypeFilter === "ALL" || alert.type === alertTypeFilter) &&
    (alertSeverityFilter === "ALL" || alert.severity === alertSeverityFilter)
  ), [alerts, alertSymbolFilter, alertTypeFilter, alertSeverityFilter]);
  const liveAlertPages = Math.max(1, Math.ceil(filteredLiveAlerts.length / 25));
  const liveAlertRows = filteredLiveAlerts.slice((alertPage - 1) * 25, alertPage * 25);
  const liveAlertTotal = filteredLiveAlerts.length;

  // Live Socket.IO feed. Only market:alerts belongs to this browser session;
  // market:alerts:history is deliberately not merged into the live alert list.
  useEffect(() => {
    const socket = io(SOCKET_URL);
    socket.on("connect", () => setConnected(true));
    socket.on("disconnect", () => setConnected(false));

    socket.on("market:snapshot", snapshot => {
      const entries = Object.entries(snapshot || {}).map(([key, asset]) => ({
        ...asset,
        symbol: asset?.symbol || key,
        time: Date.now()
      }));
      setMarket(previous => ({ ...previous, ...(snapshot || {}) }));
      setHistory(previous => appendPricePoints(previous, entries));
    });

    socket.on("market:trades", batch => {
      const safeBatch = Array.isArray(batch) ? batch : [];
      setTrades(previous => [...safeBatch.slice().reverse(), ...previous].slice(0, 80));
      // Trade batches arrive more frequently than snapshots and provide the
      // real price points used by each asset's mini-graph.
      setHistory(previous => appendPricePoints(previous, safeBatch));
    });

    socket.on("market:alerts", newAlerts => {
      const safeAlerts = Array.isArray(newAlerts) ? newAlerts : [];
      setAlerts(previous => {
        const byId = new Map();
        [...safeAlerts.slice().reverse(), ...previous].forEach(alert => {
          const key = alert.alertId || alert.id || alert._id ||
            `${alert.symbol}-${alert.type}-${alert.timestamp}`;
          if (!byId.has(key)) byId.set(key, alert);
        });
        return Array.from(byId.values())
          .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
          .slice(0, 500);
      });
    });

    return () => socket.disconnect();
  }, []);

  // Status panel uses the existing backend health/status endpoints.
  useEffect(() => {
    if (!systemStatusOpen) return undefined;
    let cancelled = false;
    setSystemStatusLoading(true);
    fetch(`${SOCKET_URL}/api/system/status`)
      .then(response => { if (!response.ok) throw new Error(`Status check failed (${response.status})`); return response.json(); })
      .then(data => { if (!cancelled) setSystemInfo(data); })
      .catch(error => { if (!cancelled) setSystemInfo({ error: error.message }); })
      .finally(() => { if (!cancelled) setSystemStatusLoading(false); });
    return () => { cancelled = true; };
  }, [systemStatusOpen]);

  // Existing historical market-data API and query parameters are preserved.
  useEffect(() => {
    if (activePage !== "history") return undefined;
    let cancelled = false;
    const loadHistoricalData = async () => {
      setHistoryLoading(true); setHistoryError("");
      try {
        const params = new URLSearchParams({ symbol: historySymbol, range: historyRange, interval: historyInterval });
        const [marketResponse, alertsResponse] = await Promise.all([
          fetch(`${SOCKET_URL}/api/market/history?${params.toString()}`),
          fetch(`${SOCKET_URL}/api/alerts?symbol=${encodeURIComponent(historySymbol)}&limit=50&page=1`)
        ]);
        if (!marketResponse.ok) { const body = await marketResponse.json().catch(() => ({})); throw new Error(body.error || `Historical data request failed (${marketResponse.status})`); }
        if (!alertsResponse.ok) throw new Error(`Historical alerts request failed (${alertsResponse.status})`);
        const marketData = await marketResponse.json();
        const alertData = await alertsResponse.json();
        if (!cancelled) {
          setHistoricalCandles(Array.isArray(marketData.candles) ? marketData.candles : []);
          setHistoricalAlerts(Array.isArray(alertData.alerts) ? alertData.alerts : []);
        }
      } catch (error) { if (!cancelled) setHistoryError(error.message || "Unable to load historical data."); }
      finally { if (!cancelled) setHistoryLoading(false); }
    };
    loadHistoricalData();
    return () => { cancelled = true; };
  }, [activePage, historySymbol, historyRange, historyInterval]);

  // Marketing Intelligence Alerts is intentionally session-only.
  // Historical MongoDB alerts are loaded only in the Historical Analysis page.

  const selectAlertFilter = setter => value => { setter(value); setAlertPage(1); };
  useEffect(() => {
    if (alertPage > liveAlertPages) setAlertPage(liveAlertPages);
  }, [alertPage, liveAlertPages]);
  const renderAlertMetrics = alert => {
    const volume = alert.type === "VOLUME_SPIKE";
    const observed = Number(alert.observedValue);
    const baseline = Number(alert.baselineValue);
    const change = alert.changePct;
    return <div className="alert-metrics">
      <AlertMetric label={volume ? "Volume observed" : "Price observed"} value={Number.isFinite(observed) ? (volume ? formatNumber(observed) : `$${formatPrice(observed)}`) : "—"} tone="observed" />
      <AlertMetric label="Baseline" value={Number.isFinite(baseline) ? (volume ? formatNumber(baseline) : `$${formatPrice(baseline)}`) : "—"} tone="baseline" />
      {change !== null && change !== undefined && <AlertMetric label="Change" value={`${Number(change) > 0 ? "+" : ""}${Number(change).toFixed(2)}%`} tone={Number(change) >= 0 ? "change-up" : "change-down"} />}
    </div>;
  };
  const alertTypeLabel = type => type === "PRICE_MOVEMENT" ? "PRICE MOVEMENT" : "VOLUME SPIKE";
  const severityClass = severity => String(severity || "low").toLowerCase();
  const renderAlertCard = (alert, compact = false) => (
    <article className={`alert-card ${severityClass(alert.severity)}`} key={alert.alertId || alert._id || `${alert.symbol}-${alert.timestamp}`}>
      <div className="alert-card-top">
        <div className="alert-identity">
          <AssetLogo symbol={alert.symbol} size={compact ? 27 : 32} />
          <div><strong>{alert.symbol}</strong><span>{alertTypeLabel(alert.type)}</span></div>
        </div>
        <span className={`severity ${severityClass(alert.severity)}`}>{alert.severity || "—"}</span>
      </div>
      <p className="alert-message">{alert.message}</p>
      {renderAlertMetrics(alert)}
      <div className="alert-time"><Clock3 size={13} />{alert.timestamp ? new Date(alert.timestamp).toLocaleString() : "—"}</div>
    </article>
  );

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-brand"><Activity size={29}/><span>MARKET<span>PULSE</span></span></div>
        <div className="sidebar-section-label">WORKSPACE</div>
        <nav className="side-nav" aria-label="Main navigation">
          <button className={activePage === "dashboard" ? "side-nav-link active" : "side-nav-link"} onClick={() => setActivePage("dashboard")}><LayoutDashboard size={19}/> <span>Dashboard</span></button>
          <button className={activePage === "history" ? "side-nav-link active" : "side-nav-link"} onClick={() => setActivePage("history")}><ChartNoAxesCombined size={19}/> <span>Historic Analysis</span></button>
          <button className={activePage === "alerts" ? "side-nav-link active" : "side-nav-link"} onClick={() => { setAlertPage(1); setActivePage("alerts"); }}><Bell size={19}/> <span>Marketing Intelligence Alerts</span></button>
        </nav>
        <button className="sidebar-status" onClick={() => setSystemStatusOpen(true)}>
          <span className={`status-pulse ${connected ? "online" : "offline"}`}/>
          <span className="sidebar-status-copy"><strong>System Status</strong><small>{connected ? "Live connection active" : "Checking connection"}</small></span>
          <span className="signal-bars"><i/><i/><i/></span>
        </button>
      </aside>

      <div className="main-shell">
        <header className="topbar">
          <div className="topbar-title"><span className="topbar-kicker">MARKET INTELLIGENCE</span><h1>{activePage === "dashboard" ? "Market Overview" : activePage === "history" ? "Historical Analysis" : "Market Intelligence Alerts"}</h1></div>
          <div className="topbar-right"><span className="exchange-pill"><Radio size={15}/> BINANCE SPOT</span><span className={`market-live-pill ${connected ? "connected" : ""}`}><i/> {connected ? "LIVE MARKET DATA" : "CONNECTING"}</span><button className="system-status-button" onClick={() => setSystemStatusOpen(true)}><Layers3 size={18}/> System Status</button></div>
        </header>

        {activePage === "dashboard" && <main className="page-content dashboard-page">
          <div className="page-intro"><div><p className="eyebrow">YOUR MARKET AT A GLANCE</p><h2>Good market day starts with good data.</h2><p>Real-time cryptocurrency prices, market activity and signals from Binance Spot.</p></div><span className="updated-chip"><span className="status-pulse online"/> Streaming market snapshots</span></div>

          <section className="asset-grid">
            {ASSETS.map(asset => {
              const data = market[asset.symbol];
              const points = history[asset.symbol] || [];
              const first = points[0]?.price;
              const last = points[points.length - 1]?.price;
              const rising = first && last ? last >= first : true;
              return <article className="asset-card" key={asset.symbol}>
                <div className="asset-card-head"><AssetLogo symbol={asset.symbol} size={34}/><div className="asset-title"><strong>{asset.name}</strong><span>{asset.short}/USDT</span></div><span className="asset-live">LIVE</span></div>
                <div className="asset-card-bottom"><div><strong className="asset-price">${formatPrice(data?.price)}</strong><small>{data?.trades10s ?? 0} trades / 10 sec</small></div><div className="sparkline"><MiniSparkline points={points}/></div></div>
              </article>;
            })}
          </section>

          <section className="dashboard-main-grid">
            <article className="panel chart-panel">
              <div className="panel-heading"><div className="heading-icon mint"><TrendingUp size={19}/></div><div><h2>Market Price Activity</h2><p>Live price movement · recent Binance snapshots</p></div><span className="panel-live-tag">LIVE</span></div>
              <div className="chart-controls">{ASSETS.map(asset => <button key={asset.symbol} className={selectedSymbol === asset.symbol ? "coin-tab selected" : "coin-tab"} onClick={() => setSelectedSymbol(asset.symbol)}><AssetLogo symbol={asset.symbol} size={19}/>{asset.short}</button>)}</div>
              <div className="chart-container dashboard-chart"><ResponsiveContainer width="100%" height="100%"><AreaChart data={history[selectedSymbol] || []} margin={{top: 8,right: 12,left: 0,bottom: 0}}><defs><linearGradient id="livePriceFill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#00e5a8" stopOpacity={0.32}/><stop offset="100%" stopColor="#00e5a8" stopOpacity={0.015}/></linearGradient></defs><CartesianGrid stroke="#23384b" strokeDasharray="3 5" vertical={true}/><XAxis dataKey="time" tickFormatter={value => new Date(value).toLocaleTimeString([], {hour:"2-digit",minute:"2-digit",second:undefined})} minTickGap={35} stroke="#8196ad" tick={{fontSize:10}}/><YAxis domain={["auto","auto"]} stroke="#8196ad" tick={{fontSize:10}} tickFormatter={value => Number(value).toLocaleString(undefined,{maximumFractionDigits:2})} width={66}/><Tooltip labelFormatter={value => new Date(value).toLocaleTimeString()} formatter={value => [`$${formatPrice(value)}`,`${selectedAsset.short} price`]} contentStyle={{background:"#0d1a29",border:"1px solid #31516a",borderRadius:10,color:"#edf7ff"}}/><Area type="monotone" dataKey="price" stroke="#00e5a8" strokeWidth={2.8} fill="url(#livePriceFill)" dot={false} activeDot={{r:4,fill:"#00e5a8",stroke:"#07131f",strokeWidth:2}} isAnimationActive={false}/></AreaChart></ResponsiveContainer>{(!history[selectedSymbol] || history[selectedSymbol].length < 2) && <div className="chart-waiting">Waiting for live price snapshots…</div>}</div>
              <div className="chart-footnote"><span>{selectedAsset.name} · {selectedAsset.short}/USDT</span><span>{(history[selectedSymbol] || []).length} live snapshots in chart</span></div>
            </article>

            <article className="panel latest-alerts-panel">
              <div className="panel-heading"><div className="heading-icon amber"><Bell size={19}/></div><div><h2>Latest Market Intelligence Alerts</h2><p>Most recent price and volume anomalies</p></div><span className="count-tag">{latestAlerts.length} ALERTS</span></div>
              <div className="latest-alerts-list">{latestAlerts.length ? latestAlerts.map(alert => renderAlertCard(alert, true)) : <div className="empty-state"><Bell size={25}/><strong>No alerts yet</strong><span>New market signals will appear here as they are detected.</span></div>}</div>
              <button className="text-link-button" onClick={() => {setAlertPage(1);setActivePage("alerts");}}>View all alerts <ArrowUpRight size={15}/></button>
            </article>
          </section>

          <section className="dashboard-bottom-grid">
            <article className="panel market-activity-panel"><div className="panel-heading"><div className="heading-icon violet"><Layers3 size={19}/></div><div><h2>Market Activity</h2><p>Rolling 10-second statistics</p></div></div><div className="activity-table-wrap"><table className="activity-table"><thead><tr><th>Asset</th><th>Trades (10s)</th><th>Volume</th></tr></thead><tbody>{ASSETS.map(asset => {const data=market[asset.symbol];return <tr key={asset.symbol}><td><span className="table-asset"><AssetLogo symbol={asset.symbol} size={27}/><span><strong>{asset.short}</strong><small>{asset.name}</small></span></span></td><td className="numeric-cell">{formatNumber(data?.trades10s)}</td><td className="numeric-cell">{formatNumber(data?.volume10s)}</td></tr>;})}</tbody></table></div></article>
            <article className="panel trade-feed-panel"><div className="panel-heading"><div className="heading-icon cyan"><Activity size={19}/></div><div><h2>Live Trade Feed</h2><p>Latest Binance trade events</p></div><span className="count-tag streaming">STREAMING</span></div><div className="trade-table-wrap"><table><thead><tr><th>Asset</th><th>Price (USDT)</th><th>Quantity</th><th>Trade time</th></tr></thead><tbody>{trades.slice(0,12).map((trade,index)=><tr key={`${trade.symbol}-${trade.tradeId}-${index}`}><td><strong>{trade.symbol}</strong></td><td className="numeric-cell">${formatPrice(Number(trade.price))}</td><td>{trade.quantity}</td><td>{trade.tradeTime ? new Date(trade.tradeTime).toLocaleTimeString() : "—"}</td></tr>)}{trades.length===0&&<tr><td colSpan="4" className="empty-cell">Waiting for live trade events…</td></tr>}</tbody></table></div></article>
          </section>
        </main>}

        {activePage === "history" && <main className="page-content historical-page">
          <div className="page-intro"><div><p className="eyebrow">MARKET DATA EXPLORER</p><h2>Historical Analysis</h2><p>Explore stored cryptocurrency price history and related market alerts.</p></div><span className="updated-chip"><Database size={15}/> MONGODB HISTORY</span></div>
          <section className="panel history-filter-panel"><div className="panel-heading"><div className="heading-icon mint"><ChartNoAxesCombined size={19}/></div><div><h2>Historical market data</h2><p>Choose an asset and period to investigate its past performance.</p></div></div><div className="history-controls"><label>Cryptocurrency<select value={historySymbol} onChange={event=>setHistorySymbol(event.target.value)}>{ASSETS.map(asset=><option key={asset.symbol} value={asset.symbol}>{asset.name} ({asset.short}/USDT)</option>)}</select></label><label>Time range<select value={historyRange} onChange={event=>setHistoryRange(event.target.value)}><option value="1h">Last hour</option><option value="3h">Last 3 hours</option></select></label><label>Candle interval<select value={historyInterval} onChange={event=>setHistoryInterval(event.target.value)}><option value="1m">1 minute</option><option value="5m">5 minutes</option><option value="15m">15 minutes</option><option value="1h">1 hour</option></select></label></div></section>
          <section className="panel history-chart-panel"><div className="panel-heading"><div className="heading-icon mint"><TrendingUp size={19}/></div><div><h2>{ASSETS.find(asset=>asset.symbol===historySymbol)?.name || historySymbol} price history</h2><p>Historical price movement from stored Binance market data</p></div><span className="panel-live-tag">HISTORICAL</span></div>
            {historyLoading?<p className="history-message">Loading historical market data…</p>:historyError?<p className="history-message history-error">{historyError}</p>:historicalCandles.length===0?<p className="history-message">No stored candles were found for this asset and time range.</p>:<><div className="history-summary"><span>{historicalCandles.length} candles</span><span>Latest close: ${Number(historicalCandles[historicalCandles.length-1].close).toLocaleString(undefined,{maximumFractionDigits:6})}</span><span>Total base volume: {formatNumber(historicalCandles.reduce((sum,candle)=>sum+Number(candle.volume||0),0))}</span></div><div className="historical-chart"><ResponsiveContainer width="100%" height="100%"><AreaChart data={historicalCandles}><defs><linearGradient id="historicalPriceFill" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#00e5a8" stopOpacity={0.3}/><stop offset="95%" stopColor="#00e5a8" stopOpacity={0}/></linearGradient></defs><CartesianGrid strokeDasharray="3 4" stroke="#23384b"/><XAxis dataKey="time" tickFormatter={value=>new Date(value).toLocaleTimeString()} minTickGap={35} stroke="#8196ad"/><YAxis domain={["auto","auto"]} stroke="#8196ad"/><Tooltip labelFormatter={value=>new Date(value).toLocaleString()} contentStyle={{background:"#0d1a29",border:"1px solid #31516a",borderRadius:10}}/><Area type="monotone" dataKey="close" name="Close price" stroke="#00e5a8" fill="url(#historicalPriceFill)" strokeWidth={2.4}/></AreaChart></ResponsiveContainer></div></>}
          </section>
          <section className="panel historical-alerts-panel">
            <div className="panel-heading">
              <div className="heading-icon amber"><Bell size={19}/></div>
              <div><h2>Historical Market Alerts</h2><p>MongoDB alerts for the selected cryptocurrency, shown in the same table format as live alerts.</p></div>
              <span className="count-tag">{historicalAlerts.length} ALERTS</span>
            </div>
            {historyLoading ? <p className="history-message">Loading historical alerts…</p> :
              historicalAlerts.length === 0 ? <p className="history-message">No stored alerts found for this asset.</p> : (
                <div className="alert-table-wrap">
                  <table className="alert-history-table">
                    <thead><tr><th>Time (UTC)</th><th>Symbol</th><th>Type</th><th>Message</th><th>Observed</th><th>Baseline</th><th>Change</th><th>Intensity</th></tr></thead>
                    <tbody>{historicalAlerts.map((alert, index) => {
                      const volume = alert.type === "VOLUME_SPIKE";
                      return <tr key={alert.alertId || alert._id || index}>
                        <td className="time-cell">{alert.timestamp ? new Date(alert.timestamp).toLocaleString() : "—"}</td>
                        <td><span className="table-asset compact"><AssetLogo symbol={alert.symbol} size={22}/><strong>{alert.symbol}</strong></span></td>
                        <td><span className={`type-label ${volume ? "volume" : "price"}`}>{alertTypeLabel(alert.type)}</span></td>
                        <td className="message-cell">{alert.message}</td>
                        <td className="numeric-cell">{volume ? formatNumber(alert.observedValue) : `$${formatPrice(alert.observedValue)}`}</td>
                        <td className="numeric-cell">{volume ? formatNumber(alert.baselineValue) : `$${formatPrice(alert.baselineValue)}`}</td>
                        <td className={`change-cell ${Number(alert.changePct) >= 0 ? "gain" : "loss"}`}>{alert.changePct === null || alert.changePct === undefined ? "—" : `${Number(alert.changePct) > 0 ? "+" : ""}${Number(alert.changePct).toFixed(2)}%`}</td>
                        <td><span className={`severity ${severityClass(alert.severity)}`}>{alert.severity}</span></td>
                      </tr>;
                    })}</tbody>
                  </table>
                </div>
              )}
          </section>
        </main>}

        {activePage === "alerts" && <main className="page-content alerts-page">
          <div className="page-intro"><div><p className="eyebrow">MARKET INTELLIGENCE</p><h2>Market Intelligence Alerts</h2><p>Alerts detected since this frontend session connected to the live MarketPulse stream.</p></div><span className="count-tag total-alert-count">{liveAlertTotal.toLocaleString()} SESSION ALERTS</span></div>
          <section className="panel alert-history-panel"><div className="panel-heading"><div className="heading-icon amber"><Bell size={19}/></div><div><h2>Live session alerts</h2><p>Only new alerts received while this backend session is running. Historical MongoDB records are shown under Historical Analysis.</p></div><button className="refresh-button" onClick={()=>setAlertPage(1)}><RefreshCw size={15}/> Reset view</button></div>
            <div className="alert-filters"><label>Asset<select value={alertSymbolFilter} onChange={event=>selectAlertFilter(setAlertSymbolFilter)(event.target.value)}><option value="ALL">All assets</option>{ASSETS.map(asset=><option key={asset.symbol} value={asset.symbol}>{asset.short}</option>)}</select></label><label>Type<select value={alertTypeFilter} onChange={event=>selectAlertFilter(setAlertTypeFilter)(event.target.value)}><option value="ALL">All types</option><option value="PRICE_MOVEMENT">Price movement</option><option value="VOLUME_SPIKE">Volume spike</option></select></label><label>Severity<select value={alertSeverityFilter} onChange={event=>selectAlertFilter(setAlertSeverityFilter)(event.target.value)}><option value="ALL">All severities</option><option value="HIGH">High</option><option value="MEDIUM">Medium</option><option value="LOW">Low</option></select></label></div>
            <div className="alert-table-wrap"><table className="alert-history-table"><thead><tr><th>Time (UTC)</th><th>Symbol</th><th>Type</th><th>Message</th><th>Observed</th><th>Baseline</th><th>Change</th><th>Intensity</th></tr></thead><tbody>{liveAlertRows.map((alert,index)=>{const volume=alert.type==="VOLUME_SPIKE";return <tr key={alert.alertId||alert.id||alert._id||index}><td className="time-cell">{alert.timestamp?new Date(alert.timestamp).toLocaleString():"—"}</td><td><span className="table-asset compact"><AssetLogo symbol={alert.symbol} size={22}/><strong>{alert.symbol}</strong></span></td><td><span className={`type-label ${volume?"volume":"price"}`}>{alertTypeLabel(alert.type)}</span></td><td className="message-cell">{alert.message}</td><td className="numeric-cell">{volume?formatNumber(alert.observedValue):`$${formatPrice(alert.observedValue)}`}</td><td className="numeric-cell">{volume?formatNumber(alert.baselineValue):`$${formatPrice(alert.baselineValue)}`}</td><td className={`change-cell ${Number(alert.changePct)>=0?"gain":"loss"}`}>{alert.changePct===null||alert.changePct===undefined?"—":`${Number(alert.changePct)>0?"+":""}${Number(alert.changePct).toFixed(2)}%`}</td><td><span className={`severity ${severityClass(alert.severity)}`}>{alert.severity}</span></td></tr>;})}{liveAlertRows.length===0&&<tr><td colSpan="8" className="empty-cell">No live alerts received in this session yet.</td></tr>}</tbody></table></div><div className="pagination"><span>Showing {liveAlertRows.length?((alertPage-1)*25+1):0}–{Math.min(alertPage*25,liveAlertTotal)} of {liveAlertTotal} session alerts</span><div><button disabled={alertPage<=1} onClick={()=>setAlertPage(page=>Math.max(1,page-1))}><ChevronLeft size={16}/> Previous</button><span>Page {alertPage} of {liveAlertPages}</span><button disabled={alertPage>=liveAlertPages} onClick={()=>setAlertPage(page=>Math.min(liveAlertPages,page+1))}>Next <ChevronRight size={16}/></button></div></div>
          </section>
        </main>}
        <footer className="app-footer"><span>MARKETPULSE © 2026 · REAL-TIME MARKET INTELLIGENCE</span><span>DATA SOURCE: BINANCE SPOT</span></footer>
      </div>

      {systemStatusOpen && <div className="status-backdrop" onClick={()=>setSystemStatusOpen(false)} role="presentation"><aside className="status-drawer" role="dialog" aria-modal="true" aria-labelledby="system-status-title" onClick={event=>event.stopPropagation()}><div className="status-drawer-head"><div><span className="eyebrow">INFRASTRUCTURE & DATA CONNECTIONS</span><h2 id="system-status-title"><Server size={20}/> System Status</h2></div><button className="icon-button" onClick={()=>setSystemStatusOpen(false)} aria-label="Close system status"><X size={19}/></button></div>{systemStatusLoading?<p className="history-message">Checking system health…</p>:systemInfo?.error?<p className="history-message history-error">Unable to retrieve system health: {systemInfo.error}</p>:systemInfo?<><div className="status-service-row"><span className="service-icon backend"><Server size={18}/></span><div><strong>Node.js Backend</strong><small>API server · Port 5000</small></div><span className="service-state online">ONLINE</span></div><div className="status-service-row"><span className="service-icon socket"><Wifi size={18}/></span><div><strong>Socket.IO</strong><small>Real-time data streaming</small></div><span className={`service-state ${connected?"online":"offline"}`}>{connected?"ONLINE":"OFFLINE"}</span></div><div className="status-service-row"><span className="service-icon database"><Database size={18}/></span><div><strong>MongoDB</strong><small>MarketPulseDB · Atlas</small></div><span className={`service-state ${systemInfo.database==="connected"?"online":"offline"}`}>{String(systemInfo.database||"unknown").toUpperCase()}</span></div><div className="status-service-row"><span className="service-icon clients"><Activity size={18}/></span><div><strong>Connected clients</strong><small>Active dashboard connections</small></div><strong className="service-count">{systemInfo.connectedClients??0}</strong></div><div className="status-extra"><div><span>Data retention</span><strong>{systemInfo.retentionHours??"—"} hours</strong></div><div><span>Storage mode</span><strong>{systemInfo.storageMode||"Live market storage"}</strong></div><div><span>Last checked</span><strong>{systemInfo.timestamp?new Date(systemInfo.timestamp).toLocaleTimeString():"—"}</strong></div></div></>:<p className="history-message">System health information is not available.</p>}<div className="status-drawer-actions"><button className="refresh-button" onClick={()=>{setSystemInfo(null);setSystemStatusOpen(false);window.setTimeout(()=>setSystemStatusOpen(true),0);}}><RefreshCw size={15}/> Refresh status</button><button className="primary-button" onClick={()=>setSystemStatusOpen(false)}>Done</button></div></aside></div>}
    </div>
  );
}

export default App;
