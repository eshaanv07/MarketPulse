# MarketPulse — Real-Time Cryptocurrency Market Intelligence

MarketPulse is a real-time cryptocurrency market monitoring dashboard built with the MERN stack. It consumes live market data from Binance Spot, streams updates to connected browser clients with Socket.IO, visualizes market activity, and detects unusual price movements and volume spikes.

The application currently monitors **BTCUSDT, ETHUSDT, SOLUSDT, and XRPUSDT**. It is intended for market observation and educational/demo use; it does not place trades or provide investment advice.

---

## Contents

- [Features](#features)
- [How it works](#how-it-works)
- [Technology stack](#technology-stack)
- [Project structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Getting started](#getting-started)
- [Environment configuration](#environment-configuration)
- [Running the application](#running-the-application)
- [Market intelligence and alert logic](#market-intelligence-and-alert-logic)
- [Data storage](#data-storage)
- [Real-time events](#real-time-events)
- [Troubleshooting](#troubleshooting)
- [Security notes](#security-notes)
- [Limitations and future improvements](#limitations-and-future-improvements)

---

## Features

### Live market dashboard
- Live price monitoring for BTC, ETH, SOL, and XRP.
- Cryptocurrency cards with compact price charts.
- Main market chart and market activity information.
- Live trade activity feed.
- Connection/system status information.

### Market intelligence alerts
- Detects significant price movements.
- Detects abnormal trading-volume spikes.
- Assigns alert types and severity/intensity.
- Shows newly generated alerts separately from historical alerts.
- Displays alert details such as symbol, observed value, baseline, change, and time where applicable.

### Historical analytics
- Retrieves persisted historical market data and alerts.
- Displays historical alerts in a structured table.
- Uses stored candle data for historical market analysis.

### Real-time updates
- Uses Binance Spot WebSocket streams for market data.
- Uses Socket.IO to deliver updates from the backend to browser clients.
- Supports multiple connected dashboard clients through server-side broadcasts.

---

## How it works

At a high level, the data flow is:

```text
Binance Spot WebSocket
        |
        v
Node.js / Express backend (index.js)
        |
        +----> Trade and ticker state maintained in memory
        |
        +----> Market analytics (analytics.js)
        |          |
        |          +----> Price movement alerts
        |          |
        |          +----> Volume spike alerts
        |
        +----> MongoDB
        |          |
        |          +----> Alert records
        |          +----> Finalized candle records
        |
        +----> Socket.IO events
                   |
                   v
             React dashboard
             (live cards, charts,
              trade feed and alerts)
```

1. The backend connects to Binance Spot WebSocket streams for trade and one-minute kline updates.
2. Incoming market events update the backend's in-memory market state.
3. The analytics module evaluates market activity and generates alerts when configured conditions are met.
4. Alert records and finalized candle data are persisted in MongoDB.
5. Socket.IO broadcasts live market updates and newly generated alerts to connected clients.
6. The React frontend renders the live dashboard and requests historical information from the backend.

**Live alerts and historical alerts are different views:** the live alert area is intended to show alerts generated while the backend is running, while historical analytics displays records retrieved from MongoDB.

---

## Technology stack

| Layer | Technology |
|---|---|
| Frontend | React, Vite, JavaScript, CSS |
| Backend | Node.js, Express |
| Real-time transport | Socket.IO |
| Market data | Binance Spot WebSocket API |
| Database | MongoDB, Mongoose |
| Charts | Frontend charting components/libraries used by the project |

---

## Project structure

The main application is organized approximately as follows:

```text
MarketPulse/
├── index.js                 # Express server, Binance streams, Socket.IO and API logic
├── analytics.js             # Price movement and volume-spike detection
├── models/
│   ├── Alert.js             # MongoDB alert schema
│   ├── Candle.js            # MongoDB candle schema
│   └── Trade.js             # Trade schema
├── client/
│   ├── src/
│   │   ├── App.jsx          # Main React application and dashboard views
│   │   ├── App.css          # Application/dashboard styling
│   │   └── index.css        # Global styling
│   ├── package.json         # Frontend dependencies and scripts
│   └── ...
├── package.json             # Backend dependencies and scripts
├── .env                     # Local environment variables (do not commit)
└── README.md
```

If your local folder names differ slightly, use the actual paths in your checkout as the source of truth.

---

## Prerequisites

Install the following before running the project:

- **Node.js** (a current LTS release is recommended).
- **npm**, which is included with Node.js.
- **MongoDB**: either a local MongoDB server or a MongoDB Atlas cluster.
- Internet access to reach Binance Spot WebSocket endpoints.
- A modern browser.

You do not need Binance API keys for the public market-data streams used by this project. You do need a valid MongoDB connection string for persistence.

---

## Getting started

### 1. Clone the repository

```bash
git clone https://github.com/eshaanv07/MarketPulse.git
cd MarketPulse
```

### 2. Install backend dependencies

From the project root:

```bash
npm install
```

### 3. Install frontend dependencies

```bash
cd client
npm install
cd ..
```

### 4. Configure environment variables

Create a `.env` file in the project root. At minimum, configure the MongoDB connection string:

```env
MONGODB_URI=mongodb://127.0.0.1:27017/marketpulse
```

For MongoDB Atlas, use the connection string supplied by Atlas instead:

```env
MONGODB_URI=mongodb+srv://<username>:<password>@<cluster-host>/<database-name>
```

If the backend reads a `PORT` environment variable, you may also set it:

```env
PORT=5000
```

Use the variable names expected by the current `index.js` in your checkout. Do not add credentials to source files or commit your real `.env` file.

### 5. Start MongoDB

If using a local MongoDB installation, make sure the MongoDB service is running before starting the backend. If using Atlas, confirm that your IP address is allowed in the Atlas network access settings and that the database user has the required permissions.

---

## Running the application

Open **two terminals** from the project root.

### Terminal 1 — Backend

```bash
node index.js
```

The backend should connect to MongoDB and Binance, then start its HTTP and Socket.IO services. Check the terminal output for connection errors.

### Terminal 2 — Frontend

```bash
cd client
npm run dev
```

Vite prints the local frontend URL in the terminal (commonly `http://localhost:5173`). Open the exact URL shown by Vite.

### Create a production frontend build

From the `client` directory:

```bash
npm run build
```

To preview a production build locally, use the preview script if it is present in `client/package.json`:

```bash
npm run preview
```

The backend and frontend are run separately during local development unless you have configured a production serving/reverse-proxy setup.

### Windows PowerShell: if npm commands are blocked or installation fails

If PowerShell reports that running `npm.ps1` is disabled by the execution policy, run this in the **same PowerShell window**:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

Then retry the command that failed. This bypass applies only to the current PowerShell process/window; it does not permanently change the machine-wide policy.

If dependency installation fails because of peer-dependency conflicts, try the following separately:

```powershell
# From the project root: install backend dependencies
npm install --legacy-peer-deps

# Install frontend dependencies
cd client
npm install --legacy-peer-deps
```

If the frontend still does not start with `npm run dev`, try invoking Vite directly:

```powershell
cd client
npx vite --host 0.0.0.0
```

If the backend does not start, run it directly from the project root and read the first error printed in the terminal:

```powershell
cd ..
node index.js
```

If installation remains broken after these attempts, remove the affected `node_modules` folder and its lock file only if you are prepared to reinstall dependencies, then run `npm install` again. Do not delete your `.env` file or source files.

---

## Market intelligence and alert logic

MarketPulse currently includes two main anomaly categories.

### 1. Price movement

The analytics module compares a recent price with a reference price from the configured lookback window.

The percentage movement is calculated as:

```text
Change (%) = ((Current Price - Reference Price) / Reference Price) * 100
```

The sign indicates direction:
- Positive: price increased.
- Negative: price decreased.

The current project configuration uses:

| Setting | Current value |
|---|---:|
| Price detection window | 20 seconds |
| Price movement threshold | 0.5% |
| Price alert cooldown | 20 seconds |
| High-severity threshold | 1.0% |

A price alert is eligible when the absolute percentage movement reaches the configured threshold. The cooldown prevents repeated alerts for the same symbol from being emitted too frequently. The detection window and cooldown serve different purposes: the window controls the comparison period; the cooldown controls alert frequency.

### 2. Volume spike

The volume detector compares recent trading volume with a historical/bucketed baseline and configured multiplier/deviation conditions. It is designed to flag unusually high activity rather than ordinary changes in price.

The exact baseline and trigger depend on the analytics configuration in `analytics.js` and the values passed by `index.js`. Review those settings before changing detection sensitivity.

### Understanding alert fields

| Field | Meaning |
|---|---|
| Symbol | Cryptocurrency market pair, such as BTCUSDT |
| Type | Detected condition, such as price movement or volume spike |
| Change | Percentage price movement, when applicable |
| Price observed | Price measured when the alert was generated |
| Volume observed | Volume measured for the relevant interval/bucket |
| Baseline | Reference value used by the detector for comparison |
| Intensity/severity | Alert priority derived from the configured detection rules |
| Time | Time at which the alert was generated/recorded |

A volume alert's observed volume and baseline are volume measurements; they should not be interpreted as prices. A price alert's percentage change is not the same thing as the absolute price.

> Alert thresholds are configurable detection rules, not guarantees of market direction or trading signals.

---

## Data storage

MongoDB is used for persisted historical information.

- **Alerts:** generated alert records are stored for historical analysis.
- **Candles:** finalized one-minute candle records are stored for historical charting/analytics.
- **Trades:** a Trade model exists in the project; the live trade-processing path maintains and broadcasts trade activity, but individual live trades should not be assumed to be persisted unless the relevant persistence logic is enabled in your version.

The current schemas use a **three-hour TTL retention policy** for the applicable stored alert/candle records. MongoDB TTL deletion is asynchronous, so expired records may remain visible briefly after their expiration time.

Live in-memory state is separate from persisted historical records. Restarting the backend resets its in-memory live state; MongoDB-backed historical records remain subject to their retention policy.

---

## Real-time events

The backend uses Socket.IO to publish updates to connected frontend clients. The frontend subscribes to these event names:

| Socket.IO event | Purpose |
|---|---|
| `market:snapshot` | Current market snapshot/state |
| `market:trades` | Live trade updates |
| `market:alerts` | Newly generated live alerts |
| `market:alerts:history` | Historical alert data |

These event names are useful when debugging the frontend/backend connection. If the dashboard does not update, check that the backend is running, the browser can reach the backend Socket.IO endpoint, and the browser console has no connection or CORS errors.

---

## Troubleshooting

### Backend cannot connect to MongoDB
- Confirm MongoDB is running.
- Check `MONGODB_URI` in the root `.env`.
- For Atlas, verify the database username/password and network access allowlist.
- Make sure special characters in the password are URL-encoded in the connection string.

### Binance data is not appearing
- Confirm internet connectivity.
- Check backend logs for WebSocket connection or reconnect messages.
- Confirm the Binance public WebSocket service is reachable from your network.
- Remember that the dashboard depends on live market activity; an empty live view immediately after startup may simply mean the stream has not delivered data yet.

### Frontend loads but live values do not change
- Start the backend separately with `node index.js`.
- Check the browser developer console and Network tab for Socket.IO connection failures.
- Confirm the frontend's configured backend URL/port matches the backend's actual listening address.
- Check backend logs for Binance and Socket.IO errors.

### `npm run dev` or `npm run build` fails
- Run `npm install` inside `client`.
- Confirm the Node.js version is compatible with the installed dependencies.
- If dependencies were copied from another operating system, remove `client/node_modules` and reinstall them on the current machine.

### No price movement alerts appear
- The movement must reach the configured threshold within the detection window.
- The per-symbol cooldown may temporarily suppress another alert.
- A 20-second lookback can be sensitive to market conditions; a symbol may not move by 0.5% in every 20-second period.

### Historical data is empty
- Historical data appears only after the backend has received and persisted data.
- Check MongoDB connectivity and backend logs.
- TTL retention removes expired records after the configured retention period.

---

## Security notes

- **Never commit `.env` or real MongoDB credentials.**
- Keep `.env` in `.gitignore`.
- Provide a `.env.example` with placeholder values when sharing the project.
- If credentials were accidentally included in a shared ZIP or public repository, rotate them.
- Do not expose database credentials or private service configuration in frontend code.
- This project is a monitoring/analytics demo and should not be treated as a production trading or financial-advice system without additional security, reliability, and compliance work.

---

## Limitations and future improvements

Potential next steps for a production-oriented deployment include:

- Add automated unit and integration tests for analytics, APIs, and Socket.IO flows.
- Add API rate limiting and stronger request validation.
- Add Socket.IO rooms/subscriptions for symbol-specific delivery when needed.
- Add explicit duplicate and out-of-order trade handling.
- Measure throughput, event latency, memory use, and frontend rendering performance under load.
- Add authentication and authorization if the dashboard or APIs become restricted.
- Add deployment instructions, monitoring, and structured logging.

These are improvement opportunities, not claims that the current version already implements them.

---

## Disclaimer

MarketPulse is provided for educational, demonstration, and market-monitoring purposes only. It is not a broker, exchange, investment adviser, or trading system. Market data may be delayed, interrupted, incomplete, or inaccurate. Always independently verify information before making financial decisions.

---

## Author

**Eshaan V.**  
GitHub: [eshaanv07](https://github.com/eshaanv07)  
Repository: [MarketPulse](https://github.com/eshaanv07/MarketPulse)
