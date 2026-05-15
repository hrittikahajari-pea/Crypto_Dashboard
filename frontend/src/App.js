import { useEffect, useState } from "react";
import {
  BrowserRouter,
  Routes,
  Route,
  useNavigate,
  useParams,
} from "react-router-dom";
import "./App.css";

const API_BASE_URL = "http://127.0.0.1:8000";

function formatCurrency(value) {
  if (value === null || value === undefined) return "N/A";

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: value < 1 ? 6 : 2,
  }).format(value);
}

function formatNumber(value) {
  if (value === null || value === undefined) return "N/A";

  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(value);
}

function formatDateTime(value) {
  if (!value) return "N/A";
  return new Date(value).toLocaleString();
}

function App() {
  const navigate = useNavigate();
  const [prices, setPrices] = useState([]);
  const [loading, setLoading] = useState(true);
  const [errorMessage, setErrorMessage] = useState("");

  useEffect(() => {
    async function fetchLatestPrices() {
      try {
        setErrorMessage("");

        const response = await fetch(`${API_BASE_URL}/prices/latest`);

        if (!response.ok) {
          throw new Error(`API request failed with status ${response.status}`);
        }

        const data = await response.json();
        setPrices(data);
      } catch (error) {
        console.error("Failed to fetch latest prices:", error);
        setErrorMessage(
          "Unable to load crypto prices. Please check if the FastAPI backend is running."
        );
      } finally {
        setLoading(false);
      }
    }

    fetchLatestPrices();

    const intervalId = setInterval(fetchLatestPrices, 30000);

    return () => clearInterval(intervalId);
  }, []);

  return (
    <main className="dashboard">
      <section className="hero">
        <div>
          <p className="eyebrow">Live Market Intelligence</p>
          <h1>Pea's Crypto Intelligence Dashboard</h1>
          <p className="subtitle">
            Real-time cryptocurrency snapshots powered by CoinGecko, PostgreSQL,
            FastAPI and React.
          </p>
        </div>

        <div className="status-card">
          <span className="status-dot"></span>
          <span>Backend API Connected</span>
        </div>
      </section>

      <section className="content-card">
        <div className="section-header">
          <div>
            <h2>Latest Crypto Prices</h2>
            <p>Most recent stored snapshot for each cryptocurrency.</p>
          </div>

          <span className="record-count">{prices.length} records</span>
        </div>

        {loading && <p className="info-text">Loading latest market data...</p>}

        {!loading && errorMessage && (
          <p className="error-text">{errorMessage}</p>
        )}

        {!loading && !errorMessage && (
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Coin</th>
                  <th>Price</th>
                  <th>Market Cap</th>
                  <th>Volume</th>
                  <th>Market Time</th>
                  <th>Ingested At</th>
                </tr>
              </thead>

              <tbody>
                {[...prices]
                  .sort((a, b) => b.price - a.price)
                  .map((coin) => (
                    <tr
                      key={coin.id}
                      onClick={() => navigate(`/coin/${coin.coin_name}`)}
                    >
                      <td className="coin-name">{coin.coin_name}</td>
                      <td>{formatCurrency(coin.price)}</td>
                      <td>{formatNumber(coin.market_cap)}</td>
                      <td>{formatNumber(coin.volume)}</td>
                      <td>{formatDateTime(coin.market_timestamp)}</td>
                      <td>{formatDateTime(coin.ingested_at)}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}
function CandleChart({ data }) {
  const [hoveredCandle, setHoveredCandle] = useState(null);

  if (!data || data.length === 0) return null;

  const width = 1000;
  const height = 330;

  const leftPadding = 95;
  const rightPadding = 35;
  const topPadding = 30;
  const bottomPadding = 45;

  const chartWidth = width - leftPadding - rightPadding;
  const chartHeight = height - topPadding - bottomPadding;

  const allPrices = data.flatMap((candle) => [
    candle.open,
    candle.high,
    candle.low,
    candle.close,
  ]);

  const rawMinPrice = Math.min(...allPrices);
  const rawMaxPrice = Math.max(...allPrices);
  const range = rawMaxPrice - rawMinPrice || 1;

  const paddedMinPrice = rawMinPrice - range * 0.04;
  const paddedMaxPrice = rawMaxPrice + range * 0.04;
  const paddedRange = paddedMaxPrice - paddedMinPrice || 1;

  const priceToY = (price) =>
    topPadding + ((paddedMaxPrice - price) / paddedRange) * chartHeight;

  const gap = chartWidth / Math.max(data.length, 1);
  const candleWidth = Math.max(24, Math.min(48, gap * 0.55));
  const priceTicks = 5;

  return (
    <div className="chart-card">
      <div className="chart-header">
        <div>
          <h2>OHLC Candlestick Trend</h2>
          <p>Hover over each candle to inspect open, high, low and close.</p>
        </div>

        <div className="ohlc-tooltip fixed-tooltip">
          {hoveredCandle ? (
            <>
              <strong>{formatDateTime(hoveredCandle.time)}</strong>
              <span>Open: {formatCurrency(hoveredCandle.open)}</span>
              <span>High: {formatCurrency(hoveredCandle.high)}</span>
              <span>Low: {formatCurrency(hoveredCandle.low)}</span>
              <span>Close: {formatCurrency(hoveredCandle.close)}</span>
              <span>
                Candle:{" "}
                {hoveredCandle.close >= hoveredCandle.open
                  ? "Bullish"
                  : "Bearish"}
              </span>
            </>
          ) : (
            <>
              <strong>OHLC Details</strong>
              <span>Move cursor over a candle</span>
            </>
          )}
        </div>
      </div>

      <svg
        viewBox={`0 0 ${width} ${height}`}
        className="candlestick-chart"
        onMouseLeave={() => setHoveredCandle(null)}
      >
        {[...Array(priceTicks)].map((_, index) => {
          const value =
            paddedMinPrice +
            ((paddedMaxPrice - paddedMinPrice) / (priceTicks - 1)) * index;

          const y = priceToY(value);

          return (
            <g key={value}>
              <line
                x1={leftPadding}
                y1={y}
                x2={width - rightPadding}
                y2={y}
                stroke="#243244"
                strokeDasharray="5 5"
              />

              <text
                x={leftPadding - 16}
                y={y + 5}
                textAnchor="end"
                fill="#93c5fd"
                fontSize="14"
              >
                {formatCurrency(value)}
              </text>
            </g>
          );
        })}

        <line
          x1={leftPadding}
          y1={topPadding}
          x2={leftPadding}
          y2={height - bottomPadding}
          stroke="#64748b"
          strokeWidth="1.5"
        />

        <line
          x1={leftPadding}
          y1={height - bottomPadding}
          x2={width - rightPadding}
          y2={height - bottomPadding}
          stroke="#64748b"
          strokeWidth="1.5"
        />

        {data.map((candle, index) => {
          const x = leftPadding + index * gap + gap / 2;

          const openY = priceToY(candle.open);
          const closeY = priceToY(candle.close);
          const highY = priceToY(candle.high);
          const lowY = priceToY(candle.low);

          const isBullish = candle.close >= candle.open;
          const candleColor = isBullish ? "#22c55e" : "#ef4444";

          return (
            <g key={`${candle.time}-${index}`}>
              <rect
                x={x - gap / 2}
                y={topPadding}
                width={gap}
                height={chartHeight}
                fill="transparent"
                pointerEvents="all"
                onMouseMove={() => {
                  if (hoveredCandle?.time !== candle.time) {
                    setHoveredCandle(candle);
                  }
                }}
              />

              <line
                x1={x}
                y1={highY}
                x2={x}
                y2={lowY}
                stroke={candleColor}
                strokeWidth="4"
                strokeLinecap="round"
                pointerEvents="none"
              />

              <rect
                x={x - candleWidth / 2}
                y={Math.min(openY, closeY)}
                width={candleWidth}
                height={Math.max(Math.abs(closeY - openY), 8)}
                fill={candleColor}
                rx="5"
                pointerEvents="none"
              />

              <text
                x={x}
                y={height - bottomPadding + 28}
                textAnchor="middle"
                fill="#93c5fd"
                fontSize="13"
                pointerEvents="none"
              >
                {new Date(candle.time).toLocaleTimeString([], {
                  hour: "2-digit",
                  minute: "2-digit",
                })}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}
function CoinDetailPage() {
  const { coinName } = useParams();

  const [ohlcData, setOhlcData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedPeriod, setSelectedPeriod] = useState("24h");

  useEffect(() => {
    async function fetchOhlcData() {
      try {
        setLoading(true);

        const response = await fetch(
          `${API_BASE_URL}/prices/ohlc/${coinName}?interval_minutes=30&period=${selectedPeriod}`
        );

        if (!response.ok) {
          throw new Error(`OHLC request failed with status ${response.status}`);
        }

        const data = await response.json();
        setOhlcData(data);
      } catch (error) {
        console.error("Failed to fetch OHLC data:", error);
      } finally {
        setLoading(false);
      }
    }

    fetchOhlcData();
  }, [coinName, selectedPeriod]);
function calculateStats(data) {
  if (!data || data.length === 0) {
    return null;
  }

  const latestCandle = data[data.length - 1];

  const periodHigh = Math.max(...data.map((candle) => candle.high));
  const periodLow = Math.min(...data.map((candle) => candle.low));

  const averageClose =
    data.reduce((sum, candle) => sum + candle.close, 0) / data.length;
  const changePercent =
  ((latestCandle.close - data[0].open) / data[0].open) * 100;

let trend = "Neutral";

if (changePercent > 0.5) {
  trend = "Bullish";
} else if (changePercent < -0.5) {
  trend = "Bearish";
}

  return {
    currentPrice: latestCandle.close,
    periodHigh,
    periodLow,
    averageClose,
    candleCount: data.length,
    changePercent,
    trend,
  };
}
const stats = calculateStats(ohlcData);
  return (
    <main className="dashboard">
      <section className="content-card">
        <button className="back-button" onClick={() => window.history.back()}>
          ← Back to Dashboard
        </button>

        <div className="coin-detail-header">
          <div>
            <h1>{coinName}</h1>
            <p className="coin-detail-subtitle">
              Historical OHLC candlestick chart
            </p>
          </div>

          <div className="period-buttons">
            <button
              className={selectedPeriod === "24h" ? "active-period" : ""}
              onClick={() => setSelectedPeriod("24h")}
            >
              24H
            </button>

            <button
              className={selectedPeriod === "7d" ? "active-period" : ""}
              onClick={() => setSelectedPeriod("7d")}
            >
              7D
            </button>

            <button
              className={selectedPeriod === "30d" ? "active-period" : ""}
              onClick={() => setSelectedPeriod("30d")}
            >
              30D
            </button>
          </div>
        </div>
        {stats && (
  <div className="stats-grid">
    <div className="stat-card">
      <span>Current Price</span>
      <strong>{formatCurrency(stats.currentPrice)}</strong>
    </div>

    <div className="stat-card">
      <span>Period High</span>
      <strong>{formatCurrency(stats.periodHigh)}</strong>
    </div>

    <div className="stat-card">
      <span>Period Low</span>
      <strong>{formatCurrency(stats.periodLow)}</strong>
    </div>

    <div className="stat-card">
      <span>Average Close</span>
      <strong>{formatCurrency(stats.averageClose)}</strong>
    </div>

    <div className="stat-card">
      <span>Candles</span>
      <strong>{stats.candleCount}</strong>
    </div>
    <div className={`stat-card trend-${stats.trend.toLowerCase()}`}>
  <span>Trend</span>
  <strong>{stats.trend}</strong>
  <small>{stats.changePercent.toFixed(2)}%</small>
</div>
  </div>
)}
        {loading && <p>Loading OHLC candle data...</p>}

        {!loading && ohlcData.length === 0 && (
          <p>No OHLC candle data available for this period.</p>
        )}

        {!loading && ohlcData.length > 0 && <CandleChart data={ohlcData} />}
      </section>
    </main>
  );
}
function Root() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<App />} />
        <Route path="/coin/:coinName" element={<CoinDetailPage />} />
      </Routes>
    </BrowserRouter>
  );
}

export default Root;