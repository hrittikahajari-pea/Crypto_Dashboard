import { useEffect, useState } from "react";
import { BrowserRouter, Routes, Route, useNavigate, useParams } from "react-router-dom";
import "./App.css";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from "recharts";

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
                    <tr key={coin.id} onClick={() => navigate(`/coin/${coin.coin_name}`)}>
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
function CoinDetailPage() {
  const { coinName } = useParams(); 
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const firstPrice = history.length > 0 ? history[0].price : null;
  const latestPrice = history.length > 0 ? history[history.length - 1].price : null;
  let trend = "Neutral";

if (firstPrice !== null && latestPrice !== null) {
  if (latestPrice > firstPrice) {
    trend = "Bullish";
  } else if (latestPrice < firstPrice) {
    trend = "Bearish";
  }
}

  useEffect(() => {
    async function fetchHistory() {
      try {
        const response = await fetch(
          `${API_BASE_URL}/prices/history/${coinName}`
        );

        const data = await response.json();
        setHistory(data);
      } catch (error) {
        console.error("Failed to fetch history:", error);
      } finally {
        setLoading(false);
      }
    }

    fetchHistory();
  }, [coinName]);

  return (
    <main className="dashboard">
      <section className="content-card">
        <button
          className="back-button"
          onClick={() => window.history.back()}
        >
          ← Back to Dashboard
        </button>

        <h1>{coinName}</h1>
        <p className={`trend-badge ${trend.toLowerCase()}`}>
  {trend} Trend
</p>
        {loading && <p>Loading historical data...</p>}

        {!loading && history.length === 0 && (
          <p>No historical data available.</p>
        )}

        {!loading && history.length > 0 && (
          <div className="chart-card">
  <h2>{coinName} Price Trend</h2>

  <ResponsiveContainer width="100%" height={420}>
    <LineChart data={history}>
      <CartesianGrid strokeDasharray="3 3" stroke="#334155" />

      <XAxis
        dataKey="market_timestamp"
        tickFormatter={(value) =>
          value ? new Date(value).toLocaleTimeString([], {
            hour: "2-digit",
            minute: "2-digit",
          }) : ""
        }
        stroke="#94a3b8"
      />

      <YAxis
        stroke="#94a3b8"
        domain={["auto", "auto"]}
        tickFormatter={(value) => `$${Number(value).toLocaleString()}`}
      />

      <Tooltip
        contentStyle={{
          backgroundColor: "#020617",
          border: "1px solid #334155",
          borderRadius: "10px",
          color: "#e5e7eb",
        }}
        labelFormatter={(value) =>
          value ? new Date(value).toLocaleString() : ""
        }
        formatter={(value) => [`$${Number(value).toLocaleString()}`, "Price"]}
      />

      <Line
        type="monotone"
        dataKey="price"
        stroke="#38bdf8"
        strokeWidth={3}
        dot={false}
        activeDot={{ r: 6 }}
      />
    </LineChart>
  </ResponsiveContainer>
</div>
        )}
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