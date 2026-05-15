import { useEffect, useState } from "react";
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
                {prices.map((coin) => (
                  <tr key={coin.id}>
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

export default App;