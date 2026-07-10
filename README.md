#### Crypto Intelligence Dashboard
<p align="center">
  <img src="https://img.shields.io/badge/Python-3.13-blue?style=for-the-badge&logo=python" />
  <img src="https://img.shields.io/badge/FastAPI-Backend-blue?style=for-the-badge&logo=fastapi" />
  <img src="https://img.shields.io/badge/React-Frontend-blue?style=for-the-badge&logo=react" />
  <img src="https://img.shields.io/badge/PostgreSQL-Database-blue?style=for-the-badge&logo=postgresql" />
  <img src="https://img.shields.io/badge/scikit--learn-Machine%20Learning-blue?style=for-the-badge&logo=scikitlearn" />
  <img src="https://img.shields.io/badge/Docker-Containerized-blue?style=for-the-badge&logo=docker" />
  <img src="https://img.shields.io/badge/CoinGecko-Live%20Data-blue?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Status-Production%20Style-blue?style=for-the-badge" />
</p>


A production-style full-stack cryptocurrency analytics platform that automatically ingests live market data, stores historical time-series in PostgreSQL while serveing advanced forecasting using machine learning-based models and presents an interactive React dashboard with technical charts and predictions.

### Project Overview

The Crypto Intelligence Dashboard is an end-to-end data engineering, backend, machine learning, and frontend project built to simulate a real-world financial analytics platform.
It solves the problem of transforming raw cryptocurrency market data into actionable intelligence by combining:

## Automated ETL Pipeline
- Fetches live cryptocurrency data from CoinGecko API
- Extracts price, market cap, volume, and timestamps
- Loads data into PostgreSQL
- Runs automatically every 5 minutes

## Historical Time-Series Database
- Stores every market snapshot
- Prevents duplicate inserts using unique constraints
- Indexed for fast historical queries

## FastAPI Backend
- Health monitoring endpoint
- Latest price API
- Historical price API
- OHLC candlestick API
- Forecast API
- Model status API

## Machine Learning Forecasting Engine
- Builds lag, rolling, return, volatility, volume, and market-cap features
- Supports multiple forecasting strategies
- Performs backtesting and validation
- Automatically selects the strongest recent model
- Stores reusable model artifacts

## Advanced Visualization
- Interactive candlestick charts
- OHLC tooltips
- Technical market metrics
- Prediction trend labels
- Confidence indicators

## Auto Refresh Frontend
- Refreshes dashboard data automatically every 30 seconds
- Updates prices, charts, and predictions without manual reload

## Fully Dockerized Deployment
- Frontend, backend, and scheduler run with one command
- Reproducible environment across systems
  
The system continuously fetches live market data from CoinGecko, stores it in PostgreSQL, trains predictive models, and displays real-time dashboards for investors and analysts.

### How to Run This Project on Your System
Prerequisites

Before running this project, install the following software:

- Docker Desktop
- PostgreSQL (with pgAdmin 4)
- Git

> **Note:** Docker is used to run the frontend, backend, and ETL scheduler.  
> **PostgreSQL is still required**, because the database is not containerized in this project and runs locally on your machine.

---

## 1. Clone the Repository

```bash
git clone https://github.com/hrittikahajari-pea/Crypto_Dashboard.git
cd Crypto_Dashboard
```
## 2. Create the PostgreSQL Database
- Open pgAdmin 4 and create a new database named: crypto_db
- Make sure PostgreSQL is running on port 5432.

## 3. Configure Environment Variables
- Copy the example environment file: copy .env.example .env
- Open .env and update the values:
DB_NAME=crypto_db
DB_USER=postgres
DB_PASSWORD=your_postgres_password
DB_HOST=host.docker.internal
DB_PORT=5432

- Replace your_postgres_password with your actual PostgreSQL password.
  
## 4. Start the Entire Application
- Make sure Docker Desktop is running, then execute:
```bash
docker compose up --build
```
This single command starts:
React frontend
FastAPI backend and
Automated ETL scheduler

## 5. Open the Application
- After the containers finish starting, open the following URLs manually in your browser:
  
Dashboard:
http://localhost:3000
API Documentation:
http://localhost:8000/docs

## Quick Start Summary
- git clone https://github.com/hrittikahajari-pea/Crypto_Dashboard.git
- cd Crypto_Dashboard
- copy .env.example .env
- docker compose up --build

### System Architecture

```
CoinGecko API -> Python ETL -> PostgreSQL Database -> Feature Engineering + Forecast Models -> FastAPI REST APIs -> React Frontend Dashboard -> User Visualization
