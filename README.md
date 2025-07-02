# Crypto Trading Analysis & Tracking System

A comprehensive 24/7 cryptocurrency trading analysis and live tracking system that continuously monitors markets to detect volume and orderbook anomalies. The system combines real-time market data collection, orderbook analysis, and historical data analysis for automated trading strategy development and execution.

## 🏗️ Architecture Overview

The system consists of three main components:

1. **Tracker** - Live market data collection and orderbook analysis
2. **Backend** - API server for data access and analysis
3. **Analysis** - Local environment for strategy development and backtesting

## 🚀 Production Components

### Tracker Service (`/tracker`)

The tracker is the core component responsible for 24/7 live market data collection and real-time analysis, continuously monitoring for volume and orderbook anomalies.

#### Key Features:
- **24/7 Continuous Monitoring**: Non-stop market surveillance for volume and orderbook anomalies
- **Real-time Orderbook Tracking**: Monitors orderbook depth and order distribution patterns
- **Multi-Connection WebSocket Pooling**: Distributes orderbook monitoring across multiple WebSocket connections for scalability
- **Orderbook Analysis**: Real-time analysis of bid/ask order distribution and price levels
- **Anomaly Detection**: Automatic detection of volume spikes and orderbook distribution anomalies
- **Trading Event Detection**: Automatic detection of volatility events and trading opportunities
- **Volume Analysis**: Tracks buy/sell volume patterns across multiple timeframes (1m, 5m, 15m, 30m, 60m)
- **Live Trade Processing**: Receives and processes live trade information from backend APIs
- **Benchmark Comparison**: Compares current volume against daily-computed benchmark data for relative volume analysis
- **Daily Benchmark Computation**: Automatically computes volume averages and benchmarks at 23:59:30 daily
- **Exchange Info Management**: Updates instrument lists and trading pairs daily at 23:59:18
- **Performance Monitoring**: Tracks trading performance and updates metrics daily at 23:59:15
- **Risk Management**: Implements configurable risk parameters and loss limits

#### Main Components:
- `TrackerController.py` - Core tracking logic and volume calculations
- `BinanceController.py` - Binance API integration and exchange info management
- `TradingController.py` - Trading logic, order management, and performance monitoring
- `BenchmarkController.py` - Daily benchmark computation and volume average calculations
- `LoggingController.py` - Comprehensive logging and error tracking
- `wss-pool-order-book.py` - WebSocket orderbook pooling system for real-time orderbook analysis
- `main.py` - Primary tracking script that runs every minute
- `side-script.py` - Background script for daily maintenance tasks

#### Configuration:
- Environment variables control trading parameters, risk limits, and connection settings
- Risk management configuration stored in `/tracker/riskmanagement/riskmanagement.json`
- User configuration in `/tracker/user_configuration/userconfiguration.json`
- Trading live mode can be enabled/disabled per user in configuration
- Minimal notional values are automatically retrieved from exchange

### Backend Service (`/backend`)

FastAPI-based backend providing RESTful APIs for data access and analysis, including live trade information retrieval.

#### Key Features:
- **Live Trade Data APIs**: Real-time access to current market trades and volume data
- **Data Retrieval APIs**: Access to tracker, market, and benchmark data
- **Timeseries Analysis**: Historical data analysis with configurable timeframes
- **Orderbook Analysis**: Orderbook metadata and depth analysis
- **User Authentication**: Secure access control
- **Real-time Data Streaming**: WebSocket endpoints for live data

#### Main Components:
- `AnalysisController.py` - Core analysis and data retrieval logic
- `BinanceController.py` - Binance API integration
- `AuthController.py` - Authentication and authorization
- `CryptoController.py` - Cryptocurrency-specific operations

#### API Endpoints:
- `/api/tracker/data` - Tracker data retrieval
- `/api/market/data` - Market trade data and live trade information
- `/api/benchmark/info` - Daily benchmark and volume information
- `/api/timeseries` - Historical timeseries data
- `/api/orderbook` - Orderbook analysis data

## 📊 Analysis Environment (`/analysis/Analysis2024`)

Local development environment for strategy development, backtesting, and data analysis.

### Key Features:
- **Strategy Development**: Jupyter notebooks for strategy testing and optimization
- **Historical Analysis**: Comprehensive backtesting framework
- **Data Visualization**: Interactive charts and performance metrics
- **Event Detection**: Pattern recognition and signal generation
- **Risk Assessment**: Portfolio risk analysis and optimization

### Main Components:
- `Analysis_official.ipynb` - Main analysis notebook
- `analyze_volatility.py` - Volatility analysis script
- `Functions.py` - Core analysis functions
- `Helpers.py` - Utility functions and helpers
- `launch_strategy.py` - Strategy execution script

### Data Structure:
- `timeseries_json/` - Historical timeseries data
- `analysis_json/` - Analysis results and outputs
- `riskmanagement_json/` - Risk management configurations
- `benchmark_json/` - Benchmark data and metrics

## 🐳 Deployment

### Docker Compose Configuration

The system uses Docker Compose for containerized deployment with the following services:

```yaml
services:
  backend:     # FastAPI backend service
  tracker:     # Live data tracking service
  mongo:       # MongoDB database
  mongo-express: # MongoDB web interface
  nginx:       # Reverse proxy and SSL termination
  certbot:     # SSL certificate management
```

### Environment Variables

Key environment variables for configuration:

```bash
# Database Configuration
MONGO_USERNAME=your_username
MONGO_PASSWORD=your_password
MONGO_DB_URL=mongo
MONGO_PORT=27017

# Trading Configuration
COINS_TRADED=BTCUSDT,ETHUSDT
MAXIMUM_LOSS=100
PRODUCTION=true

# WebSocket Configuration
SETS_WSS_BACKEND=5
MAX_CONNECTION_LOST=3
CONNECTION_COUNT=10

# Analysis Configuration
ANALYSIS=true
CHECK_PERIOD_MINUTES=1
```

### Production Deployment

1. **Build and Start Services**:
   ```bash
   docker-compose up -d mongo mongo-express tracker backend nginx
   ```

2. **Monitor Services**:
   ```bash
   docker-compose logs -f tracker
   docker-compose logs -f backend
   ```

3. **Service Scripts**:
   - **Main Script**: Runs every minute for tracking statistics
   - **Side Script**: Runs background maintenance tasks daily at 23:59
   - **Trading Live Mode**: Can be enabled/disabled per user configuration

4. **Access Services**:
   - Backend API: `https://your-domain.com/api/`
   - MongoDB Express: `https://your-domain.com/mongo/`
   - Nginx: `https://your-domain.com/`

## 📈 Data Flow

### Live Data Collection:
1. **Backend** retrieves live trade information from external sources
2. **Tracker** receives live trade data from backend APIs
3. **WebSocket Orderbook Pooling** maintains multiple connections for 24/7 orderbook monitoring
4. **Volume Comparison** compares current volume against daily benchmark data
5. **Anomaly Detection** identifies volume spikes and orderbook distribution anomalies
6. **Orderbook Analysis** processes bid/ask order distribution and detects trading events
7. **Risk Management** evaluates positions and applies risk controls
8. **Data Storage** saves processed data to MongoDB collections

### Daily Maintenance Tasks (23:59):
- **23:59:15** - Performance monitoring and metrics update
- **23:59:18** - Exchange info update (instrument lists, trading pairs)
- **23:59:30** - Daily benchmark computation (volume averages)
- **00:00:00** - Coin list refresh and volume standings update

### Analysis Pipeline:
1. **Data Retrieval** - Fetch historical data from production database
2. **Event Detection** - Identify trading patterns and signals
3. **Strategy Testing** - Backtest strategies on historical data
4. **Performance Analysis** - Calculate metrics and generate reports
5. **Strategy Deployment** - Deploy validated strategies to production

## 🔧 Development Setup

### Local Development:

1. **Clone Repository**:
   ```bash
   git clone <repository-url>
   ```

2. **Configure Environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Build and Start Services**:
   ```bash
   docker-compose up -d --build mongo-express mongo backend tracker nginx
   ```

### Analysis Environment:

2. **Run Analysis**:
   - Open `Analysis_official.ipynb`
   - Execute cells for data analysis and strategy testing
   - Use `analyze_volatility.py` for volatility analysis
   - Use `launch_strategy.py`for orderbook analysis

## 📊 Key Metrics and Analysis

## 📈 Results

The project has demonstrated promising results in identifying trading events that exhibit significant pullback patterns after substantial losses. The system's anomaly detection capabilities have proven effective in:

- **Loss Recovery Patterns**: Identifying events where significant price drops are followed by substantial pullbacks
- **Volume Anomaly Correlation**: Detecting unusual volume spikes that precede recovery movements
- **Orderbook Distribution Signals**: Recognizing orderbook anomalies that indicate potential reversal points
- **Risk-Reward Optimization**: Finding trading opportunities with favorable risk-reward ratios during market corrections

The automated 24/7 monitoring system continuously scans for these patterns across multiple cryptocurrency pairs, providing real-time alerts for potential trading opportunities.

### Volume Analysis:
- **24/7 Volume Monitoring**: Continuous surveillance of trading volume across all monitored pairs
- **Relative Volume**: Current volume vs. daily benchmark (computed once per day)
- **Buy/Sell Ratio**: Buy volume percentage analysis
- **Volume Momentum**: 7-day volume trends
- **Benchmark Comparison**: Real-time volume comparison against historical benchmarks
- **Volume Standings**: Dynamic ranking of most traded coins based on volume
- **Best Coins Selection**: Automatic selection of top-performing coins for tracking
- **Anomaly Detection**: Automatic identification of unusual volume patterns and spikes

### Orderbook Analysis:
- **24/7 Orderbook Monitoring**: Continuous surveillance of orderbook depth and distribution
- **Order Distribution**: Bid/ask order concentration and distribution patterns
- **Price Levels**: Key support/resistance levels and jump price detection
- **Market Depth**: Liquidity analysis at different levels
- **Multi-Connection Pooling**: Scalable WebSocket connections for high-frequency orderbook monitoring
- **Anomaly Detection**: Automatic identification of orderbook distribution anomalies
- **Event Detection**: Automatic detection of volatility events and trading opportunities
- **Real-time Processing**: Continuous orderbook analysis with configurable update frequencies

### Trading Signals:
- **Volume Spikes**: Unusual volume activity detection
- **Price Patterns**: Technical pattern recognition and jump price level detection
- **Orderbook Events**: Bid/ask order distribution anomalies
- **Volatility Events**: Automatic detection of market volatility patterns
- **Risk Events**: High-risk market conditions
- **Trading Opportunities**: Real-time identification of potential buy/sell signals

## 🔒 Security and Risk Management

### Risk Controls:
- **Position Limits**: Maximum position sizes per coin
- **Loss Limits**: Maximum daily loss thresholds
- **Exposure Limits**: Maximum portfolio exposure
- **Order Limits**: Maximum order sizes and frequencies

### Data Security:
- **Authentication**: Secure API access control
- **Encryption**: SSL/TLS for data transmission
- **Backup**: Regular database backups
- **Monitoring**: Comprehensive logging and alerting

## 📝 API Documentation

### Core Endpoints:

#### Tracker Data
```http
GET /api/tracker/data?start=2024-01-01T00:00:00&end=2024-01-02T00:00:00
```

#### Market Data
```http
GET /api/market/data?start=2024-01-01T00:00:00&end=2024-01-02T00:00:00
```

#### Benchmark Information
```http
GET /api/benchmark/info
```

#### Timeseries Analysis
```http
POST /api/timeseries
Content-Type: application/json

{
  "event_key": "buy_vol_1m:0/vol_1m:75/timeframe:1440/lvl:200",
  "start_date": "2024-01-01T00:00:00",
  "end_date": "2024-01-02T00:00:00"
}
```

## 🤝 Contributing

- Contact: **Alberto Rainieri** - alberto.rainieri.lavoro@gmail.com


**Note**: This system shows promising results in detecting volume and orderbook anomalies, but has not yet achieved consistent profitability in live trading. After 2 years of development, the project demonstrates solid technical architecture and anomaly detection capabilities, though further optimization is needed for steady returns.

**Data Assets**: The system has collected extensive historical data including orderbook snapshots, volume patterns, and trading events across multiple cryptocurrency pairs. This rich dataset provides valuable insights for strategy development and backtesting.

**Open for Collaboration**: I'm actively seeking collaborators, contributors, or partners to help improve the system's performance. Whether you're interested in:
- Contributing code improvements
- Testing and validation
- Strategy optimization
- Commercial partnerships
- Data analysis and research

Please reach out: **Alberto Rainieri** - alberto.rainieri.lavoro@gmail.com