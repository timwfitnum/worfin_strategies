# Quantitative Trading System

## 🚀 Institutional-Grade Algorithmic Trading Platform

A professional quantitative trading system implementing 12 uncorrelated strategies across multiple asset classes (equities, options, futures, crypto) designed for $100K-$1M capital deployment. Built with the same standards and practices used at elite trading firms like Jane Street, Citadel, and Two Sigma.

### 📊 Key Features

- **12 Diverse Trading Strategies** across momentum, mean reversion, volatility, and event-driven approaches
- **Institutional Risk Management** with VaR, Expected Shortfall, and regime detection
- **Smart Execution Engine** with VWAP, TWAP, POV, and adaptive algorithms
- **Machine Learning Integration** using XGBoost, LightGBM, and deep learning
- **Real-time Monitoring** with Grafana dashboards and Prometheus metrics
- **Comprehensive Backtesting** with CPCV validation and walk-forward analysis
- **Multi-broker Support** for Interactive Brokers, Alpaca, and Binance

### 📈 Performance Targets

- **Portfolio Sharpe Ratio**: 1.4-1.8
- **Annual Returns**: 15-25% after costs
- **Maximum Drawdown**: 20%
- **Strategy Correlation**: < 0.7

## 🛠️ Technology Stack

- **Languages**: Python 3.11+
- **Core Libraries**: Pandas, NumPy, SciPy, scikit-learn
- **ML Frameworks**: XGBoost, LightGBM, PyTorch
- **Databases**: TimescaleDB, ClickHouse, Redis
- **Monitoring**: Grafana, Prometheus
- **Deployment**: Docker, Kubernetes, AWS/GCP

## 📦 Installation

### Prerequisites

- Python 3.11 or higher
- Docker and Docker Compose
- PostgreSQL/TimescaleDB
- Redis
- Git

### Quick Start

1. **Clone the repository**

```bash
git clone https://github.com/yourorg/quant-trading-system.git
cd quant-trading-system
```

2. **Set up environment variables**

```bash
cp .env.example .env
# Edit .env with your configuration
```

3. **Install dependencies**

```bash
make install-dev
```

4. **Start Docker services**

```bash
make docker-up
```

5. **Initialize database**

```bash
make setup-db
```

6. **Run tests**

```bash
make test
```

7. **Start the application**

```bash
make run
```

## 🏗️ Project Structure

```
quant-trading-system/
├── src/
│   ├── core/               # Core components (BaseStrategy, RiskManager, ExecutionEngine)
│   ├── strategies/         # Trading strategy implementations
│   │   ├── momentum/       # Momentum-based strategies
│   │   ├── arbitrage/      # Statistical arbitrage strategies
│   │   ├── volatility/     # Options and volatility strategies
│   │   └── event_driven/   # Event and structural strategies
│   ├── data/              # Data loaders and providers
│   ├── backtesting/       # Backtesting framework
│   ├── ml/                # Machine learning models
│   ├── execution/         # Order execution and routing
│   ├── monitoring/        # Monitoring and alerting
│   ├── api/               # REST API and WebSocket servers
│   └── utils/             # Utilities and helpers
├── tests/                 # Test suites
├── notebooks/             # Jupyter notebooks for research
├── configs/               # Configuration files
├── docker/                # Docker configurations
└── docs/                  # Documentation
```

## 🎯 The 12 Trading Strategies

### Momentum & Trend Following

1. **ML-Enhanced Momentum Factor** - XGBoost/LightGBM factor timing
2. **Cross-Asset Momentum** - Risk parity across asset classes
3. **Smart Beta Factor Rotation** - Regime-based factor allocation
4. **Sector Rotation Options** - ETF options momentum plays

### Arbitrage & Mean Reversion

5. **Crypto Statistical Arbitrage** - Pairs trading across exchanges
6. **ADR Geographic Arbitrage** - Cross-listing inefficiencies
7. **Crypto Funding Rate Arbitrage** - Perpetual vs spot basis

### Volatility & Options

8. **Variance Risk Premium** - Iron condors on indices
9. **VIX Calendar Spreads** - Term structure trading
10. **Earnings Volatility** - Pre/post earnings compression

### Event & Structural

11. **Overnight Premium** - Close-to-open systematic
12. **Merger Arbitrage** - M&A spread trading

## 🧪 Testing

Run the comprehensive test suite:

```bash
# All tests
make test

# Unit tests only
make test-unit

# Integration tests
make test-integration

# Performance benchmarks
make test-performance
```

## 📊 Monitoring

Access the monitoring dashboards:

- **Grafana**: <http://localhost:3000> (admin/admin123)
- **Prometheus**: <http://localhost:9090>
- **API Documentation**: <http://localhost:8000/docs>

## 🚀 Deployment

### Development

```bash
make run
```

### Production with Docker

```bash
docker-compose -f docker-compose.prod.yml up -d
```

### Cloud Deployment (AWS/GCP)

```bash
make deploy-production
```

## 📈 Backtesting

Run backtests with proper validation:

```python
from src.backtesting import BacktestEngine
from src.strategies.momentum import MLMomentumStrategy

engine = BacktestEngine(
    initial_capital=100000,
    start_date="2020-01-01",
    end_date="2023-12-31"
)

strategy = MLMomentumStrategy(
    symbols=["AAPL", "GOOGL", "MSFT"],
    capital=100000
)

results = engine.run(
    strategy,
    validation_method="CPCV",  # Combinatorial Purged Cross-Validation
    walk_forward_splits=5
)

print(results.summary())
```

## 🔒 Risk Management

The system implements institutional-grade risk controls:

- **Position Limits**: 2% maximum per position
- **Portfolio VaR**: 2.5% daily limit at 95% confidence
- **Drawdown Control**: 20% maximum drawdown trigger
- **Correlation Monitoring**: 0.7 maximum correlation between strategies
- **Regime Detection**: Automatic adjustment for market conditions
- **Kill Switches**: Manual and automatic circuit breakers

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## ⚠️ Disclaimer

This software is for educational and research purposes only. Trading involves substantial risk of loss and is not suitable for everyone. Past performance does not guarantee future results. Always conduct your own research and consult with financial advisors before making investment decisions.

## 🙏 Acknowledgments

- Inspired by strategies and practices from Jane Street, Citadel, Two Sigma, and Renaissance Technologies
- Built on the shoulders of giants: NumPy, Pandas, scikit-learn, and the Python data science ecosystem
- Special thanks to the quantitative finance research community

## 📚 Resources

- [Documentation](https://quant-trading-system.readthedocs.io/)
- [API Reference](https://api.quanttrading.com/docs)
- [Strategy White Papers](docs/strategies/)
- [Risk Management Framework](docs/risk_management.md)

## 📧 Contact

- Website: [quanttrading.com](https://quanttrading.com)
- Email: <team@quanttrading.com>
- Discord: [Join our community](https://discord.gg/quanttrading)

---

**Built with ❤️ for the quantitative trading community**
