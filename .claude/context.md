# Claude VS Code Context - Quantitative Trading System

## Project Overview
You are helping build an institutional-grade quantitative trading system with 12 strategies across multiple asset classes. Always maintain production-quality code with comprehensive error handling, logging, and testing.

## Code Conventions

### Python Standards
- Use Python 3.9+ features (type hints mandatory)
- Follow PEP 8 with Black formatting
- Google-style docstrings for all functions
- Async/await for all I/O operations
- Type hints for all function parameters and returns

### Import Order
```python
# Standard library
import os
import sys
from typing import Dict, List, Optional

# Third party
import pandas as pd
import numpy as np

# Local
from src.strategies.base import BaseStrategy
from src.risk.manager import RiskManager
```

### Error Handling Pattern
```python
async def execute_trade(order: Order) -> ExecutionReport:
    """
    Always use this pattern for external calls.
    
    Args:
        order: Order to execute
        
    Returns:
        ExecutionReport with status and details
        
    Raises:
        ExecutionError: If order fails after retries
    """
    try:
        result = await broker_api.submit_order(order)
        logger.info(f"Order {order.id} submitted: {result}")
        return result
    except BrokerConnectionError as e:
        logger.error(f"Connection error for order {order.id}: {e}")
        return await retry_order(order)
    except InsufficientMarginError as e:
        logger.error(f"Insufficient margin for {order.id}: {e}")
        await risk_manager.handle_margin_call(order)
        raise
    except Exception as e:
        logger.exception(f"Unexpected error for {order.id}: {e}")
        await alert_team(f"Critical order failure: {order.id}")
        raise
```

## Architecture Patterns

### Strategy Implementation
Every strategy MUST inherit from BaseStrategy and implement:
```python
class NewStrategy(BaseStrategy):
    async def generate_signals(self, data: MarketData) -> List[Signal]:
        """Generate trading signals from market data."""
        pass
    
    async def calculate_position_size(self, signal: Signal) -> float:
        """Calculate position size using fractional Kelly."""
        pass
    
    async def manage_risk(self, position: Position) -> RiskAction:
        """Check position against risk limits."""
        pass
```

### Database Queries
Always use parameterized queries and connection pooling:
```python
async with db_pool.acquire() as conn:
    result = await conn.fetch(
        """
        SELECT * FROM market_data 
        WHERE symbol = $1 
        AND timestamp >= $2
        ORDER BY timestamp
        """,
        symbol, start_time
    )
```

## Testing Requirements

### Every Module Needs
1. Unit tests with >90% coverage
2. Integration tests for external APIs
3. Performance benchmarks for critical paths
4. Mock data generators for backtesting

### Test Structure
```python
@pytest.mark.asyncio
async def test_strategy_signal_generation():
    """Test signal generation with various market conditions."""
    # Arrange
    strategy = MLMomentumStrategy()
    mock_data = create_mock_market_data(scenario="trending")
    
    # Act
    signals = await strategy.generate_signals(mock_data)
    
    # Assert
    assert len(signals) > 0
    assert all(s.confidence > 0.6 for s in signals)
    assert_position_limits_respected(signals)
```

## Performance Optimization

### Always Consider
- Vectorized operations with NumPy/Pandas over loops
- Async I/O for all external calls
- Caching with Redis for frequently accessed data
- Connection pooling for databases
- Batch operations over individual calls

### Profiling Required For
- Any function called >1000 times/day
- Database queries in hot paths
- Real-time data processing
- Risk calculations

## Risk Management Integration

### Every Trade Must
1. Pass through RiskManager.check_limits()
2. Update portfolio exposure tracking
3. Log to audit trail
4. Update real-time monitoring dashboard

### Position Sizing Rules
- Maximum 2% per position
- Use fractional Kelly (0.25) as baseline
- Adjust for regime and volatility
- Never exceed margin limits

## Monitoring & Logging

### Logging Levels
```python
logger.debug(f"Calculating signal for {symbol}")     # Detailed debugging
logger.info(f"Signal generated: {signal}")           # Normal operation
logger.warning(f"Unusual spread: {spread}")          # Concerning but handled
logger.error(f"API call failed: {error}")           # Error but recovered
logger.critical(f"System failure: {error}")         # Requires immediate attention
```

### Metrics to Track
- Every strategy signal and execution
- All risk limit breaches
- API latencies and failures
- Data quality issues
- System resource usage

## Common Pitfalls to Avoid

1. **Never use market orders in production** - Always use limit orders with smart routing
2. **Don't trust single data sources** - Always have fallback data providers
3. **Avoid look-ahead bias** - Use point-in-time data in backtests
4. **Don't ignore transaction costs** - Model 2x expected costs
5. **Never deploy without paper trading** - Minimum 3 months validation

## External API Patterns

### Rate Limiting
```python
@rate_limit(calls=100, period=timedelta(seconds=1))
async def fetch_market_data(symbol: str) -> MarketData:
    """Automatically rate-limited API call."""
    pass
```

### Retry Logic
```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(TemporaryError)
)
async def execute_order(order: Order) -> ExecutionReport:
    """Auto-retry with exponential backoff."""
    pass
```

## When Writing New Code

1. **Start with tests** - Write the test first, then implementation
2. **Document assumptions** - Be explicit about market assumptions
3. **Handle errors gracefully** - Never let exceptions crash the system
4. **Profile before optimizing** - Measure, don't guess
5. **Keep it simple** - Clarity over cleverness

## Questions to Ask

Before implementing any feature, consider:
- What happens during market stress?
- How does this handle missing data?
- What are the failure modes?
- How do we detect if it stops working?
- What's the impact on latency?
- How does this scale with volume?

---

*This context is loaded for every Claude interaction in VS Code to ensure consistent, high-quality implementations aligned with the project's architecture and standards.*