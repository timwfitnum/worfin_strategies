# Strategy Template - Use this as starting point for new strategies

```python
"""
[Strategy Name] Implementation.

[Detailed description of the strategy, its edge, and market hypothesis]
"""

import asyncio
import numpy as np
import pandas as pd
from typing import List, Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass

from src.core.base import BaseStrategy
from src.core.types import Signal, MarketData, Position
from src.risk.manager import RiskManager
from src.utils.logger import get_logger

logger = get_logger(__name__)


class [StrategyName]Strategy(BaseStrategy):
    """
    [One-line description of strategy].
    
    Market Hypothesis:
        [Explain why this strategy should work]
    
    Key Features:
        - [Feature 1]
        - [Feature 2]
        - [Feature 3]
    
    Risk Considerations:
        - [Risk 1]
        - [Risk 2]
        - [Risk 3]
    """
    
    def __init__(
        self,
        strategy_id: str,
        config: Dict[str, Any]
    ):
        """
        Initialize strategy.
        
        Args:
            strategy_id: Unique strategy identifier
            config: Strategy configuration parameters
        """
        super().__init__(strategy_id, config)
        
        # Strategy-specific parameters
        self.lookback_period = config.get('lookback_period', 20)
        self.entry_threshold = config.get('entry_threshold', 2.0)
        self.position_limit = config.get('position_limit', 0.02)
        
        # Internal state
        self._historical_data: Dict[str, pd.DataFrame] = {}
        self._current_positions: Dict[str, Position] = {}
        
        logger.info(
            f"Initialized {self.__class__.__name__}",
            strategy_id=strategy_id,
            config=config
        )
    
    async def initialize(self) -> None:
        """
        Perform async initialization.
        
        Loads historical data, calibrates parameters, etc.
        """
        logger.info(f"Starting initialization for {self.strategy_id}")
        
        try:
            # Load historical data for calibration
            await self._load_historical_data()
            
            # Calibrate strategy parameters
            await self._calibrate_parameters()
            
            # Initialize risk manager
            await self.risk_manager.initialize()
            
            logger.info(f"Successfully initialized {self.strategy_id}")
            
        except Exception as e:
            logger.error(
                f"Failed to initialize {self.strategy_id}",
                error=str(e)
            )
            raise
    
    async def generate_signals(
        self,
        market_data: MarketData
    ) -> List[Signal]:
        """
        Generate trading signals from market data.
        
        Args:
            market_data: Current market data
            
        Returns:
            List of trading signals
        """
        signals = []
        
        try:
            # Update historical data
            self._update_historical(market_data)
            
            # Calculate signal strength
            signal_strength = await self._calculate_signal_strength(
                market_data.symbol
            )
            
            # Generate signal if threshold met
            if abs(signal_strength) > self.entry_threshold:
                signal = Signal(
                    symbol=market_data.symbol,
                    direction=int(np.sign(signal_strength)),
                    target_position=self._calculate_position_size(
                        signal_strength
                    ),
                    confidence=min(abs(signal_strength) / 10, 1.0),
                    strategy_id=self.strategy_id,
                    timestamp=datetime.utcnow(),
                    metadata={
                        'signal_strength': signal_strength,
                        'lookback_period': self.lookback_period
                    }
                )
                
                # Validate signal with risk manager
                if await self.risk_manager.validate_signal(signal):
                    signals.append(signal)
                    logger.info(
                        "Signal generated",
                        signal=signal.dict()
                    )
                else:
                    logger.warning(
                        "Signal rejected by risk manager",
                        signal=signal.dict()
                    )
            
        except Exception as e:
            logger.error(
                f"Error generating signals for {market_data.symbol}",
                error=str(e)
            )
            self.metrics.signal_errors.inc()
        
        return signals
    
    async def calculate_position_size(
        self,
        signal: Signal,
        portfolio_value: float
    ) -> float:
        """
        Calculate position size for signal.
        
        Args:
            signal: Trading signal
            portfolio_value: Current portfolio value
            
        Returns:
            Position size in units
        """
        # Base position from Kelly criterion
        kelly_fraction = 0.25  # Use 1/4 Kelly
        
        # Adjust for confidence
        base_position = kelly_fraction * signal.confidence
        
        # Apply position limit
        position_pct = min(base_position, self.position_limit)
        
        # Convert to units
        position_value = portfolio_value * position_pct
        current_price = await self._get_current_price(signal.symbol)
        
        return position_value / current_price
    
    async def manage_risk(
        self,
        position: Position
    ) -> Optional[Signal]:
        """
        Manage risk for existing position.
        
        Args:
            position: Current position
            
        Returns:
            Exit signal if risk limit breached, None otherwise
        """
        # Check stop loss
        if position.unrealized_pnl_pct < -0.02:  # 2% stop loss
            return Signal(
                symbol=position.symbol,
                direction=0,  # Flat
                target_position=0,
                confidence=1.0,
                strategy_id=self.strategy_id,
                timestamp=datetime.utcnow(),
                metadata={'reason': 'stop_loss'}
            )
        
        # Check profit target  
        if position.unrealized_pnl_pct > 0.05:  # 5% profit target
            return Signal(
                symbol=position.symbol,
                direction=0,  # Flat
                target_position=0,
                confidence=1.0,
                strategy_id=self.strategy_id,
                timestamp=datetime.utcnow(),
                metadata={'reason': 'profit_target'}
            )
        
        return None
    
    async def on_fill(
        self,
        execution: ExecutionReport
    ) -> None:
        """
        Handle execution confirmation.
        
        Args:
            execution: Execution report from broker
        """
        logger.info(
            f"Execution confirmed for {self.strategy_id}",
            execution=execution.dict()
        )
        
        # Update internal position tracking
        if execution.symbol in self._current_positions:
            position = self._current_positions[execution.symbol]
            position.quantity += execution.filled_quantity
            
            if position.quantity == 0:
                del self._current_positions[execution.symbol]
        else:
            self._current_positions[execution.symbol] = Position(
                symbol=execution.symbol,
                quantity=execution.filled_quantity,
                entry_price=execution.avg_price,
                entry_time=execution.timestamp,
                strategy_id=self.strategy_id
            )
    
    # Private methods
    
    async def _load_historical_data(self) -> None:
        """Load historical data for strategy calibration."""
        # Implementation here
        pass
    
    async def _calibrate_parameters(self) -> None:
        """Calibrate strategy parameters from historical data."""
        # Implementation here
        pass
    
    def _update_historical(
        self,
        market_data: MarketData
    ) -> None:
        """Update historical data buffer with new data."""
        # Implementation here
        pass
    
    async def _calculate_signal_strength(
        self,
        symbol: str
    ) -> float:
        """
        Calculate signal strength for symbol.
        
        Returns:
            Signal strength from -10 to +10
        """
        # Implementation here
        return 0.0
    
    def _calculate_position_size(
        self,
        signal_strength: float
    ) -> float:
        """Calculate position size from signal strength."""
        # Implementation here
        return min(abs(signal_strength) / 100, self.position_limit)
    
    async def _get_current_price(
        self,
        symbol: str
    ) -> float:
        """Get current price for symbol."""
        # Implementation here
        return 100.0


# Unit tests

import pytest
from unittest.mock import Mock, AsyncMock

class Test[StrategyName]Strategy:
    """Test suite for [StrategyName] strategy."""
    
    @pytest.fixture
    async def strategy(self):
        """Create strategy instance for testing."""
        config = {
            'lookback_period': 20,
            'entry_threshold': 2.0,
            'position_limit': 0.02
        }
        strategy = [StrategyName]Strategy('test_strategy', config)
        await strategy.initialize()
        yield strategy
    
    @pytest.mark.asyncio
    async def test_signal_generation(self, strategy):
        """Test signal generation logic."""
        # Arrange
        market_data = MarketData(
            symbol='AAPL',
            timestamp=datetime.utcnow(),
            bid=150.00,
            ask=150.02,
            volume=1000000
        )
        
        # Mock internal methods
        strategy._calculate_signal_strength = AsyncMock(return_value=3.0)
        
        # Act
        signals = await strategy.generate_signals(market_data)
        
        # Assert
        assert len(signals) == 1
        assert signals[0].direction == 1  # Long
        assert signals[0].symbol == 'AAPL'
    
    @pytest.mark.asyncio
    async def test_risk_management(self, strategy):
        """Test risk management logic."""
        # Arrange
        position = Position(
            symbol='AAPL',
            quantity=100,
            entry_price=150.00,
            unrealized_pnl_pct=-0.025  # -2.5% loss
        )
        
        # Act
        exit_signal = await strategy.manage_risk(position)
        
        # Assert
        assert exit_signal is not None
        assert exit_signal.direction == 0  # Flat
        assert exit_signal.metadata['reason'] == 'stop_loss'
```