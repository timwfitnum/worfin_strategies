"""
Comprehensive logging configuration for the quantitative trading system.
Supports multiple handlers, formatters, and structured logging.
"""

import sys
import logging
import json
from typing import Optional, Dict, Any, Union
from pathlib import Path
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import traceback

import structlog
from loguru import logger as loguru_logger
from pythonjsonlogger import jsonlogger


class TradingSystemLogger:
    """Custom logger for the trading system with structured logging support."""

    def __init__(
        self,
        name: str,
        log_level: str = "INFO",
        log_format: str = "json",
        log_file: Optional[Path] = None,
        enable_console: bool = True,
        enable_file: bool = True,
        rotation: str = "1 day",
        retention: str = "30 days",
        max_bytes: int = 100 * 1024 * 1024,  # 100MB
    ):
        self.name = name
        self.log_level = getattr(logging, log_level.upper())
        self.log_format = log_format
        self.log_file = log_file or Path(f"logs/{name}.log")
        self.enable_console = enable_console
        self.enable_file = enable_file
        self.rotation = rotation
        self.retention = retention
        self.max_bytes = max_bytes

        # Ensure log directory exists
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

        # Setup logger
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup and configure the logger."""
        logger = logging.getLogger(self.name)
        logger.setLevel(self.log_level)
        logger.propagate = False

        # Remove existing handlers
        logger.handlers.clear()

        # Add handlers
        if self.enable_console:
            logger.addHandler(self._get_console_handler())

        if self.enable_file:
            logger.addHandler(self._get_file_handler())

        return logger

    def _get_console_handler(self) -> logging.Handler:
        """Create console handler with appropriate formatter."""
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(self.log_level)

        if self.log_format == "json":
            formatter = self._get_json_formatter()
        else:
            formatter = self._get_text_formatter()

        handler.setFormatter(formatter)
        return handler

    def _get_file_handler(self) -> logging.Handler:
        """Create file handler with rotation."""
        if "day" in self.rotation or "hour" in self.rotation:
            # Time-based rotation
            interval, period = self.rotation.split()
            handler = TimedRotatingFileHandler(
                filename=str(self.log_file),
                when=period[0].upper(),  # D for day, H for hour
                interval=int(interval),
                backupCount=30,
                encoding="utf-8",
            )
        else:
            # Size-based rotation
            handler = RotatingFileHandler(
                filename=str(self.log_file),
                maxBytes=self.max_bytes,
                backupCount=10,
                encoding="utf-8",
            )

        handler.setLevel(self.log_level)
        handler.setFormatter(self._get_json_formatter())
        return handler

    def _get_json_formatter(self) -> logging.Formatter:
        """Create JSON formatter for structured logging."""
        fmt = {
            "timestamp": "%(asctime)s",
            "level": "%(levelname)s",
            "logger": "%(name)s",
            "module": "%(module)s",
            "function": "%(funcName)s",
            "line": "%(lineno)d",
            "message": "%(message)s",
            "process_id": "%(process)d",
            "thread_id": "%(thread)d",
        }

        return jsonlogger.JsonFormatter(
            fmt=json.dumps(fmt),
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    def _get_text_formatter(self) -> logging.Formatter:
        """Create text formatter for human-readable logs."""
        fmt = (
            "%(asctime)s | %(levelname)-8s | %(name)s | "
            "%(module)s:%(funcName)s:%(lineno)d | %(message)s"
        )
        return logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")

    def debug(self, message: str, **kwargs):
        """Log debug message with extra context."""
        self.logger.debug(message, extra=kwargs)

    def info(self, message: str, **kwargs):
        """Log info message with extra context."""
        self.logger.info(message, extra=kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message with extra context."""
        self.logger.warning(message, extra=kwargs)

    def error(self, message: str, exc_info: bool = True, **kwargs):
        """Log error message with extra context and optional traceback."""
        self.logger.error(message, exc_info=exc_info, extra=kwargs)

    def critical(self, message: str, exc_info: bool = True, **kwargs):
        """Log critical message with extra context and optional traceback."""
        self.logger.critical(message, exc_info=exc_info, extra=kwargs)

    def trade(self, action: str, symbol: str, quantity: float, price: float, **kwargs):
        """Log trade execution with structured data."""
        trade_data = {
            "action": action,
            "symbol": symbol,
            "quantity": quantity,
            "price": price,
            "value": quantity * price,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **kwargs,
        }
        self.info(f"Trade executed: {action} {quantity} {symbol} @ {price}", **trade_data)

    def performance(self, metric: str, value: float, **kwargs):
        """Log performance metrics."""
        perf_data = {
            "metric": metric,
            "value": value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **kwargs,
        }
        self.info(f"Performance metric: {metric} = {value}", **perf_data)

    def risk(self, metric: str, value: float, threshold: float, **kwargs):
        """Log risk metrics and alerts."""
        risk_data = {
            "risk_metric": metric,
            "value": value,
            "threshold": threshold,
            "breach": value > threshold,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **kwargs,
        }

        if risk_data["breach"]:
            self.warning(f"Risk threshold breach: {metric} = {value} > {threshold}", **risk_data)
        else:
            self.info(f"Risk metric: {metric} = {value}", **risk_data)


class StructuredLogger:
    """Structured logging using structlog for advanced features."""

    def __init__(self, name: str = "quant_system"):
        self.name = name
        self._configure_structlog()
        self.logger = structlog.get_logger(name)

    def _configure_structlog(self):
        """Configure structlog with processors."""
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer(),
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

    def bind(self, **kwargs) -> "StructuredLogger":
        """Bind context variables to the logger."""
        self.logger = self.logger.bind(**kwargs)
        return self

    def unbind(self, *keys) -> "StructuredLogger":
        """Unbind context variables from the logger."""
        self.logger = self.logger.unbind(*keys)
        return self

    def log_event(self, event: str, level: str = "info", **kwargs):
        """Log a structured event."""
        log_method = getattr(self.logger, level.lower())
        log_method(event, **kwargs)


class LoguruLogger:
    """Loguru-based logger for simplified logging with rich features."""

    def __init__(
        self,
        log_file: Optional[Path] = None,
        rotation: str = "100 MB",
        retention: str = "30 days",
        level: str = "INFO",
        enable_console: bool = True,
        enable_file: bool = True,
        serialize: bool = False,
    ):
        # Remove default handler
        loguru_logger.remove()

        # Add console handler
        if enable_console:
            loguru_logger.add(
                sys.stdout,
                level=level,
                format=self._get_format(),
                colorize=True,
                serialize=serialize,
            )

        # Add file handler
        if enable_file and log_file:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            loguru_logger.add(
                log_file,
                level=level,
                format=self._get_format(),
                rotation=rotation,
                retention=retention,
                compression="gz",
                serialize=serialize,
                backtrace=True,
                diagnose=True,
            )

        self.logger = loguru_logger

    def _get_format(self) -> str:
        """Get log format string."""
        return (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        )

    def with_context(self, **kwargs):
        """Add context to logs."""
        return self.logger.bind(**kwargs)


# Factory functions for different logger types
def get_logger(
    name: str = "quant_system", log_type: str = "standard", **kwargs
) -> Union[TradingSystemLogger, StructuredLogger, LoguruLogger]:
    """
    Factory function to get appropriate logger.

    Args:
        name: Logger name
        log_type: Type of logger ('standard', 'structured', 'loguru')
        **kwargs: Additional arguments for logger configuration

    Returns:
        Logger instance
    """
    if log_type == "structured":
        return StructuredLogger(name)
    elif log_type == "loguru":
        return LoguruLogger(**kwargs)
    else:
        return TradingSystemLogger(name, **kwargs)


# Default logger instance
default_logger = get_logger("quant_system", log_type="standard")


# Logging decorators
def log_execution(logger: Optional[Any] = None, level: str = "info"):
    """Decorator to log function execution."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            log = logger or default_logger
            start_time = datetime.now()

            log.info(f"Starting {func.__name__}", function=func.__name__)
            try:
                result = func(*args, **kwargs)
                duration = (datetime.now() - start_time).total_seconds()
                log.info(f"Completed {func.__name__}", function=func.__name__, duration=duration)
                return result
            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds()
                log.error(
                    f"Failed {func.__name__}: {str(e)}",
                    function=func.__name__,
                    duration=duration,
                    error=str(e),
                    exc_info=True,
                )
                raise

        return wrapper

    return decorator


def log_performance(logger: Optional[Any] = None):
    """Decorator to log function performance metrics."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            log = logger or default_logger
            start_time = datetime.now()

            result = func(*args, **kwargs)
            duration = (datetime.now() - start_time).total_seconds()

            log.performance(
                metric=f"{func.__name__}_duration", value=duration, function=func.__name__
            )
            return result

        return wrapper

    return decorator


# Specialized loggers for different components
class StrategyLogger(TradingSystemLogger):
    """Specialized logger for strategy execution."""

    def signal(self, strategy: str, symbol: str, signal: str, strength: float, **kwargs):
        """Log strategy signals."""
        signal_data = {
            "strategy": strategy,
            "symbol": symbol,
            "signal": signal,
            "strength": strength,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **kwargs,
        }
        self.info(f"Signal generated: {strategy} - {symbol} - {signal}", **signal_data)


class BacktestLogger(TradingSystemLogger):
    """Specialized logger for backtesting."""

    def backtest_start(self, strategy: str, start_date: str, end_date: str, **kwargs):
        """Log backtest start."""
        self.info(
            f"Backtest started: {strategy}",
            strategy=strategy,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    def backtest_complete(self, strategy: str, metrics: Dict[str, float], **kwargs):
        """Log backtest completion with metrics."""
        self.info(f"Backtest completed: {strategy}", strategy=strategy, metrics=metrics, **kwargs)


# Export main components
__all__ = [
    "TradingSystemLogger",
    "StructuredLogger",
    "LoguruLogger",
    "StrategyLogger",
    "BacktestLogger",
    "get_logger",
    "default_logger",
    "log_execution",
    "log_performance",
]
