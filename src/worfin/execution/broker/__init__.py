"""
execution/broker
IBKR Gateway connector. One module per broker — currently just ib_insync.

Public API:
    get_broker()           — singleton accessor
    IBKRBroker             — the connector class
    BrokerOrderRequest     — submission DTO
    BrokerOrderStatus      — status DTO
    OrderStatusValue       — status enum (pending/submitted/partial/filled/...)
    Side                   — buy/sell enum
    OrderType              — limit_passive/limit_aggressive/market enum
    BrokerConnectionError  — connection problems
    BrokerOrderError       — order placement/cancel problems
    BrokerPermissionError  — account lacks trading permissions for contract
"""

from worfin.execution.broker.ibkr import (
    BrokerConnectionError,
    BrokerOrderError,
    BrokerOrderRequest,
    BrokerOrderStatus,
    BrokerPermissionError,
    IBKRBroker,
    OrderStatusValue,
    OrderType,
    Quote,
    Side,
    get_broker,
)

__all__ = [
    "BrokerConnectionError",
    "BrokerOrderError",
    "BrokerOrderRequest",
    "BrokerOrderStatus",
    "BrokerPermissionError",
    "IBKRBroker",
    "OrderStatusValue",
    "OrderType",
    "Quote",
    "Side",
    "get_broker",
]
