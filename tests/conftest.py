"""
tests/conftest.py
Shared fixtures for all test modules.
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def no_live_trading(monkeypatch):
    """
    Safety guard: prevent any test from touching live systems.
    All tests run in DEVELOPMENT mode automatically.
    """
    monkeypatch.setenv("ENVIRONMENT", "development")