"""
Shared pytest configuration.

Safety net: every test runs with real network access disabled, so no test can
ever reach the real OMDb API (or use the daily quota), even by mistake.
Tests that need API behaviour pass a FakeSession instead.
"""

import pytest
import requests


@pytest.fixture(autouse=True)
def block_real_network(monkeypatch):
    def refuse(*args, **kwargs):
        raise RuntimeError("Real network access is disabled during tests")

    monkeypatch.setattr(requests.Session, "request", refuse)