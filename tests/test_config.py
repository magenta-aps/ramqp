# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the connection settings."""
import os

from _pytest.monkeypatch import MonkeyPatch

from ramqp.config import ConnectionSettings


def test_default_amqp_url() -> None:
    """Test default AMQP URL"""
    settings = ConnectionSettings()
    assert settings.url == "amqp://guest:guest@msg_broker:5672"


def test_explicit_amqp_url() -> None:
    """Test explicit AMQP URL"""
    settings = ConnectionSettings(url="amqp://test")
    assert settings.url == "amqp://test"


def test_amqp_url_from_individual_fields() -> None:
    """Test AMQP URL from individual fields"""
    settings = ConnectionSettings(
        host="test",
        user="me",
        password="hunter2",
        port=1234,
    )
    assert settings.url == "amqp://me:hunter2@test:1234"


def test_amqp_vhost_field() -> None:
    """Test AMQP vhost field"""
    settings = ConnectionSettings(vhost="cough")  # it's funny in danish
    assert settings.url == "amqp://guest:guest@msg_broker:5672/cough"


def test_from_environment(monkeypatch: MonkeyPatch) -> None:
    """Test that AMQP settings are picked up from the environment."""
    environment = {
        "amqp_url": "amqp://test",
    }
    monkeypatch.setattr(os, "environ", environment)
    settings = ConnectionSettings()
    assert settings.url == "amqp://test"
