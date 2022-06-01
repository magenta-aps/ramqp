# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the connection settings."""
from ramqp.config import ConnectionSettings


def test_default_amqp_url() -> None:
    """Test default AMQP URL"""
    settings = ConnectionSettings()
    assert settings.amqp_url == "amqp://guest:guest@msg_broker:5672"


def test_explicit_amqp_url() -> None:
    """Test explicit AMQP URL"""
    settings = ConnectionSettings(amqp_url="amqp://test")
    assert settings.amqp_url == "amqp://test"


def test_amqp_url_from_individual_fields() -> None:
    """Test AMQP URL from individual fields"""
    settings = ConnectionSettings(
        amqp_host="test",
        amqp_user="me",
        amqp_password="hunter2",
        amqp_port=1234,
    )
    assert settings.amqp_url == "amqp://me:hunter2@test:1234"


def test_amqp_vhost_field() -> None:
    """Test AMQP vhost field"""
    settings = ConnectionSettings(amqp_vhost="cough")  # it's funny in danish
    assert settings.amqp_url == "amqp://guest:guest@msg_broker:5672/cough"
