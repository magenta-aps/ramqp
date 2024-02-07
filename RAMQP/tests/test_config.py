# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=too-few-public-methods,missing-class-docstring
"""This module tests the connection settings."""
from pydantic import AmqpDsn
from pydantic import BaseSettings
from pydantic import parse_obj_as
from pytest import MonkeyPatch

from ramqp.config import AMQPConnectionSettings
from ramqp.config import StructuredAmqpDsn


def test_amqp_connection_settings_url() -> None:
    """Test AMQP URL using AmqpDsn directly."""
    settings = AMQPConnectionSettings(
        url=parse_obj_as(AmqpDsn, "amqp://guest:guest@msg_broker:5672/cough")
    )
    url = settings.get_url()
    assert url == "amqp://guest:guest@msg_broker:5672/cough"
    assert url.scheme == "amqp"
    assert url.user == "guest"
    assert url.password == "guest"
    assert url.host == "msg_broker"
    assert url.port == "5672"
    assert url.path == "/cough"


def test_amqp_connection_settings_structured_fields() -> None:
    """Test AMQP URL using StructuredAmqpDsn directly."""
    settings = AMQPConnectionSettings(
        url=StructuredAmqpDsn(
            scheme="amqp",
            user="guest",
            password="guest",
            host="msg_broker",
            port="5672",
            vhost="cough",  # noqa
        )
    )
    assert settings.get_url() == "amqp://guest:guest@msg_broker:5672/cough"


def test_amqp_connection_settings_url_environment_variables(
    monkeypatch: MonkeyPatch,
) -> None:
    """Test AMQP URL directly through environment variables."""
    monkeypatch.setenv("AMQP__URL", "amqp://guest:guest@msg_broker:5672/cough")

    class Settings(BaseSettings):
        amqp: AMQPConnectionSettings

        class Config:
            env_nested_delimiter = "__"  # allows setting e.g. AMQP__URL__HOST=foo

    settings = Settings()

    assert settings.amqp.get_url() == "amqp://guest:guest@msg_broker:5672/cough"


def test_amqp_connection_settings_fields_environment_variables(
    monkeypatch: MonkeyPatch,
) -> None:
    """Test structured fields through environment variables."""
    monkeypatch.setenv("AMQP__URL__SCHEME", "amqp")
    monkeypatch.setenv("AMQP__URL__USER", "guest")
    monkeypatch.setenv("AMQP__URL__PASSWORD", "guest")
    monkeypatch.setenv("AMQP__URL__HOST", "msg_broker")
    monkeypatch.setenv("AMQP__URL__PORT", "5672")
    monkeypatch.setenv("AMQP__URL__VHOST", "cough")

    class Settings(BaseSettings):
        amqp: AMQPConnectionSettings

        class Config:
            env_nested_delimiter = "__"  # allows setting e.g. AMQP__URL__HOST=foo

    settings = Settings()

    assert settings.amqp.get_url() == "amqp://guest:guest@msg_broker:5672/cough"
