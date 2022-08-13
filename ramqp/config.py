# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=too-few-public-methods
"""This module contains the all pydantic BaseSetting(s)."""
from typing import Any
from typing import Dict
from typing import Optional

from pydantic import AmqpDsn
from pydantic import BaseSettings
from pydantic import parse_obj_as
from pydantic import root_validator
from pydantic import SecretStr


class ConnectionSettings(BaseSettings):
    """Settings for the AMQP connection.

    These settings can either be provided via environmental variables or directly to
    the AMQPSystem's init.
    """

    # The AMQP connection url, including credentials and vhost.
    url: AmqpDsn

    # Alternatively, AMQP connection settings can be set individually.
    scheme: Optional[str]
    host: Optional[str]
    user: Optional[str]
    password: Optional[SecretStr]
    port: Optional[int]
    vhost: Optional[str]

    # The AMQP Exchange we are binding queue messages from. Can be overridden
    # for integration specific exchanges.
    exchange: str = "os2mo"

    # Program specific queue name prefix, should be globally unique, but
    # consistent across program restarts. The program name is a good candidate.
    queue_prefix: Optional[str]

    # Maximum number of messages to fetch and handle in parallel.
    prefetch_count: int = 10

    @root_validator(pre=True)
    def url_from_fields(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Construct AMQP URI from individual fields if one is not set explicitly"""
        # Use full AMQP connection URI if set
        if values.get("url"):
            return values

        # Otherwise, construct one from the individual fields.
        # Pre-validators, which we need to use to have 'url' non-optional,
        # do not get defaults from untouched fields, so default values are
        # defined here instead of above.
        uri = "{scheme}://{user}:{password}@{host}:{port}".format(
            scheme=values.get("scheme", "amqp"),
            host=values.get("host", "msg_broker"),
            user=values.get("user", "guest"),
            password=values.get("password", "guest"),
            port=values.get("port", 5672),
        )
        if vhost := values.get("vhost"):
            uri = f"{uri}/{vhost}"
        values["url"] = parse_obj_as(AmqpDsn, uri)
        return values

    # pylint: disable=missing-class-docstring
    class Config:
        env_prefix = "amqp_"
