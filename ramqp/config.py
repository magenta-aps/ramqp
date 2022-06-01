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

    These settings can either be provided via environmental variables or as overrides
    to the AMQPSystem.start method via. args / kwargs.
    """

    # The AMQP connection url, including credentials and vhost.
    amqp_url: AmqpDsn

    # Alternatively, AMQP connection settings can be set individually.
    amqp_scheme: Optional[str]
    amqp_host: Optional[str]
    amqp_user: Optional[str]
    amqp_password: Optional[SecretStr]
    amqp_port: Optional[int]
    amqp_vhost: Optional[str]

    # The AMQP Exchange we are binding queue messages from. Can be overridden
    # for integration specific exchanges.
    amqp_exchange: str = "os2mo"

    # Program specific queue name prefix, should be globally unique, but
    # consistent across program restarts. The program name is a good candidate.
    amqp_queue_prefix: Optional[str]

    @root_validator(pre=True)
    def amqp_url_from_fields(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Construct AMQP URI from individual fields if one is not set explicitly"""
        # Use full AMQP connection URI if set
        if values.get("amqp_url"):
            return values

        # Otherwise, construct one from the individual fields.
        # Pre-validators, which we need to use to have 'amqp_url' non-optional,
        # do not get defaults from untouched fields, so default values are
        # defined here instead of above.
        uri = "{scheme}://{user}:{password}@{host}:{port}".format(
            scheme=values.get("amqp_scheme", "amqp"),
            host=values.get("amqp_host", "msg_broker"),
            user=values.get("amqp_user", "guest"),
            password=values.get("amqp_password", "guest"),
            port=values.get("amqp_port", 5672),
        )
        if amqp_vhost := values.get("amqp_vhost"):
            uri = f"{uri}/{amqp_vhost}"
        values["amqp_url"] = parse_obj_as(AmqpDsn, uri)
        return values
