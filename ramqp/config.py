# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=too-few-public-methods
"""This module contains the all pydantic BaseSetting(s)."""
from typing import Optional

from pydantic import AmqpDsn
from pydantic import BaseSettings
from pydantic import parse_obj_as


class ConnectionSettings(BaseSettings):
    """Settings for the AMQP connection.

    These settings can either be provided via environmental variables or as overrides
    to the AMQPSystem.start method via. args / kwargs.

    Attributes:
        queue_prefix:
            Program specific queue name prefix, should be globally unique, but
            consistent across program restarts. The program name is a good candidate.

            Only required for receiving.

        amqp_url:
            The AMQP connection url, including credentials and vhost.

        amqp_exchange:
            The AMQP Exchange we are binding queue messages from. Default is os2mo,
            but can be overridden for integration specific exchanges.
    """

    queue_prefix: Optional[str]
    amqp_url: AmqpDsn = parse_obj_as(AmqpDsn, "amqp://guest:guest@localhost:5672")
    amqp_exchange: str = "os2mo"
