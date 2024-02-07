# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=too-few-public-methods
"""This module contains the all pydantic BaseModel settings(s)."""
import typing

from pydantic import AmqpDsn
from pydantic import BaseModel
from pydantic import Field
from pydantic import parse_obj_as
from pydantic import validator


class StructuredAmqpDsn(BaseModel):
    """Allows specifying AMQP connection URL as individual fields."""

    scheme: str
    user: str | None = None
    password: str | None = None
    host: str | None = None
    port: str | None = None
    path: str | None = Field(None, alias="vhost")

    @validator("path")
    def add_slash_to_path(cls, value: str | None) -> str | None:
        """Ensure the path component starts with a slash.

        The path component in Pydantic needs to start with a slash, which makes sense
        for HTTP URLs, but generally you would define `vhost = os2mo`, not `/os2mo`.

        Args:
            value: The input url.

        Returns:
            The input value, but with a slash prepended if it was missing.
        """
        if value is not None and not value.startswith("/"):
            value = f"/{value}"
        return value

    # pylint: disable=missing-class-docstring
    class Config:
        allow_population_by_field_name = True


class AMQPConnectionSettings(BaseModel):
    """Settings for the AMQP connection.

    The connection URL can be specified as a single value, or, alternatively, as
    individual components. In either case, the user should utilise the `get_url()`
    method to access the URL.
    """

    url: AmqpDsn | StructuredAmqpDsn

    @typing.no_type_check
    def get_url(self) -> AmqpDsn:
        """Get AMQP URL.

        The AMQP URL is either taken directly from the `url` attribute, if specified as
        an AmqpDsn, or built from the individual fields if a StructuredAmqpDsn.
        Ideally, the URL would be exposed simply as the attribute, but it is difficult
        to get working properly when the input has to accept different types.

        Returns:
            The AMQP connection URL.
        """
        if isinstance(self.url, AmqpDsn):
            return self.url
        return parse_obj_as(AmqpDsn, AmqpDsn.build(**self.url.dict()))

    # The AMQP Exchange we are binding queue messages from. Can be overridden
    # for integration specific exchanges.
    exchange: str = "os2mo"

    # Program specific queue name prefix, should be globally unique, but
    # consistent across program restarts. The program name is a good candidate.
    queue_prefix: str | None

    # Maximum number of messages to fetch and handle in parallel.
    prefetch_count: int = 10
