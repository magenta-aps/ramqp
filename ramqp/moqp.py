# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
from datetime import datetime
from enum import auto
from enum import Enum
from enum import unique
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import cast
from typing import List
from typing import Tuple
from uuid import UUID

from aio_pika import IncomingMessage
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from pydantic import parse_raw_as

from .abstract_amqpsystem import AbstractAMQPSystem


class AutoNameEnum(Enum):
    # From: https://docs.python.org/3/library/enum.html#using-automatic-values
    @staticmethod
    def _generate_next_value_(
        name: Any, start: int, count: int, last_values: List[Any]
    ) -> str:
        return cast(str, name)


@unique
class ServiceType(str, AutoNameEnum):
    EMPLOYEE: str = cast(str, auto())
    ORG_UNIT: str = cast(str, auto())
    WILDCARD: str = "*"


@unique
class ObjectType(str, AutoNameEnum):
    ADDRESS: str = cast(str, auto())
    ASSOCIATION: str = cast(str, auto())
    EMPLOYEE: str = cast(str, auto())
    ENGAGEMENT: str = cast(str, auto())
    IT: str = cast(str, auto())
    KLE: str = cast(str, auto())
    LEAVE: str = cast(str, auto())
    MANAGER: str = cast(str, auto())
    OWNER: str = cast(str, auto())
    ORG_UNIT: str = cast(str, auto())
    RELATED_UNIT: str = cast(str, auto())
    ROLE: str = cast(str, auto())
    WILDCARD: str = "*"


@unique
class RequestType(str, AutoNameEnum):
    CREATE: str = cast(str, auto())
    EDIT: str = cast(str, auto())
    TERMINATE: str = cast(str, auto())
    REFRESH: str = cast(str, auto())
    WILDCARD: str = "*"


class PayloadType(BaseModel):
    uuid: UUID
    object_uuid: UUID
    time: datetime


MOCallbackType = Callable[
    [ServiceType, ObjectType, RequestType, PayloadType], Awaitable
]


def to_routing_key(
    service_type: ServiceType, object_type: ObjectType, request_type: RequestType
) -> str:
    """Convert the given service.object.request tuple to a routing_key.

    Args:
        service_type: The service type to convert.
        object_type: The object type to convert.
        request_type: The request type to convert.

    Returns:
        The equivalent routing key.
    """
    return ".".join([service_type, object_type, request_type])


def from_routing_key(routing_key: str) -> Tuple[ServiceType, ObjectType, RequestType]:
    """Convert the given routing_key to a service.object.request tuple.

    Raises:
        ValueError: Raised if the routing_key does not contain 2 periods.
        ValueError: Raised if the routing_key components cannot be parsed.

    Args:
        routing_key: The routing key to convert.

    Returns:
        The equivalent service.object.request tuple.
    """
    parts = routing_key.split(".")
    if len(parts) != 3:
        raise ValueError("Expected a three tuple!")
    service_str, object_str, request_str = parts
    return ServiceType(service_str), ObjectType(object_str), RequestType(request_str)


class MOAMQPSystem(AbstractAMQPSystem):
    def register(
        self,
        service_type: ServiceType,
        object_type: ObjectType,
        request_type: RequestType,
    ) -> Callable:
        """Get a decorator for registering callbacks.

        Examples:
            ```
            address_create_decorator = moamqp_system.register(
                ServiceType.EMPLOYEE,
                ObjectType.ADDRESS,
                RequestType.CREATE
            )
            @address_create_decorator
            def callback(
                service_type: ServiceType,
                object_type: ObjectType,
                request_type: RequestType,
                payload: PayloadType
            ) -> None:
                pass
            ```
            Or directly:
            ```
            @moamqp_system.register(
                ServiceType.EMPLOYEE,
                ObjectType.ADDRESS,
                RequestType.CREATE
            )
            def callback(
                service_type: ServiceType,
                object_type: ObjectType,
                request_type: RequestType,
                payload: PayloadType
            ) -> None:
                pass
            ```


        Args:
            service_type: The service type to bind messages for.
            object_type: The object type to bind messages for.
            request_type: The request type to bind messages for.

        Returns:
            A decorator for registering a function to receive callbacks.
        """
        routing_key = to_routing_key(service_type, object_type, request_type)
        amqp_decorator = self._register(routing_key)

        def decorator(function: MOCallbackType) -> MOCallbackType:
            async def wrapper(message: IncomingMessage) -> None:
                assert message.routing_key is not None
                routing_tuple = from_routing_key(message.routing_key)
                payload = parse_raw_as(PayloadType, message.body)
                await function(*routing_tuple, payload)

            wrapper.__name__ = function.__name__
            amqp_decorator(wrapper)
            return function

        return decorator

    async def publish_message(
        self,
        service_type: ServiceType,
        object_type: ObjectType,
        request_type: RequestType,
        payload: PayloadType,
    ) -> None:
        """Publish a message to the given service.object.request tuple.

        Args:
            service_type: The service type to send the message to.
            object_type: The object type to send the message to.
            request_type: The request type to send the message to.
            payload: The message payload.

        Returns:
            None
        """
        routing_key = to_routing_key(service_type, object_type, request_type)
        payload_obj = jsonable_encoder(payload)
        await self._publish_message(routing_key, payload_obj)
