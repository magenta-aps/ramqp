# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module contains the MO specific AMQPSystem."""
from functools import wraps
from typing import Any
from typing import Callable
from typing import Dict
from typing import Tuple

from aio_pika import IncomingMessage
from fastapi.encoders import jsonable_encoder
from pydantic import parse_raw_as

from .abstract_amqpsystem import AbstractAMQPSystem
from .metrics import exception_mo_routing_counter
from .mo_models import MOCallbackType
from .mo_models import ObjectType
from .mo_models import PayloadType
from .mo_models import RequestType
from .mo_models import ServiceType
from .utils import CallbackType


MORoutingTuple = Tuple[ServiceType, ObjectType, RequestType]


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
    return f"{service_type.value}.{object_type.value}.{request_type.value}"


def from_routing_key(routing_key: str) -> MORoutingTuple:
    """Convert the given routing_key to a service.object.request tuple.

    Raises:
        ValueError: Raised if the routing_key does not contain 2 periods.
        ValueError: Raised if the routing_key components cannot be parsed.

    Args:
        routing_key: The routing key to convert.

    Returns:
        The equivalent service.object.request tuple.
    """
    with exception_mo_routing_counter.labels(routing_key).count_exceptions():
        parts = routing_key.split(".")
        if len(parts) != 3:
            raise ValueError("Expected a three tuple!")
        service_str, object_str, request_str = parts
        return (
            ServiceType(service_str),
            ObjectType(object_str),
            RequestType(request_str),
        )


class MOAMQPSystem(AbstractAMQPSystem):
    """MO specific AMQPSystem.

    Has specifically tailored `register` and `publish_message` methods.
    Both of which utilize the MO AMQP routing-key structure and payload format.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._adapter_map: Dict[MOCallbackType, CallbackType] = {}

    def _construct_adapter(self, adaptee: MOCallbackType) -> CallbackType:
        """Construct an adapter from a MOCallbackType to CallbackType.

        Note:
            Multiple calls with the same adaptee always return the same adapter.

        Args:
            adaptee: The callback function to be adapted.

        Returns:
            The adapted callback function calling the input adaptee on invocation.
        """

        # Early return the previously constructed adapter.
        # This is required so every function maps to one and only one adapter,
        # which is in itself required by start to ensure uniqueness of queues.
        if adaptee in self._adapter_map:
            return self._adapter_map[adaptee]

        @wraps(adaptee)
        async def adapter(message: IncomingMessage) -> None:
            """Adapter function mapping MOCallbackType to CallbackType.

            Unpacks the provided message and converts it into a routing tuple and
            PayloadType before calling the captured callback.

            Args:
                message: incoming message to converted and passed to callback.

            Returns:
                None
            """
            assert message.routing_key is not None
            routing_tuple = from_routing_key(message.routing_key)
            payload = parse_raw_as(PayloadType, message.body)
            await adaptee(*routing_tuple, payload)

        # Register our newly created adapter for early return.
        self._adapter_map[adaptee] = adapter
        return adapter

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
            amqp_decorator(self._construct_adapter(function))
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
