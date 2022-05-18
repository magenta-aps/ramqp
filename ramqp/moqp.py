# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module contains the MO specific AMQPSystem."""
from functools import wraps
from typing import Any
from typing import Callable
from typing import Dict
from typing import overload
from typing import Tuple

from aio_pika import IncomingMessage
from fastapi.encoders import jsonable_encoder
from pydantic import parse_raw_as

from .abstract_amqpsystem import AbstractAMQPSystem
from .mo_models import MOCallbackType
from .mo_models import MORoutingKey
from .mo_models import ObjectType
from .mo_models import PayloadType
from .mo_models import RequestType
from .mo_models import ServiceType
from .utils import CallbackType


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
            routing_key = MORoutingKey.from_routing_key(message.routing_key)
            payload = parse_raw_as(PayloadType, message.body)
            await adaptee(routing_key, payload)

        # Register our newly created adapter for early return.
        self._adapter_map[adaptee] = adapter
        return adapter

    @overload
    def register(
        self,
        service_type: ServiceType,
        object_type: ObjectType,
        request_type: RequestType,
    ) -> Callable:  # pragma: no cover
        ...

    @overload
    def register(
        self, routing_key_tuple: Tuple[ServiceType, ObjectType, RequestType]
    ) -> Callable:  # pragma: no cover
        ...

    @overload
    def register(self, routing_key: str) -> Callable:  # pragma: no cover
        ...

    @overload
    def register(self, mo_routing_key: MORoutingKey) -> Callable:  # pragma: no cover
        ...

    def register(self, *args: Any, **kwargs: Any) -> Callable:
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
        mo_routing_key = MORoutingKey.build(*args, **kwargs)
        amqp_decorator = self._register(str(mo_routing_key))

        def decorator(function: MOCallbackType) -> MOCallbackType:
            amqp_decorator(self._construct_adapter(function))
            return function

        return decorator

    @overload
    async def publish_message(
        self,
        service_type: ServiceType,
        object_type: ObjectType,
        request_type: RequestType,
        payload: PayloadType,
    ) -> None:  # pragma: no cover
        ...

    @overload
    async def publish_message(
        self,
        routing_key_tuple: Tuple[ServiceType, ObjectType, RequestType],
        payload: PayloadType,
    ) -> None:  # pragma: no cover
        ...

    @overload
    async def publish_message(
        self, routing_key: str, payload: PayloadType
    ) -> None:  # pragma: no cover
        ...

    @overload
    async def publish_message(
        self, mo_routing_key: MORoutingKey, payload: PayloadType
    ) -> None:  # pragma: no cover
        ...

    async def publish_message(self, *args: Any, **kwargs: Any) -> None:
        """Publish a message to the given service.object.request tuple.

        Args:
            service_type: The service type to send the message to.
            object_type: The object type to send the message to.
            request_type: The request type to send the message to.
            payload: The message payload.

        Returns:
            None
        """
        if "payload" in kwargs:  # pragma: no cover
            payload = kwargs["payload"]
            del kwargs["payload"]
        else:
            payload = args[-1]
            args = args[:-1]

        routing_key = MORoutingKey.build(*args, **kwargs)
        payload_obj = jsonable_encoder(payload)
        await self._publish_message(str(routing_key), payload_obj)
