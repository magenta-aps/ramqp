# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=too-few-public-methods
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

from ..abstract import AbstractAMQPSystem
from ..abstract import AbstractPublishMixin
from ..abstract import AbstractRouter
from ..mo.models import MOCallbackType
from ..mo.models import MORoutingKey
from ..mo.models import ObjectType
from ..mo.models import PayloadType
from ..mo.models import RequestType
from ..mo.models import ServiceType
from ..utils import CallbackType
from ..utils import handle_exclusively


class MORouter(AbstractRouter):
    """MO specific Router.

    Has specifically tailored `register` methods, which utilize the MO AMQP
    routing-key structure and payload format.
    """

    def __init__(self) -> None:
        super().__init__()
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

        def mo_payload_exclusivity_key(payload: PayloadType, **_: Any) -> tuple:
            return payload.uuid, payload.object_uuid

        exclusive_handler = handle_exclusively(key=mo_payload_exclusivity_key)

        @wraps(adaptee)
        async def adapter(message: IncomingMessage, context: dict) -> None:
            """Adapter function mapping MOCallbackType to CallbackType.

            Unpacks the provided message and converts it into a routing tuple and
            PayloadType before calling the captured callback.

            Args:
                message: incoming message to converted and passed to callback.
                context: Additional context from the AMQP system passed handlers.

            Returns:
                None
            """
            assert message.routing_key is not None
            mo_routing_key = MORoutingKey.from_routing_key(message.routing_key)
            payload = parse_raw_as(PayloadType, message.body)
            exclusive_adaptee = exclusive_handler(adaptee)
            await exclusive_adaptee(
                mo_routing_key=mo_routing_key, payload=payload, context=context
            )

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
            address_create_decorator = morouter.register(
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
            @morouter.register(
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

        Note that messages for each callback are always handled exclusively to avoid
        potential race conditions in the application. This means that multiple messages
        for the same MO PayloadType uuid/object_uuid cannot be handled concurrently.

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


class MOPublishMixin(AbstractPublishMixin):
    """MO specific PublishMixin.

    Has a specifically tailored `publish_message` method, which utilize the MO AMQP
    routing-key structure and payload format.
    """

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


class MOAMQPSystem(AbstractAMQPSystem[MORouter], MOPublishMixin):
    """MO specific AMQPSystem."""

    router_cls = MORouter
