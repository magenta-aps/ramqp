# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
from typing import Callable

import structlog

from .abstract_amqpsystem import AbstractAMQPSystem
from .utils import CallbackType


logger = structlog.get_logger()


class AMQPSystem(AbstractAMQPSystem):
    def register(self, routing_key: str) -> Callable[[CallbackType], CallbackType]:
        """Get a decorator for registering callbacks.

        Examples:
            ```
            address_create_decorator = amqp_system.register("EMPLOYEE.ADDRESS.CREATE")

            @address_create_decorator
            def callback1(message: IncomingMessage):
                pass

            @address_create_decorator
            def callback2(message: IncomingMessage):
                pass
            ```
            Or directly:
            ```
            @amqp_system.register("EMPLOYEE.ADDRESS.CREATE")
            def callback1(message: IncomingMessage):
                pass

            @amqp_system.register("EMPLOYEE.ADDRESS.CREATE")
            def callback2(message: IncomingMessage):
                pass
            ```

        Args:
            routing_key: The routing key to bind messages for.

        Returns:
            A decorator for registering a function to receive callbacks.
        """
        return self._register(routing_key)

    async def publish_message(self, routing_key: str, payload: dict) -> None:
        """Publish a message to the given routing key.

        Args:
            routing_key: The routing key to send the message to.
            payload: The message payload.

        Returns:
            None
        """
        await self._publish_message(routing_key, payload)
