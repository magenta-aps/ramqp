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
        return self._register(routing_key)

    async def publish_message(self, routing_key: str, payload: dict) -> None:
        await self._publish_message(routing_key, payload)
