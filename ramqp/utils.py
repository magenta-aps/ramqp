# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import asyncio
import json
from asyncio import TimerHandle
from functools import wraps
from typing import Dict
from typing import List
from typing import Optional

import structlog
from aio_pika import IncomingMessage
from prometheus_client import Counter


logger = structlog.get_logger()


exception_parse_counter = Counter(
    "amqp_exceptions_parse",
    "Exception counter",
    ["routing_key"],
)


class Batch:
    def __init__(
        self,
        callback,
        batch_refresh_time: int = 5,
        batch_max_time: int = 60,
        batch_max_size: int = 10,
    ):
        self.callback = callback

        self.batch_refresh_time = batch_refresh_time
        self.batch_max_time = batch_max_time
        self.batch_max_size = batch_max_size

        self.refresh_dispatch: Optional[TimerHandle] = None
        self.max_time_dispatch: Optional[TimerHandle] = None
        self.payloads: List[dict] = []

    def append(self, payload: dict) -> None:
        loop = asyncio.get_running_loop()

        if self.refresh_dispatch:
            self.refresh_dispatch.cancel()
        self.refresh_dispatch = loop.call_later(
            self.batch_refresh_time, self.dispatch_refresh_time
        )

        if self.max_time_dispatch is None:
            self.max_time_dispatched = loop.call_at(
                loop.time() + self.batch_max_time, self.dispatch_max_time
            )

        self.payloads.append(payload)
        if len(self.payloads) == self.batch_max_size:
            self.dispatch_max_length()

    def clear(self) -> None:
        if self.refresh_dispatch:
            self.refresh_dispatch.cancel()
        self.refresh_dispatch = None

        if self.max_time_dispatch:
            self.max_time_dispatch.cancel()
        self.max_time_dispatch = None

        self.payloads = []

    # Dispatch functions
    def dispatch_refresh_time(self):
        logger.debug("Dispatched by refresh time timer")
        return self.dispatch()

    def dispatch_max_time(self):
        logger.debug("Dispatched by max time timer")
        return self.dispatch()

    def dispatch_max_length(self):
        logger.debug("Dispatched by max length")
        return self.dispatch()

    def dispatch(self):
        payloads = self.payloads
        self.clear()
        return self.callback(payloads)


def bulk_messages(*args, **kwargs):
    """Bulk messages before calling wrapped function."""

    def decorator(function):
        batches: Dict[str, Batch] = {}

        @wraps(function)
        async def wrapper(routing_key: str, payload: dict) -> None:
            batch = batches.setdefault(routing_key, Batch(function, *args, **kwargs))
            batch.append(payload)

        return wrapper

    return decorator


def parse_payload(message: IncomingMessage) -> dict:
    with exception_parse_counter.labels(routing_key).count_exceptions():
        decoded = message.body.decode("utf-8")
        payload = json.loads(decoded)
    logger.debug("Parsed message", routing_key=routing_key, payload=payload)
    return payload


def pass_arguments(*arguments: List[str]):
    argument_map = {
        "message": lambda message: message,
        "routing_key": lambda message: message.routing_key,
        "message_id": lambda message: message.message_id,
        "payload": lambda message: parse_payload(message),
    }
    for argument in arguments:
        assert argument in argument_map

    def decorator(function):
        async def message_receiver(routing_key: str, message: IncomingMessage) -> None:
            kwargs = {}
            for argument in arguments:
                kwargs[argument] = argument_map[argument](message)
            await function(**kwargs)

        return message_receiver

    return decorator
