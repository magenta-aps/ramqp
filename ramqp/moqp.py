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


MOCallbackType = Callable[[str, PayloadType], Awaitable]


def mo_routing_key(
    service_type: ServiceType, object_type: ObjectType, request_type: RequestType
) -> str:
    return ".".join([service_type, object_type, request_type])


class MOAMQPSystem(AbstractAMQPSystem):
    def register(
        self,
        service_type: ServiceType,
        object_type: ObjectType,
        request_type: RequestType,
    ) -> Callable:
        routing_key = mo_routing_key(service_type, object_type, request_type)
        amqp_decorator = self._register(routing_key)

        def decorator(function: MOCallbackType) -> MOCallbackType:
            async def wrapper(message: IncomingMessage) -> None:
                assert message.routing_key is not None
                payload = parse_raw_as(PayloadType, message.body)
                await function(message.routing_key, payload)

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
        routing_key = mo_routing_key(service_type, object_type, request_type)
        payload_obj = jsonable_encoder(payload)
        await self._publish_message(routing_key, payload_obj)
