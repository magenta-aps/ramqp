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
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID

from aio_pika import IncomingMessage
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from pydantic import parse_raw_as
from pydantic import validator

from .amqpsystem import AMQPSystem


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

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }

    @validator("time", pre=True)
    def time_validate(cls, v: Any) -> Any:
        return datetime.fromisoformat(v)


MOCallbackType = Callable[[str, PayloadType], Awaitable]


def mo_routing_key(
    service_type: ServiceType, object_type: ObjectType, request_type: RequestType
) -> str:
    return ".".join([service_type, object_type, request_type])


class MOAMQPSystem:
    def __init__(self, amqp_system: Optional[AMQPSystem] = None) -> None:
        self._amqp_system = amqp_system or AMQPSystem()

    def has_started(self) -> bool:
        return self._amqp_system.has_started()

    def register(
        self,
        service_type: ServiceType,
        object_type: ObjectType,
        request_type: RequestType,
    ) -> Callable:
        routing_key = mo_routing_key(service_type, object_type, request_type)
        amqp_decorator = self._amqp_system.register(routing_key)

        def decorator(function: MOCallbackType) -> MOCallbackType:
            async def wrapper(message: IncomingMessage) -> None:
                assert message.routing_key is not None
                payload = parse_raw_as(PayloadType, message.body)
                await function(message.routing_key, payload)

            wrapper.__name__ = function.__name__
            amqp_decorator(wrapper)
            return function

        return decorator

    async def stop(self) -> None:
        await self._amqp_system.stop()

    async def start(self, *args: List[Any], **kwargs: Dict[str, Any]) -> None:
        await self._amqp_system.start(*args, **kwargs)

    def run_forever(self, *args: List[Any], **kwargs: Dict[str, Any]) -> None:
        self._amqp_system.run_forever(*args, **kwargs)

    async def publish_message(
        self,
        service_type: ServiceType,
        object_type: ObjectType,
        request_type: RequestType,
        payload: PayloadType,
    ) -> None:
        routing_key = mo_routing_key(service_type, object_type, request_type)
        payload_obj = jsonable_encoder(payload)
        await self._amqp_system.publish_message(routing_key, payload_obj)
