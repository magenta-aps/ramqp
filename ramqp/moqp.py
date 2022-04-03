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

from pydantic import BaseModel
from pydantic import validator


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
    def time_validate(cls, v):
        return datetime.fromisoformat(v)


CallbackType = Callable[[ServiceType, ObjectType, RequestType, PayloadType], Awaitable]
