# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=too-few-public-methods
"""This module contains the MO specific AMQP models."""
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


class AutoLowerNameEnum(Enum):
    """Enum subclass which lets `auto` assign the enum key as value.

    From: https://docs.python.org/3/library/enum.html#using-automatic-values

    Example:
        ```
        class Color(Enum):
            RED = auto()
            BLUE = auto()

        print(list(Color))
        ```
        Yields:
        ```
        [<Color.RED: 1>, <Color.BLUE: 2>]
        ```
        Whereas:
        ```
        class Color(str, AutoLowerNameEnum):
            RED = auto()
            BLUE = auto()

        print(list(Color))
        ```
        Yields:
        ```
        [<Color.RED: 'red'>, <Color.BLUE: 'blue'>]
        ```
    """

    @staticmethod
    def _generate_next_value_(
        name: Any, start: int, count: int, last_values: List[Any]
    ) -> str:
        # The next `auto` value is simply the key name
        return cast(str, name).lower()


@unique
class ServiceType(str, AutoLowerNameEnum):
    """The MO service types.

    Describes the root object for the object the operation was executed on.
    """

    EMPLOYEE: str = cast(str, auto())
    ORG_UNIT: str = cast(str, auto())
    WILDCARD: str = "*"


@unique
class ObjectType(str, AutoLowerNameEnum):
    """The MO object types.

    Describes the object the operation was executed on.
    """

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
class RequestType(str, AutoLowerNameEnum):
    """The MO request types.

    Describes the type of operation which was executed.

    Args:
        CREATE: Something was created.
        EDIT: Something was edited.
        TERMINATE: Something was terminated.
        REFRESH: Something was requested to be refreshed.
        WILDCARD: Something happened.
    """

    CREATE: str = cast(str, auto())
    EDIT: str = cast(str, auto())
    TERMINATE: str = cast(str, auto())
    REFRESH: str = cast(str, auto())
    WILDCARD: str = "*"


class PayloadType(BaseModel):
    """MO AMQP message format.

    Args:
        uuid: The UUID of the org-unit or employee.
        object_uuid: The UUID of the object itself.
        time: Time when MO emitted the message.
    """

    uuid: UUID
    object_uuid: UUID
    time: datetime


MOCallbackType = Callable[
    [ServiceType, ObjectType, RequestType, PayloadType], Awaitable
]
