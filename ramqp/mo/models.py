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
from typing import overload
from typing import Tuple
from uuid import UUID

from more_itertools import one
from pydantic import BaseModel
from pydantic.dataclasses import dataclass

from ..metrics import exception_mo_routing_counter


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


@dataclass
class MORoutingKey:
    """MO Routing Key format.

    Args:
        service_type: The service type component.
        object_type: The object type to component.
        request_type: The request type to component.
    """

    service_type: ServiceType
    object_type: ObjectType
    request_type: RequestType

    @classmethod
    def from_tuple(
        cls, routing_key_tuple: Tuple[ServiceType, ObjectType, RequestType]
    ) -> "MORoutingKey":
        """Convert the given routing_key_tuple to a MORoutingKey.

        Args:
            routing_key_tuple: The routing tuple to convert.

        Returns:
            The equivalent MORoutingKey.
        """
        return cls(*routing_key_tuple)

    @classmethod
    def from_routing_key(cls, routing_key: str) -> "MORoutingKey":
        """Convert the given routing_key to a MORoutingKey.

        Raises:
            ValueError: Raised if the routing_key does not contain 2 periods.
            ValueError: Raised if the routing_key components cannot be parsed.

        Args:
            routing_key: The routing key to convert.

        Returns:
            The equivalent MORoutingKey.
        """
        with exception_mo_routing_counter.labels(routing_key).count_exceptions():
            parts = routing_key.split(".")
            if len(parts) != 3:
                raise ValueError("Expected a three tuple!")
            service_str, object_str, request_str = parts
            return cls(
                ServiceType(service_str),
                ObjectType(object_str),
                RequestType(request_str),
            )

    @overload
    @classmethod
    def build(
        cls,
        service_type: ServiceType,
        object_type: ObjectType,
        request_type: RequestType,
    ) -> "MORoutingKey":  # pragma: no cover
        ...

    @overload
    @classmethod
    def build(
        cls, routing_key_tuple: Tuple[ServiceType, ObjectType, RequestType]
    ) -> "MORoutingKey":  # pragma: no cover
        ...

    @overload
    @classmethod
    def build(cls, routing_key: str) -> "MORoutingKey":  # pragma: no cover
        ...

    @overload
    @classmethod
    def build(
        cls, mo_routing_key: "MORoutingKey"
    ) -> "MORoutingKey":  # pragma: no cover
        ...

    @classmethod
    def build(cls, *args: Any, **kwargs: Any) -> "MORoutingKey":
        """Attempt to construct a MORoutingKey.

        Returns:
            The constructed MORoutingKey.
        """
        num_arguments = len(args) + len(kwargs)

        if num_arguments == 3:
            return cls(*args, **kwargs)

        assert num_arguments == 1
        if args:
            arg = one(args)
        else:
            arg = kwargs[one(kwargs)]

        if isinstance(arg, MORoutingKey):
            return arg
        if isinstance(arg, tuple):
            return cls.from_tuple(
                cast(Tuple[ServiceType, ObjectType, RequestType], arg)
            )

        assert isinstance(arg, str)
        return cls.from_routing_key(arg)

    def __str__(self) -> str:
        """Convert this object to a routing_key.

        Args:
            self: The object to be converted.

        Returns:
            The equivalent routing key.
        """
        return ".".join(
            [self.service_type.value, self.object_type.value, self.request_type.value]
        )


MOCallbackType = Callable[..., Awaitable]
