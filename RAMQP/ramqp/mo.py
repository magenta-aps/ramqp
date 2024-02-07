# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=too-few-public-methods
"""This module contains the MO specific AMQPSystem."""
from collections.abc import Callable
from datetime import datetime
from typing import Annotated
from typing import Literal
from uuid import UUID

from fastapi import Depends
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

from ramqp.abstract import AbstractAMQPSystem
from ramqp.abstract import AbstractPublishMixin
from ramqp.abstract import AbstractRouter
from ramqp.depends import get_payload_as_type
from ramqp.depends import get_routing_key
from ramqp.utils import CallbackType

# >>> print("\n".join(((f"\"{'.'.join(x)}\"," for x in itertools.product(["employee", "org_unit", "*"], ["address", "association", "employee", "engagement", "it", "kle", "leave", "manager", "owner", "org_unit", "related_unit", "role", "*"], ["create", "edit", "terminate", "refresh", "*"])))))  # noqa: E501
_MORoutingKeyOld = Literal[
    "employee.address.create",
    "employee.address.edit",
    "employee.address.terminate",
    "employee.address.refresh",
    "employee.address.*",
    "employee.association.create",
    "employee.association.edit",
    "employee.association.terminate",
    "employee.association.refresh",
    "employee.association.*",
    "employee.employee.create",
    "employee.employee.edit",
    "employee.employee.terminate",
    "employee.employee.refresh",
    "employee.employee.*",
    "employee.engagement.create",
    "employee.engagement.edit",
    "employee.engagement.terminate",
    "employee.engagement.refresh",
    "employee.engagement.*",
    "employee.it.create",
    "employee.it.edit",
    "employee.it.terminate",
    "employee.it.refresh",
    "employee.it.*",
    "employee.kle.create",
    "employee.kle.edit",
    "employee.kle.terminate",
    "employee.kle.refresh",
    "employee.kle.*",
    "employee.leave.create",
    "employee.leave.edit",
    "employee.leave.terminate",
    "employee.leave.refresh",
    "employee.leave.*",
    "employee.manager.create",
    "employee.manager.edit",
    "employee.manager.terminate",
    "employee.manager.refresh",
    "employee.manager.*",
    "employee.owner.create",
    "employee.owner.edit",
    "employee.owner.terminate",
    "employee.owner.refresh",
    "employee.owner.*",
    "employee.org_unit.create",
    "employee.org_unit.edit",
    "employee.org_unit.terminate",
    "employee.org_unit.refresh",
    "employee.org_unit.*",
    "employee.related_unit.create",
    "employee.related_unit.edit",
    "employee.related_unit.terminate",
    "employee.related_unit.refresh",
    "employee.related_unit.*",
    "employee.role.create",
    "employee.role.edit",
    "employee.role.terminate",
    "employee.role.refresh",
    "employee.role.*",
    "employee.*.create",
    "employee.*.edit",
    "employee.*.terminate",
    "employee.*.refresh",
    "employee.*.*",
    "org_unit.address.create",
    "org_unit.address.edit",
    "org_unit.address.terminate",
    "org_unit.address.refresh",
    "org_unit.address.*",
    "org_unit.association.create",
    "org_unit.association.edit",
    "org_unit.association.terminate",
    "org_unit.association.refresh",
    "org_unit.association.*",
    "org_unit.employee.create",
    "org_unit.employee.edit",
    "org_unit.employee.terminate",
    "org_unit.employee.refresh",
    "org_unit.employee.*",
    "org_unit.engagement.create",
    "org_unit.engagement.edit",
    "org_unit.engagement.terminate",
    "org_unit.engagement.refresh",
    "org_unit.engagement.*",
    "org_unit.it.create",
    "org_unit.it.edit",
    "org_unit.it.terminate",
    "org_unit.it.refresh",
    "org_unit.it.*",
    "org_unit.kle.create",
    "org_unit.kle.edit",
    "org_unit.kle.terminate",
    "org_unit.kle.refresh",
    "org_unit.kle.*",
    "org_unit.leave.create",
    "org_unit.leave.edit",
    "org_unit.leave.terminate",
    "org_unit.leave.refresh",
    "org_unit.leave.*",
    "org_unit.manager.create",
    "org_unit.manager.edit",
    "org_unit.manager.terminate",
    "org_unit.manager.refresh",
    "org_unit.manager.*",
    "org_unit.owner.create",
    "org_unit.owner.edit",
    "org_unit.owner.terminate",
    "org_unit.owner.refresh",
    "org_unit.owner.*",
    "org_unit.org_unit.create",
    "org_unit.org_unit.edit",
    "org_unit.org_unit.terminate",
    "org_unit.org_unit.refresh",
    "org_unit.org_unit.*",
    "org_unit.related_unit.create",
    "org_unit.related_unit.edit",
    "org_unit.related_unit.terminate",
    "org_unit.related_unit.refresh",
    "org_unit.related_unit.*",
    "org_unit.role.create",
    "org_unit.role.edit",
    "org_unit.role.terminate",
    "org_unit.role.refresh",
    "org_unit.role.*",
    "org_unit.*.create",
    "org_unit.*.edit",
    "org_unit.*.terminate",
    "org_unit.*.refresh",
    "org_unit.*.*",
    "*.address.create",
    "*.address.edit",
    "*.address.terminate",
    "*.address.refresh",
    "*.address.*",
    "*.association.create",
    "*.association.edit",
    "*.association.terminate",
    "*.association.refresh",
    "*.association.*",
    "*.employee.create",
    "*.employee.edit",
    "*.employee.terminate",
    "*.employee.refresh",
    "*.employee.*",
    "*.engagement.create",
    "*.engagement.edit",
    "*.engagement.terminate",
    "*.engagement.refresh",
    "*.engagement.*",
    "*.it.create",
    "*.it.edit",
    "*.it.terminate",
    "*.it.refresh",
    "*.it.*",
    "*.kle.create",
    "*.kle.edit",
    "*.kle.terminate",
    "*.kle.refresh",
    "*.kle.*",
    "*.leave.create",
    "*.leave.edit",
    "*.leave.terminate",
    "*.leave.refresh",
    "*.leave.*",
    "*.manager.create",
    "*.manager.edit",
    "*.manager.terminate",
    "*.manager.refresh",
    "*.manager.*",
    "*.owner.create",
    "*.owner.edit",
    "*.owner.terminate",
    "*.owner.refresh",
    "*.owner.*",
    "*.org_unit.create",
    "*.org_unit.edit",
    "*.org_unit.terminate",
    "*.org_unit.refresh",
    "*.org_unit.*",
    "*.related_unit.create",
    "*.related_unit.edit",
    "*.related_unit.terminate",
    "*.related_unit.refresh",
    "*.related_unit.*",
    "*.role.create",
    "*.role.edit",
    "*.role.terminate",
    "*.role.refresh",
    "*.role.*",
    "*.*.create",
    "*.*.edit",
    "*.*.terminate",
    "*.*.refresh",
    "*.*.*",
]

_MORoutingKey = Literal[
    "address",
    "association",
    "class",
    "engagement",
    "facet",
    "itsystem",
    "ituser",
    "kle",
    "leave",
    "manager",
    "manager",
    "org_unit",
    "owner",
    "person",
    "related_unit",
    "role",
]

MORoutingKey = Annotated[_MORoutingKey | _MORoutingKeyOld, Depends(get_routing_key)]


class _PayloadType(BaseModel):
    """MO AMQP message format.

    Args:
        uuid: The UUID of the org-unit or employee.
        object_uuid: The UUID of the object itself.
        time: Time when MO emitted the message.
    """

    uuid: UUID
    object_uuid: UUID
    time: datetime


PayloadType = Annotated[_PayloadType, Depends(get_payload_as_type(_PayloadType))]
# The new MO amqp system payloads are just uuids
PayloadUUID = Annotated[UUID, Depends(get_payload_as_type(UUID))]


class MORouter(AbstractRouter):
    """MO specific Router.

    Has specifically tailored `register` methods, which utilize the MO AMQP
    routing-key structure and payload format.
    """

    def register(
        self, routing_key: MORoutingKey
    ) -> Callable[[CallbackType], CallbackType]:
        """Get a decorator for registering callbacks.

        See the documentation in AbstractRouter for more information.

        Args:
            routing_key: The routing key to bind messages for.

        Returns:
            A decorator for registering a function to receive callbacks.
        """
        return self._register(routing_key)


class MOPublishMixin(AbstractPublishMixin):
    """MO specific PublishMixin.

    Has a specifically tailored `publish_message` method, which utilize the MO AMQP
    routing-key structure and payload format.
    """

    async def publish_message(
        self, routing_key: MORoutingKey, payload: PayloadType
    ) -> None:
        """Publish a message to the given service.object.request tuple.

        Args:
            routing_key: The routing key to send the message to.
            payload: The message payload.
        """
        payload_obj = jsonable_encoder(payload)
        await self._publish_message(str(routing_key), payload_obj)


class MOAMQPSystem(AbstractAMQPSystem[MORouter], MOPublishMixin):
    """MO specific AMQPSystem."""

    router_cls = MORouter
