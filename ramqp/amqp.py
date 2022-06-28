# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=too-few-public-methods,protected-access
"""This module contains the Generic AMQPSystem."""
from .abstract import AbstractAMQPSystem
from .abstract import AbstractPublishMixin
from .abstract import AbstractRouter


class Router(AbstractRouter):
    """Generic Router."""

    register = AbstractRouter._register


class PublishMixin(AbstractPublishMixin):
    """Generic PublishMixin."""

    publish_message = AbstractPublishMixin._publish_message


class AMQPSystem(AbstractAMQPSystem[Router], PublishMixin):
    """Generic AMQPSystem."""

    router_cls = Router
