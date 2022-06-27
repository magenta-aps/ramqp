# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=too-few-public-methods,protected-access
"""This module contains the Generic AMQPSystem."""
from .abstract_amqpsystem import AbstractAMQPRouter
from .abstract_amqpsystem import AbstractAMQPSystem


class AMQPRouter(AbstractAMQPRouter):
    """Generic AMQPRouter."""

    register = AbstractAMQPRouter._register


class AMQPSystem(AbstractAMQPSystem[AMQPRouter]):
    """Generic AMQPSystem."""

    router_cls = AMQPRouter
    publish_message = AbstractAMQPSystem._publish_message
