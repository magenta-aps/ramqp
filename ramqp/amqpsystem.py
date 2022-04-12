# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module contains the Generic AMQPSystem."""
from .abstract_amqpsystem import AbstractAMQPSystem


class AMQPSystem(AbstractAMQPSystem):
    """Generic AMQPSystem."""

    register = AbstractAMQPSystem._register
    publish_message = AbstractAMQPSystem._publish_message
