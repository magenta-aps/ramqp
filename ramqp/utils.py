# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module contains the utilities."""
from typing import Awaitable
from typing import Callable

import structlog

logger = structlog.get_logger()


CallbackType = Callable[..., Awaitable]


def function_to_name(function: Callable) -> str:
    """Get a uniquely qualified name for a given function."""
    return function.__name__
