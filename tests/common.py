# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import random
import string

from aio_pika import IncomingMessage


async def callback_func(message: IncomingMessage) -> None:
    pass


async def callback_func2(message: IncomingMessage) -> None:
    pass


def random_string(length: int = 30) -> str:
    """Generate a random string of characters.

    Args:
        length: The desired length of the string

    Returns:
        A string of random numbers, upper- and lower-case letters.
    """
    return "".join(
        random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
        for _ in range(length)
    )
