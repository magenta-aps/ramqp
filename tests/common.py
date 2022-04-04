# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
from aio_pika import IncomingMessage


async def callback_func(message: IncomingMessage) -> None:
    pass


async def callback_func2(message: IncomingMessage) -> None:
    pass
