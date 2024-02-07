<!--
SPDX-FileCopyrightText: 2021 Magenta ApS <https://magenta.dk>
SPDX-License-Identifier: MPL-2.0
-->

# Rammearkitektur AMQP
Rammearkitektur AMQP (RAMQP) is an opinionated library for AMQP.

It is implemented as a thin wrapper around `aio_pika`, with a generic and a MO
specific AMQPSystem abstract, the MO abstraction being implementing using a thin
wrapper around the generic abstraction.

## Usage

### Generic
Receiving:

```python
import asyncio

from ramqp import AMQPSystem
from ramqp import Router
from ramqp.config import AMQPConnectionSettings
from ramqp.depends import RoutingKey

router = Router()


# Configure the callback function to receive messages for the two routing keys.
# If an exception is thrown from the function, the message is not acknowledged.
# Thus, it will be retried immediately.
@router.register("my.routing.key")
@router.register("my.other.routing.key")
async def callback_function(routing_key: RoutingKey) -> None:
    pass


async def main() -> None:
    settings = AMQPConnectionSettings(url=..., queue_prefix="my-program")
    async with AMQPSystem(settings=settings, router=router) as amqp_system:
        await amqp_system.run_forever()


asyncio.run(main())
```


Sending:
```python
from ramqp import AMQPSystem

with AMQPSystem(...) as amqp_system:
    await amqp_system.publish_message("my.routing.key", {"key": "value"})
```

### Dependency Injection
The callback handlers support
[FastAPI dependency injection](https://fastapi.tiangolo.com/tutorial/dependencies).
This allows handlers to request exactly the data that they need, as seen with
FastAPI dependencies or PyTest fixtures. A callback may look like:
```python
from ramqp.mo import MORoutingKey
from ramqp.mo import PayloadType

async def callback(mo_routing_key: MORoutingKey, payload: PayloadType):
    ...
```

Experienced FastAPI developers might wonder how this works without the `Depends`
function. Indeed, this less verbose pattern was
[introduced in FastAPI v0.95](https://fastapi.tiangolo.com/release-notes/#0950),
and works by defining the dependency directly on the type using the `Annotated`
mechanism from [PEP593](https://peps.python.org/pep-0593/). For example:
```python
MORoutingKey = Annotated[MORoutingKey, Depends(get_routing_key)]
PayloadType = Annotated[PayloadType, Depends(get_payload_as_type(PayloadType))]
```
whereby the previous example is equivalent to
```python
async def callback(
    mo_routing_key: MORoutingKey = Depends(get_routing_key),
    payload: PayloadType = Depends(get_payload_as_type(PayloadType))
):
    ...
```
.

Reference documentation should be made available for these types in the future,
but for now they can be found mainly in `ramqp/depends.py` and `ramqp/mo.py`.


### Context
```python
import asyncio
from typing import Annotated

import httpx
from fastapi import Depends

from ramqp import AMQPSystem
from ramqp import Router
from ramqp.depends import Context
from ramqp.depends import from_context

router = Router()

async def main() -> None:
    async with httpx.AsyncClient() as client:
        context = {
            "client": client,
        }
        async with AMQPSystem(..., context=context) as amqp_system:
            await amqp_system.run_forever()


HTTPXClient = Annotated[httpx.AsyncClient, Depends(from_context("client"))]

@router.register("my.routing.key")
async def callback_function(context: Context, client: HTTPXClient) -> None:
    pass

asyncio.run(main())
```


### Settings
In most cases, `AMQPConnectionSettings` is probably initialised by being
included in the `BaseSettings` of the application using the library. The `url`
parameter of the `AMQPConnectionSettings` object can be given as a single URL
string or as individual structured fields. Consider the following:
```python
from pydantic import BaseSettings

from ramqp.config import AMQPConnectionSettings

# BaseSettings makes the entire model initialisable using environment variables
class Settings(BaseSettings):
    amqp: AMQPConnectionSettings

    class Config:
        env_nested_delimiter = "__"  # allows setting e.g. AMQP__URL__HOST=foo

settings = Settings()
```
The above would work with either multiple structured environment variables
```
AMQP__URL__SCHEME=amqp
AMQP__URL__USER=guest
AMQP__URL__PASSWORD=guest
AMQP__URL__HOST=msg_broker
AMQP__URL__PORT=5672
AMQP__URL__VHOST=os2mo
```
or a single URL definition
```
AMQP__URL=amqp://guest:guest@msg_broker:5672/os2mo
```

### MO AMQP
Receiving:

```python
import asyncio

from ramqp.config import AMQPConnectionSettings
from ramqp.mo import MOAMQPSystem
from ramqp.mo import MORouter
from ramqp.mo import MORoutingKey
from ramqp.mo import PayloadType

router = MORouter()


# Configure the callback function to receive messages for the two routing keys.
# If an exception is thrown from the function, the message is not acknowledged.
# Thus, it will be retried immediately.
@router.register("employee.address.edit")
@router.register("employee.it.create")
async def callback_function(
    mo_routing_key: MORoutingKey, payload: PayloadType
) -> None:
    pass


async def main() -> None:
    settings = AMQPConnectionSettings(url=..., queue_prefix="my-program")
    async with MOAMQPSystem(settings=settings, router=router) as amqp_system:
        await amqp_system.run_forever()


asyncio.run(main())
```

Sending:

```python
from datetime import datetime
from uuid import uuid4

from ramqp.mo import MOAMQPSystem
from ramqp.mo import PayloadType

payload = PayloadType(uuid=uuid4(), object_uuid=uuid4(), time=datetime.now())

async with MOAMQPSystem(...) as amqp_system:
    await amqp_system.publish_message("employee.address.edit", payload)
```


### Metrics
RAMQP exports a myriad of prometheus metrics via `prometheus/client_python`.

These can be exported using:
```
from prometheus_client import start_http_server

start_http_server(8000)
```
Or similar, see the promethues client library for details.


## Development

### Prerequisites
- [Poetry](https://github.com/python-poetry/poetry)

### Getting Started
1. Clone the repository:
```
git clone git@git.magenta.dk:rammearkitektur/ramqp.git
```

2. Install all dependencies:
```
poetry install
```

3. Set up pre-commit:
```
poetry run pre-commit install
```

### Running the tests
You use `poetry` and `pytest` to run the tests:

`poetry run pytest`

You can also run specific files

`poetry run pytest tests/<test_folder>/<test_file.py>`

and even use filtering with `-k`

`poetry run pytest -k "Manager"`

You can use the flags `-vx` where `v` prints the test & `x` makes the test stop if any tests fails (Verbose, X-fail)

#### Running the integration tests
To run the integration tests, an AMQP instance must be available.

If an instance is already available, it can be used by configuring the `AMQP_URL`
environmental variable. Alternatively a RabbitMQ can be started in docker, using:
```
docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

## Versioning
This project uses [Semantic Versioning](https://semver.org/) with the following strategy:
- MAJOR: Incompatible changes to existing data models
- MINOR: Backwards compatible updates to existing data models OR new models added
- PATCH: Backwards compatible bug fixes


## Authors
Magenta ApS <https://magenta.dk>


## License
This project uses: [MPL-2.0](MPL-2.0.txt)

This project uses [REUSE](https://reuse.software) for licensing.
All licenses can be found in the [LICENSES folder](LICENSES/) of the project.
