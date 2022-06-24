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
from typing import Any
from ramqp import AMQPSystem

amqp_system = AMQPSystem(amqp_queue_prefix="my-program")

# Configure the callback function to receive messages for the two routing keys.
# If an exception is thrown from the function, the message is not acknowledged.
# Thus, it will be retried immediately.
@amqp_system.register("my.routing.key")
@amqp_system.register("my.other.routing.key")
async def callback_function(routing_key: str, **kwargs: Any) -> None:
    pass

amqp_system.run_forever()
```
**NOTE**: `**kwargs` is required in all handlers for forward compatibility: the
framework can add new keywords in the future, and existing handlers should
accept them without breaking, if they do not use them. 
It can be named `**_`  to prevent the "unused variable" warning by linters.


Sending:
```python
from ramqp import AMQPSystem

with AMQPSystem() as amqp_system:
    await amqp_system.publish_message("my.routing.key", {"key": "value"})
```

### MO AMQP
Receiving:
```python
from typing import Any

from ramqp.moqp import MOAMQPSystem
from ramqp.mo_models import MORoutingKey
from ramqp.mo_models import ServiceType
from ramqp.mo_models import ObjectType
from ramqp.mo_models import RequestType
from ramqp.mo_models import PayloadType

amqp_system = MOAMQPSystem(amqp_queue_prefix="my-program")

# Configure the callback function to receive messages for the two routing keys.
# If an exception is thrown from the function, the message is not acknowledged.
# Thus, it will be retried immediately.
@amqp_system.register(ServiceType.EMPLOYEE, ObjectType.ADDRESS, RequestType.EDIT)
@amqp_system.register("employee.it.create")
async def callback_function(
    mo_routing_key: MORoutingKey, payload: PayloadType, **kwargs: Any
) -> None:
    pass

amqp_system.run_forever()
```

Sending:
```python
from uuid import uuid4
from datetime import datetime

from ramqp.moqp import MOAMQPSystem
from ramqp.mo_models import ServiceType
from ramqp.mo_models import ObjectType
from ramqp.mo_models import RequestType
from ramqp.mo_models import PayloadType

payload = PayloadType(uuid=uuid4(), object_uuid=uuid4(), time=datetime.now())

async with MOAMQPSystem() as amqp_system:
    await amqp_system.publish_message(
        ServiceType.EMPLOYEE, ObjectType.ADDRESS, RequestType.EDIT, payload
    )
```

### FastAPI and Additional Context
The `run_forever()` function is not very useful if you would like to serve an
API at the same time. To solve this issue, the main entrypoint of the program is
the ASGI application, with the AMQP system tied to its lifespan. The example
below also shows how additional context can be injected into the AMQP system,
and subsequently retrieved in the handlers:
```python
from contextlib import asynccontextmanager
from functools import partial
from typing import Any

from fastapi import FastAPI
from starlette.requests import Request

from ramqp.moqp import MOAMQPSystem

amqp_system = MOAMQPSystem(amqp_queue_prefix="my-program")


@amqp_system.register("employee.it.create")
async def callback_function(context: dict, **kwargs: Any) -> None:
    print(context["state"].greeting)


def create_app() -> FastAPI:
    context = {"greeting": "Hello, world!"}
    app = FastAPI()
    app.router.lifespan_context = partial(lifespan, context)
    return app


@asynccontextmanager
async def lifespan(context: dict, app: FastAPI):
    app.state.context = context
    async with amqp_system:
        amqp_system.context = context
        yield


app = create_app()


@app.get("/hello")
async def hello(request: Request) -> None:
    return request.app.state.greeting
```
Save the example as `example.py` and try it with `uvicorn example:app`.


### Metrics
RAMQP exports a myraid of prometheus metrics via `prometheus/client_python`.

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
