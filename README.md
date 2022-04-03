<!--
SPDX-FileCopyrightText: 2021 Magenta ApS <https://magenta.dk>
SPDX-License-Identifier: MPL-2.0
-->

# Rammearkitektur AMQP

Rammearkitektur AMQP library is a wrapper around `aio_pika`.

## Usage

Receiving:
```
amqp_system = AMQPSystem(queue_name="my-program")

# Configure the callback function to receive messages for the two routing keys.
# If an exception is thrown from the function, the message is not acknowledged.
# Thus it will be retried immediately.
@amqp_system.register("my.routing.key")
@amqp_system.register("my.other.routing.key")
async def callback_function(routing_key: str, payload: dict) -> None:
    pass

await amqp_system.run_forever()
```

Sending:
```
amqp_system = AMQPSystem(queue_name="my-program")
await amqp_system.start()
await amqp_system.publish_message("my.routing.key", {"key": "value"})
await amqp_system.stop()
```

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
