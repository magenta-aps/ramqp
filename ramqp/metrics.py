# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module contains the all prometheus metrics."""
from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram

exception_callback_counter = Counter(
    "amqp_exceptions_callback",
    "Exception counter",
    ["routing_key", "function"],
)
exception_parse_counter = Counter(
    "amqp_exceptions_parse",
    "Exception counter",
    ["routing_key"],
)
processing_time = Histogram(
    "amqp_processing_seconds",
    "Time spent running callback",
    ["routing_key", "function"],
)
processing_inprogress = Gauge(
    "amqp_inprogress",
    "Number of callbacks currently running",
    ["routing_key", "function"],
)
processing_calls = Counter(
    "amqp_calls",
    "Number of callbacks made",
    ["routing_key", "function"],
)
last_on_message = Gauge("amqp_last_on_message", "Timestamp of the last on_message call")
last_periodic = Gauge("amqp_last_periodic", "Timestamp of the last periodic call")
last_loop_periodic = Gauge(
    "amqp_last_loop_periodic", "Timestamp (monotonic) of the last periodic call"
)
backlog_count = Gauge(
    "amqp_backlog",
    "Number of messages waiting for processing in the backlog",
    ["function"],
)
routes_bound = Counter(
    "amqp_routes_bound", "Number of routing-keys bound to the queue", ["function"]
)
callbacks_registered = Counter(
    "amqp_callbacks_registered", "Number of callbacks registered", ["routing_key"]
)
