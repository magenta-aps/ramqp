# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the from_routing_key and to_routing_key functions."""
from typing import Any
from typing import cast
from typing import Type

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ramqp.moqp import from_routing_key
from ramqp.moqp import MORoutingTuple
from ramqp.moqp import ObjectType
from ramqp.moqp import RequestType
from ramqp.moqp import ServiceType
from ramqp.moqp import to_routing_key


def test_to_routing_key(mo_routing_tuple: MORoutingTuple) -> None:
    """Test that to_routing_key works on expected input."""
    routing_key = to_routing_key(*mo_routing_tuple)
    assert routing_key == "EMPLOYEE.ADDRESS.CREATE"


def test_from_routing_key() -> None:
    """Test that from_routing_key works on expected input."""
    service_type, object_type, request_type = from_routing_key(
        "EMPLOYEE.ADDRESS.CREATE"
    )
    assert service_type == ServiceType.EMPLOYEE
    assert object_type == ObjectType.ADDRESS
    assert request_type == RequestType.CREATE


def test_from_routing_key_too_few_parts() -> None:
    """Test that from_routing_key fails with too short routing_keys."""
    with pytest.raises(ValueError) as exc_info:
        from_routing_key("a.b")
    assert str(exc_info.value) == "Expected a three tuple!"


def test_from_routing_key_too_many_parts() -> None:
    """Test that from_routing_key fails with too long routing_keys."""
    with pytest.raises(ValueError) as exc_info:
        from_routing_key("a.b.c.d")
    assert str(exc_info.value) == "Expected a three tuple!"


def test_from_routing_key_invalid_values() -> None:
    """Test that from_routing_key fails with non-enum values."""
    with pytest.raises(ValueError) as exc_info:
        from_routing_key("a.b.c")
    assert str(exc_info.value) == "'a' is not a valid ServiceType"

    with pytest.raises(ValueError) as exc_info:
        from_routing_key("EMPLOYEE.b.c")
    assert str(exc_info.value) == "'b' is not a valid ObjectType"

    with pytest.raises(ValueError) as exc_info:
        from_routing_key("EMPLOYEE.ADDRESS.c")
    assert str(exc_info.value) == "'c' is not a valid RequestType"


@given(st.from_type(cast(Type[Any], MORoutingTuple)))
def test_from_routing_key_inverts_to_routing_key(routing_tuple: MORoutingTuple) -> None:
    """Test that from_routing_key inverts to_routing_key."""
    assert from_routing_key(to_routing_key(*routing_tuple)) == routing_tuple
