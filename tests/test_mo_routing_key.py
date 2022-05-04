# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the from_routing_key and to_routing_key functions."""
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ramqp.mo_models import MORoutingKey
from ramqp.mo_models import ObjectType
from ramqp.mo_models import RequestType
from ramqp.mo_models import ServiceType


def test_to_routing_key(mo_routing_key: MORoutingKey) -> None:
    """Test that to_routing_key works on expected input."""
    assert str(mo_routing_key) == "employee.address.create"


def test_from_routing_key() -> None:
    """Test that from_routing_key works on expected input."""
    routing_key = "employee.address.create"

    mo_routing_key_from_routing_key = MORoutingKey.from_routing_key(routing_key)
    mo_routing_key1 = MORoutingKey.build(routing_key)
    mo_routing_key2 = MORoutingKey.build(routing_key=routing_key)
    assert mo_routing_key1 == mo_routing_key2
    assert mo_routing_key1 == mo_routing_key_from_routing_key
    assert mo_routing_key1.service_type == ServiceType.EMPLOYEE
    assert mo_routing_key1.object_type == ObjectType.ADDRESS
    assert mo_routing_key1.request_type == RequestType.CREATE


def test_from_routing_key_too_few_parts() -> None:
    """Test that from_routing_key fails with too short routing_keys."""
    routing_key = "a.b"

    with pytest.raises(ValueError) as exc_info:
        MORoutingKey.from_routing_key(routing_key)
    assert str(exc_info.value) == "Expected a three tuple!"

    with pytest.raises(ValueError) as exc_info:
        MORoutingKey.build(routing_key)
    assert str(exc_info.value) == "Expected a three tuple!"


def test_from_routing_key_too_many_parts() -> None:
    """Test that from_routing_key fails with too long routing_keys."""
    routing_key = "a.b.c.d"

    with pytest.raises(ValueError) as exc_info:
        MORoutingKey.from_routing_key(routing_key)
    assert str(exc_info.value) == "Expected a three tuple!"

    with pytest.raises(ValueError) as exc_info:
        MORoutingKey.build(routing_key)
    assert str(exc_info.value) == "Expected a three tuple!"


def test_from_routing_key_invalid_values() -> None:
    """Test that from_routing_key fails with non-enum values."""
    routing_key = "a.b.c"
    with pytest.raises(ValueError) as exc_info:
        MORoutingKey.from_routing_key(routing_key)
    assert str(exc_info.value) == "'a' is not a valid ServiceType"

    with pytest.raises(ValueError) as exc_info:
        MORoutingKey.build(routing_key)
    assert str(exc_info.value) == "'a' is not a valid ServiceType"

    routing_key = "employee.b.c"
    with pytest.raises(ValueError) as exc_info:
        MORoutingKey.from_routing_key(routing_key)
    assert str(exc_info.value) == "'b' is not a valid ObjectType"

    with pytest.raises(ValueError) as exc_info:
        MORoutingKey.build(routing_key)
    assert str(exc_info.value) == "'b' is not a valid ObjectType"

    routing_key = "employee.address.c"
    with pytest.raises(ValueError) as exc_info:
        MORoutingKey.from_routing_key(routing_key)
    assert str(exc_info.value) == "'c' is not a valid RequestType"

    with pytest.raises(ValueError) as exc_info:
        MORoutingKey.build(routing_key)
    assert str(exc_info.value) == "'c' is not a valid RequestType"


@given(st.from_type(MORoutingKey))
def test_from_routing_key_inverts_str(routing_key: MORoutingKey) -> None:
    """Test that from_routing_key inverts str."""
    mo_routing_key1 = MORoutingKey.build(str(routing_key))
    mo_routing_key2 = MORoutingKey.from_routing_key(str(routing_key))
    assert mo_routing_key1 == mo_routing_key2
    assert mo_routing_key1 == routing_key


@given(st.from_type(ServiceType), st.from_type(ObjectType), st.from_type(RequestType))
def test_build_from_parts_and_tuple(
    service_type: ServiceType, object_type: ObjectType, request_type: RequestType
) -> None:
    """Test that we construct as expected via constructor and from_tuple."""
    routing_key = MORoutingKey(
        service_type=service_type, object_type=object_type, request_type=request_type
    )

    mo_routing_key1 = MORoutingKey.build(service_type, object_type, request_type)
    mo_routing_key2 = MORoutingKey(service_type, object_type, request_type)
    assert mo_routing_key1 == mo_routing_key2
    assert mo_routing_key1 == routing_key

    mo_routing_key1 = MORoutingKey.build((service_type, object_type, request_type))
    mo_routing_key2 = MORoutingKey.from_tuple((service_type, object_type, request_type))
    assert mo_routing_key1 == mo_routing_key2
    assert mo_routing_key1 == routing_key

    mo_routing_key1 = MORoutingKey.build(
        service_type=service_type, object_type=object_type, request_type=request_type
    )
    assert mo_routing_key1 == routing_key
