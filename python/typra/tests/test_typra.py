"""Tests for the native `typra` extension module."""

from __future__ import annotations

import re

import pytest

import typra


def test_module_docstring() -> None:
    assert isinstance(typra.__doc__, str)
    assert len(typra.__doc__) > 0
    assert "Typra" in typra.__doc__


def test_version_is_semver() -> None:
    v = typra.__version__
    assert isinstance(v, str)
    # Workspace / Cargo release versions (e.g. 0.1.0)
    assert re.match(r"^\d+\.\d+\.\d+", v), f"unexpected __version__: {v!r}"


def test_version_parts_numeric() -> None:
    major, minor, patch, *_rest = typra.__version__.split(".")
    assert major.isdigit()
    assert minor.isdigit()
    assert patch.split("+")[0].split("-")[0].isdigit()


def test_module_has_expected_attributes() -> None:
    assert hasattr(typra, "__version__")
    assert hasattr(typra, "__doc__")


@pytest.mark.parametrize("name", ("__version__", "__doc__"))
def test_attributes_are_not_none(name: str) -> None:
    assert getattr(typra, name) is not None
