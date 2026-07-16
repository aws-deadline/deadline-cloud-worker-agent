# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from typing import Optional

import pytest

from deadline_worker_agent._session_runtime_kind import SessionRuntimeKind
from deadline_worker_agent.sessions.runtime import select_runtime


@pytest.mark.parametrize(
    ("configured", "hint", "expected"),
    [
        pytest.param(
            SessionRuntimeKind.PYTHON,
            "rust",
            SessionRuntimeKind.PYTHON,
            id="python-mode-ignores-hint",
        ),
        pytest.param(
            SessionRuntimeKind.PYTHON, None, SessionRuntimeKind.PYTHON, id="python-mode-no-hint"
        ),
        pytest.param(
            SessionRuntimeKind.RUST,
            "pythonexpr",
            SessionRuntimeKind.RUST,
            id="rust-mode-ignores-hint",
        ),
        pytest.param(
            SessionRuntimeKind.RUST, None, SessionRuntimeKind.RUST, id="rust-mode-no-hint"
        ),
        pytest.param(
            SessionRuntimeKind.SERVICE_SELECTED,
            "rust",
            SessionRuntimeKind.RUST,
            id="service-selected-follows-rust-hint",
        ),
        pytest.param(
            SessionRuntimeKind.SERVICE_SELECTED,
            "pythonexpr",
            SessionRuntimeKind.PYTHON,
            id="service-selected-follows-pythonexpr-hint",
        ),
        pytest.param(
            SessionRuntimeKind.SERVICE_SELECTED,
            None,
            SessionRuntimeKind.PYTHON,
            id="service-selected-defaults-to-python",
        ),
    ],
)
def test_select_runtime(
    configured: SessionRuntimeKind, hint: Optional[str], expected: SessionRuntimeKind
) -> None:
    assert select_runtime(configured, runtime_hint=hint) is expected


@pytest.mark.parametrize(
    "bad_hint",
    [
        pytest.param("python", id="config-string-not-a-wire-value"),
        pytest.param("service-selected", id="service-selected-is-not-a-valid-hint"),
        pytest.param("RUST", id="wrong-case"),
        pytest.param("", id="empty-string"),
        pytest.param("cobol", id="unknown-runtime"),
    ],
)
def test_select_runtime_invalid_hint_errors(bad_hint: str) -> None:
    with pytest.raises(ValueError, match="Invalid runtimeHint"):
        select_runtime(SessionRuntimeKind.SERVICE_SELECTED, runtime_hint=bad_hint)


def test_pinned_mode_does_not_validate_hint() -> None:
    # In python/rust mode the hint is ignored entirely — even a garbage value
    # must not raise, because pinned fleets shouldn't break on bad service data.
    assert (
        select_runtime(SessionRuntimeKind.PYTHON, runtime_hint="garbage")
        is SessionRuntimeKind.PYTHON
    )


def test_hint_whitespace_is_not_stripped() -> None:
    # The wire value must match exactly; " rust " is treated as invalid, not trimmed.
    with pytest.raises(ValueError, match="Invalid runtimeHint"):
        select_runtime(SessionRuntimeKind.SERVICE_SELECTED, runtime_hint=" rust ")
