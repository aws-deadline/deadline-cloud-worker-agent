# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from openjd.sessions import ActionStatus

if TYPE_CHECKING:
    from ..api_models import CompletedActionStatus


@dataclass(frozen=True)
class SessionActionStatus:
    id: str
    update_time: datetime | None = None
    status: ActionStatus | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    completed_status: CompletedActionStatus | None = None
