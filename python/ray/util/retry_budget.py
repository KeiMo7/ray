"""Restart and task-retry budgets supervised by a ``BudgetedActor``.

This module is intentionally importable without calling :func:`ray.init`.
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import enum
import hashlib
import json
import logging
import math
import os
import threading
import time
import uuid
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Callable

import ray
from ray.actor import ActorHandle
from ray.exceptions import (
    ActorDiedError,
    ActorUnschedulableError,
    ActorUnavailableError,
    RayError,
    RayTaskError,
    TaskCancelledError,
)
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
_EVENT_FIELDS = (
    "seq",
    "ts",
    "kind",
    "phase",
    "request_id",
    "token",
    "task_key",
    "attempt",
    "delay_s",
    "error_type",
    "detail",
)
_RECORD_FIELDS = ("schema_version", "actor_key") + _EVENT_FIELDS

_OUTCOMES = frozenset({"succeeded", "failed", "cancelled", "exhausted"})
_TASK_PHASES = {
    "succeeded": "task_succeeded",
    "failed": "task_failed",
    "cancelled": "task_cancelled",
    "exhausted": "task_exhausted",
}
_PHASE_OUTCOMES = {phase: outcome for outcome, phase in _TASK_PHASES.items()}
_TERMINAL_PHASES = frozenset({"committed", "released"} | set(_TASK_PHASES.values()))


def _is_real_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _error_type_name(error: Any) -> str | None:
    if isinstance(error, type) and issubclass(error, BaseException):
        return error.__name__
    if isinstance(error, BaseException):
        return type(error).__name__
    return None


def _error_summary(error: Any, limit: int = 120) -> str:
    if error is None:
        return ""
    text = str(error).replace("\n", " ").replace("\r", " ")
    if len(text) > limit:
        digest = hashlib.sha1(str(error).encode("utf-8", "replace")).hexdigest()[:10]
        return f"{text[:limit]}...{digest}"
    return text


@DeveloperAPI
class BudgetKind(str, enum.Enum):
    """The kind of budget reservation."""

    RESTART = "restart"
    RETRY = "retry"


@DeveloperAPI
@dataclasses.dataclass(frozen=True)
class BudgetPolicy:
    """Limits and backoff configuration for :class:`RestartRetryBudget`."""

    max_restarts: int = 0
    max_task_retries: int = 0
    max_total_spend: int = -1
    window_seconds: float | None = None
    restart_backoff: tuple[float, ...] = (0.0,)
    retry_backoff: tuple[float, ...] = (0.0,)
    max_backoff_seconds: float = 30.0
    count_preemptions: bool = False
    max_preemption_restarts: int = 100
    retry_exception_types: tuple[type[BaseException], ...] = ()
    replay_safe_methods: frozenset[str] = frozenset()
    orphan_policy: str = "spend"

    def __post_init__(self) -> None:
        for field_name in (
            "max_restarts",
            "max_task_retries",
            "max_total_spend",
            "max_preemption_restarts",
        ):
            value = getattr(self, field_name)
            if not _is_real_int(value) or (value != -1 and value < 0):
                raise ValueError(f"{field_name} must be -1 or a non-negative integer")

        if self.window_seconds is not None:
            if (
                isinstance(self.window_seconds, bool)
                or not isinstance(self.window_seconds, (int, float))
                or not math.isfinite(float(self.window_seconds))
                or self.window_seconds <= 0
            ):
                raise ValueError("window_seconds must be None or a positive number")
            object.__setattr__(self, "window_seconds", float(self.window_seconds))

        for field_name in ("restart_backoff", "retry_backoff"):
            values = getattr(self, field_name)
            if not isinstance(values, (tuple, list)) or len(values) == 0:
                raise ValueError(f"{field_name} must be a non-empty sequence")
            normalized: list[float] = []
            for value in values:
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(float(value))
                    or value < 0
                ):
                    raise ValueError(
                        f"{field_name} must contain finite non-negative numbers"
                    )
                normalized.append(float(value))
            object.__setattr__(self, field_name, tuple(normalized))

        if (
            isinstance(self.max_backoff_seconds, bool)
            or not isinstance(self.max_backoff_seconds, (int, float))
            or not math.isfinite(float(self.max_backoff_seconds))
            or self.max_backoff_seconds < 0
        ):
            raise ValueError("max_backoff_seconds must be a non-negative number")
        object.__setattr__(self, "max_backoff_seconds", float(self.max_backoff_seconds))

        if not isinstance(self.count_preemptions, bool):
            raise ValueError("count_preemptions must be a bool")

        if self.orphan_policy not in ("spend", "release"):
            raise ValueError("orphan_policy must be 'spend' or 'release'")

        exception_types = self.retry_exception_types
        if not isinstance(exception_types, (tuple, list)):
            raise ValueError("retry_exception_types must be a tuple of exception types")
        normalized_types: list[type[BaseException]] = []
        for exc_type in exception_types:
            if not isinstance(exc_type, type) or not issubclass(
                exc_type, BaseException
            ):
                raise ValueError(
                    "retry_exception_types must contain BaseException subclasses"
                )
            normalized_types.append(exc_type)
        object.__setattr__(self, "retry_exception_types", tuple(normalized_types))

        methods = self.replay_safe_methods
        if not isinstance(methods, (frozenset, set)):
            raise ValueError("replay_safe_methods must be a frozenset of strings")
        if not all(isinstance(method, str) for method in methods):
            raise ValueError("replay_safe_methods must contain only strings")
        object.__setattr__(self, "replay_safe_methods", frozenset(methods))


@DeveloperAPI
@dataclasses.dataclass(frozen=True)
class BudgetGrant:
    """A successful reservation."""

    token: str
    kind: BudgetKind
    request_id: str
    task_key: str | None
    incarnation: int
    delay_s: float
    spent_before: int
    remaining: int | None
    state: str
    replayed: bool


@DeveloperAPI
@dataclasses.dataclass(frozen=True)
class BudgetEvent:
    """An immutable event appended to a budget store."""

    seq: int
    ts: float
    kind: str
    phase: str
    request_id: str
    token: str | None
    task_key: str | None
    attempt: int
    delay_s: float
    error_type: str | None
    detail: Mapping[str, Any] | None = None


@DeveloperAPI
@dataclasses.dataclass(frozen=True)
class TaskBudgetState:
    """Budget state for one task identity."""

    task_key: str
    attempts: int
    retries_spent: int
    retries_remaining: int | None
    outcome: str | None
    last_error_type: str | None


class BudgetTaskSequence(tuple, Mapping):
    """Tuple of task states that also supports lookup by ``task_key``."""

    _by_key: Mapping[str, TaskBudgetState]

    def __new__(cls, tasks: Iterable[TaskBudgetState] = ()) -> "BudgetTaskSequence":
        task_tuple = tuple(tasks)
        obj = super().__new__(cls, task_tuple)
        by_key: dict[str, TaskBudgetState] = {}
        for task in task_tuple:
            by_key[task.task_key] = task
        obj._by_key = by_key
        return obj

    def __getitem__(self, key: Any):
        if isinstance(key, str):
            return self._by_key[key]
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> TaskBudgetState | Any:
        return self._by_key.get(key, default)

    def keys(self):
        return self._by_key.keys()

    def values(self):
        return self._by_key.values()

    def items(self):
        return self._by_key.items()

    def __iter__(self):
        # Preserve tuple iteration over task states; Mapping consumers use keys().
        return tuple.__iter__(self)

    def by_key(self) -> Mapping[str, TaskBudgetState]:
        return self._by_key

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Mapping):
            return self._by_key == dict(other)
        if isinstance(other, (list, tuple)):
            return tuple.__eq__(self, tuple(other))
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        result = self.__eq__(other)
        return result if result is NotImplemented else not result

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __reduce__(self):
        return (BudgetTaskSequence, (tuple(self),))


Mapping.register(BudgetTaskSequence)


@DeveloperAPI
@dataclasses.dataclass(frozen=True)
class BudgetSnapshot:
    """Point-in-time budget state."""

    actor_key: str
    incarnation: int
    restart_spent: int
    retry_spent: int
    total_spent: int
    pending: int
    preemption_restarts: int
    recovered_orphans: int
    restarts_remaining: int | None
    total_remaining: int | None
    exhausted: str | None
    last_event_seq: int
    tasks: BudgetTaskSequence


@DeveloperAPI
class BudgetStoreError(Exception):
    """Raised when a budget store cannot be read or written."""

    def __init__(
        self,
        message: str,
        *,
        path: str | os.PathLike[str] | None = None,
        operation: str | None = None,
    ) -> None:
        self.path = None if path is None else os.fspath(path)
        self.operation = operation
        prefix = ""
        if self.path is not None:
            prefix += f"{self.path}: "
        if operation:
            prefix += f"store {operation} failed: "
        super().__init__(prefix + message)


@DeveloperAPI
class BudgetStoreCorruption(BudgetStoreError):
    """Raised when persisted budget data is inconsistent or corrupt."""

    def __init__(
        self,
        message: str,
        *,
        path: str | os.PathLike[str] | None = None,
        line: int | None = None,
        operation: str = "load",
    ) -> None:
        self.line = line
        location = ""
        if line is not None:
            location = f"line {line}: "
        super().__init__(location + message, path=path, operation=operation)


@DeveloperAPI
class BudgetStore:
    """Persistence interface for budget event records."""

    def append(self, record: Mapping[str, Any]) -> None:
        """Append one complete record and durably persist it."""
        raise NotImplementedError

    def load(self) -> Iterable[Mapping[str, Any]]:
        """Load all complete records."""
        raise NotImplementedError


@DeveloperAPI
class InMemoryBudgetStore(BudgetStore):
    """In-memory :class:`BudgetStore` with append/load semantics."""

    def __init__(self, records: Iterable[Mapping[str, Any]] | None = None) -> None:
        self._lock = threading.RLock()
        self.records: list[dict[str, Any]] = []
        if records is not None:
            for record in records:
                self.records.append(dict(record))

    def append(self, record: Mapping[str, Any]) -> None:
        with self._lock:
            self.records.append(dict(record))

    def load(self) -> tuple[Mapping[str, Any], ...]:
        with self._lock:
            return tuple(dict(record) for record in self.records)


@DeveloperAPI
class JsonlBudgetStore(BudgetStore):
    """One-JSON-object-per-line durable budget store."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()

    def append(self, record: Mapping[str, Any]) -> None:
        line = json.dumps(
            dict(record),
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._lock:
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with self.path.open("a", encoding="utf-8") as handle:
                    handle.write(line + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            except (OSError, TypeError, ValueError) as exc:
                raise BudgetStoreError(
                    str(exc), path=self.path, operation="append"
                ) from exc

    def load(self) -> tuple[Mapping[str, Any], ...]:
        with self._lock:
            if not self.path.exists():
                return ()
            try:
                raw = self.path.read_bytes()
            except OSError as exc:
                raise BudgetStoreError(
                    str(exc), path=self.path, operation="load"
                ) from exc

        if not raw:
            return ()

        # A final line without a newline may be a partially written record.
        if not raw.endswith(b"\n"):
            last_newline = raw.rfind(b"\n")
            raw = raw[: last_newline + 1] if last_newline >= 0 else b""

        records: list[Mapping[str, Any]] = []
        line_number = 0
        for raw_line in raw.split(b"\n"):
            line_number += 1
            if raw_line == b"":
                # split() produces one final empty element after the last newline.
                if line_number == len(raw.split(b"\n")):
                    continue
                raise BudgetStoreCorruption(
                    "empty record line", path=self.path, line=line_number
                )
            try:
                text = raw_line.decode("utf-8")
                record = json.loads(text)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BudgetStoreCorruption(
                    f"invalid JSON: {exc}", path=self.path, line=line_number
                ) from exc
            if not isinstance(record, dict):
                raise BudgetStoreCorruption(
                    "record is not a JSON object",
                    path=self.path,
                    line=line_number,
                )
            if record.get("schema_version") != _SCHEMA_VERSION:
                raise BudgetStoreCorruption(
                    f"unknown schema_version: {record.get('schema_version')!r}",
                    path=self.path,
                    line=line_number,
                )
            missing = [key for key in _RECORD_FIELDS if key not in record]
            if missing:
                raise BudgetStoreCorruption(
                    f"record missing fields: {missing}",
                    path=self.path,
                    line=line_number,
                )
            extra = [key for key in record if key not in _RECORD_FIELDS]
            if extra:
                raise BudgetStoreCorruption(
                    f"record has unexpected fields: {extra}",
                    path=self.path,
                    line=line_number,
                )
            records.append(record)
        return tuple(records)


@DeveloperAPI
class RetryBudgetExhausted(RayError):
    """Raised when a restart or retry would exceed a configured budget."""

    def __init__(
        self,
        message: str | None = None,
        *,
        actor_key: str | None = None,
        kind: BudgetKind | str | None = None,
        limit_kind: str | None = None,
        limit: int | None = None,
        spent: int | None = None,
        task_key: str | None = None,
        incarnation: int = 0,
        retry_after_s: float | None = None,
        last_error: BaseException | str | None = None,
    ) -> None:
        self.actor_key = actor_key
        self.kind = kind
        self.limit_kind = limit_kind
        self.limit = limit
        self.spent = spent
        self.task_key = task_key
        self.incarnation = incarnation
        self.retry_after_s = retry_after_s
        self.last_error = last_error
        if message is None:
            message = (
                f"retry budget exhausted for {actor_key}: "
                f"{limit_kind} limit {limit} reached with effective spend {spent}"
            )
        super().__init__(message)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, RetryBudgetExhausted):
            return NotImplemented
        return self.__dict__ == other.__dict__ and self.args == other.args

    def __hash__(self) -> int:
        return hash((self.limit_kind, self.actor_key, self.task_key, self.spent))


@dataclasses.dataclass
class _Reservation:
    token: str
    kind: BudgetKind
    request_id: str
    task_key: str | None
    attempt: int
    incarnation: int
    delay_s: float
    spent_before: int
    remaining: int | None
    preempted: bool
    counts_restart_limit: bool
    state: str = "pending"
    committed_ts: float | None = None
    error_type: str | None = None
    detail: dict[str, Any] = dataclasses.field(default_factory=dict)


def _backoff_delay(table: tuple[float, ...], ordinal: int, cap: float) -> float:
    index = min(max(ordinal - 1, 0), len(table) - 1)
    return min(table[index], cap)


@DeveloperAPI
class RestartRetryBudget:
    """Thread-safe restart/retry accounting backed by an optional event store."""

    def __init__(
        self,
        actor_key: str,
        policy: BudgetPolicy,
        store: BudgetStore | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not isinstance(policy, BudgetPolicy):
            raise TypeError("policy must be a BudgetPolicy")
        self.actor_key = actor_key
        self.policy = policy
        self._store = store if store is not None else InMemoryBudgetStore()
        self._clock = clock
        self._lock = threading.RLock()

        self._events: list[BudgetEvent] = []
        self._reservations: dict[str, _Reservation] = {}
        self._request_tokens: dict[str, str] = {}
        self._tasks: dict[str, dict[str, Any]] = {}
        self._last_event_seq = 0
        self._incarnation = 0
        self._recovered_orphans = 0
        self._exhausted: str | None = None

        self._recover()

    # ------------------------------------------------------------------
    # Event storage and recovery
    # ------------------------------------------------------------------

    def _store_path(self) -> str | None:
        path = getattr(self._store, "path", None)
        return os.fspath(path) if path is not None else None

    def _corruption(
        self,
        message: str,
        *,
        line: int | None = None,
        record: Mapping[str, Any] | None = None,
    ) -> BudgetStoreCorruption:
        if line is None and record is not None:
            seq = record.get("seq")
            if _is_real_int(seq):
                line = seq
        return BudgetStoreCorruption(message, path=self._store_path(), line=line)

    def _recover(self) -> None:
        try:
            records = list(self._store.load())
        except BudgetStoreCorruption:
            raise
        except Exception as exc:
            raise BudgetStoreError(
                str(exc), path=self._store_path(), operation="load"
            ) from exc

        for line_number, record in enumerate(records, start=1):
            if not isinstance(record, Mapping):
                raise self._corruption("record is not an object", line=line_number)
            if record.get("actor_key") != self.actor_key:
                raise self._corruption(
                    "actor_key does not match budget ledger",
                    line=line_number,
                    record=record,
                )
            seq = record.get("seq")
            if not _is_real_int(seq) or seq <= 0:
                raise self._corruption(
                    "seq must be a positive integer", line=line_number, record=record
                )
            if self._last_event_seq == 0 and seq != 1:
                raise self._corruption(
                    "first event seq must be 1", line=line_number, record=record
                )
            if seq <= self._last_event_seq:
                raise self._corruption(
                    f"seq {seq} regressed or duplicated after "
                    f"{self._last_event_seq}",
                    line=line_number,
                    record=record,
                )
            try:
                event = BudgetEvent(
                    seq=seq,
                    ts=float(record["ts"]),
                    kind=str(record["kind"]),
                    phase=str(record["phase"]),
                    request_id=str(record["request_id"]),
                    token=record.get("token"),
                    task_key=record.get("task_key"),
                    attempt=int(record["attempt"] or 0),
                    delay_s=float(record["delay_s"] or 0.0),
                    error_type=record.get("error_type"),
                    detail=record.get("detail"),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise self._corruption(
                    f"invalid event record: {exc}",
                    line=line_number,
                    record=record,
                ) from exc
            if event.kind not in ("restart", "retry", "task"):
                raise self._corruption(
                    f"unknown event kind: {event.kind}",
                    line=line_number,
                    record=record,
                )
            if event.phase not in (
                "granted",
                "committed",
                "released",
                "exhausted",
                *_TASK_PHASES.values(),
            ):
                raise self._corruption(
                    f"unknown event phase: {event.phase}",
                    line=line_number,
                    record=record,
                )
            self._apply_replayed_event(event, line_number)

        pending = [
            reservation
            for reservation in self._reservations.values()
            if reservation.state == "pending"
        ]
        for reservation in pending:
            phase = "committed" if self.policy.orphan_policy == "spend" else "released"
            detail = dict(reservation.detail)
            detail.update(
                {
                    "orphan_recovery": True,
                    "orphan_policy": self.policy.orphan_policy,
                }
            )
            # _resolve_pending() accounts for recovered_orphans from this detail.
            self._resolve_pending(
                reservation.token,
                phase,
                detail=detail,
                incarnation_delta=0,
            )

    def _apply_replayed_event(self, event: BudgetEvent, line_number: int) -> None:
        if event.phase == "granted":
            if not event.token or event.token in self._reservations:
                raise self._corruption(
                    "granted event has a missing or duplicate token",
                    line=line_number,
                )
            detail = dict(event.detail or {})
            preempted = bool(detail.get("preempted", False))
            counts_restart_limit = bool(
                detail.get(
                    "counts_restart_limit",
                    not (preempted and not self.policy.count_preemptions),
                )
            )
            reservation = _Reservation(
                token=event.token,
                kind=BudgetKind(event.kind),
                request_id=event.request_id,
                task_key=event.task_key,
                attempt=event.attempt,
                incarnation=int(detail.get("incarnation", self._incarnation)),
                delay_s=event.delay_s,
                spent_before=int(detail.get("spent_before", 0)),
                remaining=detail.get("remaining"),
                preempted=preempted,
                counts_restart_limit=counts_restart_limit,
                error_type=event.error_type,
                detail=detail,
            )
            if reservation.remaining is not None:
                reservation.remaining = int(reservation.remaining)
            self._reservations[event.token] = reservation
            self._request_tokens[event.request_id] = event.token
            if event.task_key is not None:
                self._ensure_task(event.task_key)
                task_state = self._tasks[event.task_key]
                task_state["attempts"] = max(task_state["attempts"], event.attempt)
                if event.error_type is not None:
                    task_state["last_error_type"] = event.error_type
            self._events.append(event)
            self._last_event_seq = event.seq
            return

        if event.phase in ("committed", "released"):
            if not event.token or event.token not in self._reservations:
                raise self._corruption(
                    f"{event.phase} event lacks a matching granted event",
                    line=line_number,
                )
            reservation = self._reservations[event.token]
            if reservation.state != "pending":
                raise self._corruption(
                    f"token {event.token} is already terminal",
                    line=line_number,
                )
            reservation.state = event.phase
            detail = dict(event.detail or {})
            if event.phase == "committed":
                reservation.committed_ts = event.ts
                if "new_incarnation" in detail:
                    self._incarnation = max(
                        self._incarnation, int(detail["new_incarnation"])
                    )
                if "incarnation_delta" in detail:
                    self._incarnation += int(detail["incarnation_delta"])
                elif event.kind == BudgetKind.RESTART.value:
                    # Records produced without private detail still treat a
                    # committed restart as one actor incarnation.
                    self._incarnation += 1
            self._events.append(event)
            self._last_event_seq = event.seq
            return

        if event.phase == "exhausted":
            detail = event.detail or {}
            self._exhausted = detail.get("limit_kind")
            self._events.append(event)
            self._last_event_seq = event.seq
            return

        outcome = _PHASE_OUTCOMES.get(event.phase)
        if outcome is not None:
            if event.task_key is None:
                raise self._corruption(
                    "task terminal event is missing task_key", line=line_number
                )
            task = self._ensure_task(event.task_key)
            if task["outcome"] is not None and task["outcome"] != outcome:
                raise self._corruption(
                    f"task {event.task_key} has conflicting terminal states",
                    line=line_number,
                )
            task["outcome"] = outcome
            task["attempts"] = max(task["attempts"], event.attempt)
            if event.error_type is not None:
                task["last_error_type"] = event.error_type
            self._events.append(event)
            self._last_event_seq = event.seq

    def _ensure_task(self, task_key: str) -> dict[str, Any]:
        if task_key not in self._tasks:
            self._tasks[task_key] = {
                "task_key": task_key,
                "attempts": 0,
                "outcome": None,
                "last_error_type": None,
            }
        return self._tasks[task_key]

    def _record_from_event(self, event: BudgetEvent) -> dict[str, Any]:
        record: dict[str, Any] = {
            "schema_version": _SCHEMA_VERSION,
            "actor_key": self.actor_key,
        }
        for field_name in _EVENT_FIELDS:
            value = getattr(event, field_name)
            if isinstance(value, enum.Enum):
                value = value.value
            record[field_name] = value
        return record

    def _persist_event(self, event: BudgetEvent) -> None:
        record = self._record_from_event(event)
        try:
            self._store.append(record)
        except BudgetStoreError:
            raise
        except Exception as exc:
            raise BudgetStoreError(
                str(exc), path=self._store_path(), operation="append"
            ) from exc
        self._events.append(event)
        self._last_event_seq = event.seq

    def _resolve_pending(
        self,
        token: str,
        phase: str,
        *,
        detail: Mapping[str, Any] | None = None,
        incarnation_delta: int | None = None,
    ) -> bool:
        reservation = self._reservations.get(token)
        if reservation is None or reservation.state != "pending":
            return False
        now = self._clock()
        event_detail = dict(detail or {})
        if incarnation_delta is not None:
            event_detail["incarnation_delta"] = incarnation_delta
            event_detail["new_incarnation"] = self._incarnation + incarnation_delta
        event = BudgetEvent(
            seq=self._last_event_seq + 1,
            ts=now,
            kind=reservation.kind.value,
            phase=phase,
            request_id=reservation.request_id,
            token=token,
            task_key=reservation.task_key,
            attempt=reservation.attempt,
            delay_s=reservation.delay_s,
            error_type=reservation.error_type,
            detail=event_detail or None,
        )
        self._persist_event(event)
        reservation.state = phase
        if event_detail.get("orphan_recovery"):
            self._recovered_orphans += 1
        if phase == "committed":
            reservation.committed_ts = now
            if incarnation_delta is not None:
                self._incarnation += incarnation_delta
        return True

    # ------------------------------------------------------------------
    # Counting
    # ------------------------------------------------------------------

    def _in_window(self, ts: float | None, now: float) -> bool:
        if ts is None:
            return False
        window = self.policy.window_seconds
        return window is None or ts > now - window

    def _reservations_of_kind(self, kind: BudgetKind) -> list[_Reservation]:
        return [r for r in self._reservations.values() if r.kind == kind]

    def _restart_used(
        self, now: float, *, pending: bool, only_limited: bool = True
    ) -> int:
        total = 0
        for reservation in self._reservations_of_kind(BudgetKind.RESTART):
            if only_limited and not reservation.counts_restart_limit:
                continue
            if reservation.state == "pending" and pending:
                total += 1
            elif (
                not pending
                and reservation.state == "committed"
                and self._in_window(reservation.committed_ts, now)
            ):
                total += 1
        return total

    def _retry_used(self, now: float, task_key: str, *, pending: bool) -> int:
        total = 0
        for reservation in self._reservations_of_kind(BudgetKind.RETRY):
            if reservation.task_key != task_key:
                continue
            if reservation.state == "pending" and pending:
                total += 1
            elif (
                not pending
                and reservation.state == "committed"
                and self._in_window(reservation.committed_ts, now)
            ):
                total += 1
        return total

    def _total_used(self, now: float, *, pending: bool) -> int:
        total = 0
        for reservation in self._reservations.values():
            if reservation.kind not in (BudgetKind.RESTART, BudgetKind.RETRY):
                continue
            if reservation.state == "pending" and pending:
                total += 1
            elif (
                not pending
                and reservation.state == "committed"
                and self._in_window(reservation.committed_ts, now)
            ):
                total += 1
        return total

    def _preemption_used(self, now: float, *, pending: bool) -> int:
        total = 0
        for reservation in self._reservations_of_kind(BudgetKind.RESTART):
            if not reservation.preempted:
                continue
            if reservation.state == "pending" and pending:
                total += 1
            elif (
                not pending
                and reservation.state == "committed"
                and self._in_window(reservation.committed_ts, now)
            ):
                total += 1
        return total

    def _effective_restarts(self, now: float) -> int:
        return self._restart_used(now, pending=False) + self._restart_used(
            now, pending=True
        )

    def _effective_retries(self, now: float, task_key: str) -> int:
        return self._retry_used(now, task_key, pending=False) + self._retry_used(
            now, task_key, pending=True
        )

    def _effective_total(self, now: float) -> int:
        return self._total_used(now, pending=False) + self._total_used(
            now, pending=True
        )

    def _effective_preemptions(self, now: float) -> int:
        return self._preemption_used(now, pending=False) + self._preemption_used(
            now, pending=True
        )

    def _retry_after(
        self, now: float, limit_kind: str, task_key: str | None = None
    ) -> float | None:
        window = self.policy.window_seconds
        if window is None:
            return None
        timestamps: list[float] = []
        for reservation in self._reservations.values():
            if reservation.state != "committed" or reservation.committed_ts is None:
                continue
            if limit_kind == "restarts":
                if (
                    reservation.kind == BudgetKind.RESTART
                    and reservation.counts_restart_limit
                ):
                    timestamps.append(reservation.committed_ts)
            elif limit_kind == "task_retries":
                if (
                    reservation.kind == BudgetKind.RETRY
                    and reservation.task_key == task_key
                ):
                    timestamps.append(reservation.committed_ts)
            elif limit_kind == "preemptions":
                if reservation.kind == BudgetKind.RESTART and reservation.preempted:
                    timestamps.append(reservation.committed_ts)
            elif limit_kind == "total" and reservation.kind in (
                BudgetKind.RESTART,
                BudgetKind.RETRY,
            ):
                timestamps.append(reservation.committed_ts)
        if not timestamps:
            return 0.0
        wait_s = min(timestamps) + window - now
        if wait_s <= 0:
            return 0.0
        return math.ceil(wait_s * 1000.0) / 1000.0

    # ------------------------------------------------------------------
    # Reservations
    # ------------------------------------------------------------------

    def _param_fingerprint(self, value: Any) -> Any:
        if isinstance(value, BaseException):
            return f"{_error_type_name(value)}:{_error_summary(value)}"
        if isinstance(value, type) and issubclass(value, BaseException):
            return _error_type_name(value)
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        try:
            return json.dumps(value, sort_keys=True, default=repr)
        except (TypeError, ValueError):
            return repr(value)

    def _idempotent_grant(
        self, request_id: str, params: Mapping[str, Any]
    ) -> BudgetGrant | None:
        token = self._request_tokens.get(request_id)
        if token is None:
            return None
        reservation = self._reservations[token]
        expected = {
            "kind": reservation.kind.value,
            "task_key": reservation.task_key,
            "attempt": reservation.attempt,
            "preempted": reservation.preempted,
            "cause": reservation.detail.get("cause_param"),
            "error_type": reservation.error_type,
        }
        actual = {
            "kind": (
                params["kind"].value
                if isinstance(params["kind"], BudgetKind)
                else params["kind"]
            ),
            "task_key": params.get("task_key"),
            "attempt": params.get("attempt", 0),
            "preempted": params.get("preempted", False),
            "cause": self._param_fingerprint(params.get("cause")),
            "error_type": params.get("error_type"),
        }
        if expected != actual:
            raise ValueError(
                f"request_id {request_id!r} was already used with different arguments"
            )
        return self._grant(reservation, replayed=True)

    def _grant(self, reservation: _Reservation, *, replayed: bool) -> BudgetGrant:
        return BudgetGrant(
            token=reservation.token,
            kind=reservation.kind,
            request_id=reservation.request_id,
            task_key=reservation.task_key,
            incarnation=reservation.incarnation,
            delay_s=reservation.delay_s,
            spent_before=reservation.spent_before,
            remaining=reservation.remaining,
            state=reservation.state,
            replayed=replayed,
        )

    def _reject(
        self,
        *,
        now: float,
        kind: BudgetKind,
        limit_kind: str,
        limit: int,
        spent: int,
        request_id: str,
        task_key: str | None,
        attempt: int,
        error_type: str | None,
        last_error: Any,
    ) -> RetryBudgetExhausted:
        retry_after_s = self._retry_after(now, limit_kind, task_key)
        last_error_text: str | None
        if isinstance(last_error, BaseException):
            last_error_text = (
                f"{_error_type_name(last_error)}: {_error_summary(last_error)}"
            )
        elif last_error is not None:
            last_error_text = str(last_error)
        else:
            last_error_text = error_type

        detail = {
            "limit_kind": limit_kind,
            "limit": limit,
            "spent": spent,
            "retry_after_s": retry_after_s,
            "last_error": last_error_text,
        }
        event = BudgetEvent(
            seq=self._last_event_seq + 1,
            ts=now,
            kind=kind.value,
            phase="exhausted",
            request_id=request_id,
            token=None,
            task_key=task_key,
            attempt=attempt,
            delay_s=0.0,
            error_type=error_type,
            detail=detail,
        )
        self._persist_event(event)
        self._exhausted = limit_kind
        return RetryBudgetExhausted(
            actor_key=self.actor_key,
            kind=kind,
            limit_kind=limit_kind,
            limit=limit,
            spent=spent,
            task_key=task_key,
            incarnation=self._incarnation,
            retry_after_s=retry_after_s,
            last_error=last_error_text,
        )

    def reserve_restart(
        self,
        request_id: str,
        *,
        cause: Any,
        preempted: bool = False,
    ) -> BudgetGrant:
        params = {
            "kind": BudgetKind.RESTART,
            "preempted": preempted,
            "cause": cause,
            "attempt": 0,
            "error_type": _error_type_name(cause),
        }
        with self._lock:
            replay = self._idempotent_grant(request_id, params)
            if replay is not None:
                return replay

            now = self._clock()
            if preempted:
                preemptions = self._effective_preemptions(now)
                limit = self.policy.max_preemption_restarts
                if limit >= 0 and preemptions >= limit:
                    raise self._reject(
                        now=now,
                        kind=BudgetKind.RESTART,
                        limit_kind="preemptions",
                        limit=limit,
                        spent=preemptions,
                        request_id=request_id,
                        task_key=None,
                        attempt=0,
                        error_type=_error_type_name(cause),
                        last_error=cause,
                    )

            counts_restart_limit = not (preempted and not self.policy.count_preemptions)
            restarts = self._effective_restarts(now)
            if (
                counts_restart_limit
                and self.policy.max_restarts >= 0
                and restarts >= self.policy.max_restarts
            ):
                raise self._reject(
                    now=now,
                    kind=BudgetKind.RESTART,
                    limit_kind="restarts",
                    limit=self.policy.max_restarts,
                    spent=restarts,
                    request_id=request_id,
                    task_key=None,
                    attempt=0,
                    error_type=_error_type_name(cause),
                    last_error=cause,
                )

            total = self._effective_total(now)
            if (
                self.policy.max_total_spend >= 0
                and total >= self.policy.max_total_spend
            ):
                raise self._reject(
                    now=now,
                    kind=BudgetKind.RESTART,
                    limit_kind="total",
                    limit=self.policy.max_total_spend,
                    spent=total,
                    request_id=request_id,
                    task_key=None,
                    attempt=0,
                    error_type=_error_type_name(cause),
                    last_error=cause,
                )

            restart_ordinal = (
                self._restart_used(now, pending=False, only_limited=False)
                + self._restart_used(now, pending=True, only_limited=False)
                + 1
            )
            delay_s = _backoff_delay(
                self.policy.restart_backoff,
                restart_ordinal,
                self.policy.max_backoff_seconds,
            )
            spent_before = restarts if counts_restart_limit else restarts
            if self.policy.max_restarts < 0:
                remaining: int | None = None
            else:
                remaining = max(
                    0,
                    self.policy.max_restarts
                    - restarts
                    - (1 if counts_restart_limit else 0),
                )

            token = f"grant-{uuid.uuid4().hex}"
            cause_param = self._param_fingerprint(cause)
            detail = {
                "preempted": preempted,
                "counts_restart_limit": counts_restart_limit,
                "cause": _error_summary(cause) if cause is not None else None,
                "cause_param": cause_param,
                "spent_before": spent_before,
                "remaining": remaining,
                "incarnation": self._incarnation,
                "ordinal": restart_ordinal,
            }
            reservation = _Reservation(
                token=token,
                kind=BudgetKind.RESTART,
                request_id=request_id,
                task_key=None,
                attempt=0,
                incarnation=self._incarnation,
                delay_s=delay_s,
                spent_before=spent_before,
                remaining=remaining,
                preempted=preempted,
                counts_restart_limit=counts_restart_limit,
                error_type=_error_type_name(cause),
                detail=detail,
            )
            event = BudgetEvent(
                seq=self._last_event_seq + 1,
                ts=now,
                kind=BudgetKind.RESTART.value,
                phase="granted",
                request_id=request_id,
                token=token,
                task_key=None,
                attempt=0,
                delay_s=delay_s,
                error_type=_error_type_name(cause),
                detail=detail,
            )
            self._persist_event(event)
            self._reservations[token] = reservation
            self._request_tokens[request_id] = token
            return self._grant(reservation, replayed=False)

    def reserve_retry(
        self,
        request_id: str,
        *,
        task_key: str,
        attempt: int,
        error_type: str | type[BaseException] | BaseException | None,
        error_message: str | None = None,
    ) -> BudgetGrant:
        if not isinstance(task_key, str) or not task_key:
            raise ValueError("task_key must be a non-empty string")
        if not _is_real_int(attempt) or attempt < 0:
            raise ValueError("attempt must be a non-negative integer")

        normalized_error_type: str | None
        if error_type is None or isinstance(error_type, str):
            normalized_error_type = error_type
        else:
            normalized_error_type = _error_type_name(error_type)

        params = {
            "kind": BudgetKind.RETRY,
            "task_key": task_key,
            "attempt": attempt,
            "error_type": normalized_error_type,
        }
        with self._lock:
            replay = self._idempotent_grant(request_id, params)
            if replay is not None:
                return replay

            now = self._clock()
            self._ensure_task(task_key)
            task_used = self._effective_retries(now, task_key)
            if (
                self.policy.max_task_retries >= 0
                and task_used >= self.policy.max_task_retries
            ):
                last_error = error_message or normalized_error_type
                raise self._reject(
                    now=now,
                    kind=BudgetKind.RETRY,
                    limit_kind="task_retries",
                    limit=self.policy.max_task_retries,
                    spent=task_used,
                    request_id=request_id,
                    task_key=task_key,
                    attempt=attempt,
                    error_type=normalized_error_type,
                    last_error=last_error,
                )

            total = self._effective_total(now)
            if (
                self.policy.max_total_spend >= 0
                and total >= self.policy.max_total_spend
            ):
                last_error = error_message or normalized_error_type
                raise self._reject(
                    now=now,
                    kind=BudgetKind.RETRY,
                    limit_kind="total",
                    limit=self.policy.max_total_spend,
                    spent=total,
                    request_id=request_id,
                    task_key=task_key,
                    attempt=attempt,
                    error_type=normalized_error_type,
                    last_error=last_error,
                )

            ordinal = task_used + 1
            delay_s = _backoff_delay(
                self.policy.retry_backoff,
                ordinal,
                self.policy.max_backoff_seconds,
            )
            remaining = (
                None
                if self.policy.max_task_retries < 0
                else max(0, self.policy.max_task_retries - task_used - 1)
            )
            token = f"grant-{uuid.uuid4().hex}"
            detail = {
                "spent_before": task_used,
                "remaining": remaining,
                "incarnation": self._incarnation,
                "ordinal": ordinal,
                "error_message": error_message,
            }
            reservation = _Reservation(
                token=token,
                kind=BudgetKind.RETRY,
                request_id=request_id,
                task_key=task_key,
                attempt=attempt,
                incarnation=self._incarnation,
                delay_s=delay_s,
                spent_before=task_used,
                remaining=remaining,
                preempted=False,
                counts_restart_limit=False,
                error_type=normalized_error_type,
                detail=detail,
            )
            self._tasks[task_key]["attempts"] = max(
                self._tasks[task_key]["attempts"], attempt
            )
            if normalized_error_type is not None:
                self._tasks[task_key]["last_error_type"] = normalized_error_type
            event = BudgetEvent(
                seq=self._last_event_seq + 1,
                ts=now,
                kind=BudgetKind.RETRY.value,
                phase="granted",
                request_id=request_id,
                token=token,
                task_key=task_key,
                attempt=attempt,
                delay_s=delay_s,
                error_type=normalized_error_type,
                detail=detail,
            )
            self._persist_event(event)
            self._reservations[token] = reservation
            self._request_tokens[request_id] = token
            return self._grant(reservation, replayed=False)

    def commit(self, token: str) -> bool:
        """Commit a pending reservation. Returns False if it is terminal/unknown."""
        with self._lock:
            reservation = self._reservations.get(token)
            incarnation_delta = (
                1
                if reservation is not None and reservation.kind == BudgetKind.RESTART
                else None
            )
            return self._resolve_pending(
                token,
                "committed",
                incarnation_delta=incarnation_delta,
            )

    def _commit_successful_restart(self, token: str) -> bool:
        with self._lock:
            return self._resolve_pending(token, "committed", incarnation_delta=1)

    def _commit_failed_restart(self, token: str) -> bool:
        with self._lock:
            return self._resolve_pending(token, "committed", incarnation_delta=0)

    def release(self, token: str, *, reason: str) -> bool:
        """Release a pending reservation. Returns False if terminal/unknown."""
        with self._lock:
            return self._resolve_pending(token, "released", detail={"reason": reason})

    # ------------------------------------------------------------------
    # Tasks and snapshots
    # ------------------------------------------------------------------

    def finish_task(
        self,
        task_key: str,
        outcome: str,
        attempt: int,
        error_type: str | type[BaseException] | BaseException | None = None,
    ) -> BudgetEvent | None:
        if outcome not in _OUTCOMES:
            raise ValueError(f"outcome must be one of {sorted(_OUTCOMES)}")
        if not _is_real_int(attempt) or attempt < 0:
            raise ValueError("attempt must be a non-negative integer")
        normalized_error_type: str | None
        if error_type is None or isinstance(error_type, str):
            normalized_error_type = error_type
        else:
            normalized_error_type = _error_type_name(error_type)

        with self._lock:
            task = self._ensure_task(task_key)
            if task["outcome"] is not None:
                for event in reversed(self._events):
                    if (
                        event.kind == "task"
                        and event.task_key == task_key
                        and event.phase in _TASK_PHASES.values()
                    ):
                        return event
                return None

            now = self._clock()
            phase = _TASK_PHASES[outcome]
            event = BudgetEvent(
                seq=self._last_event_seq + 1,
                ts=now,
                kind="task",
                phase=phase,
                request_id="",
                token=None,
                task_key=task_key,
                attempt=attempt,
                delay_s=0.0,
                error_type=normalized_error_type,
                detail=None,
            )
            self._persist_event(event)
            task["outcome"] = outcome
            task["attempts"] = max(task["attempts"], attempt)
            if normalized_error_type is not None:
                task["last_error_type"] = normalized_error_type
            return event

    def snapshot(self) -> BudgetSnapshot:
        with self._lock:
            now = self._clock()
            restart_spent = self._restart_used(now, pending=False)
            retry_spent = self._retry_spent_all(now)
            total_spent = self._total_used(now, pending=False)
            pending = sum(
                1 for r in self._reservations.values() if r.state == "pending"
            )
            preemption_restarts = self._preemption_used(now, pending=False)
            effective_restarts = self._effective_restarts(now)
            effective_total = self._effective_total(now)

            if self.policy.max_restarts < 0:
                restarts_remaining: int | None = None
            else:
                restarts_remaining = max(
                    0, self.policy.max_restarts - effective_restarts
                )
            total_remaining = (
                None
                if self.policy.max_total_spend < 0
                else max(0, self.policy.max_total_spend - effective_total)
            )

            task_states: list[TaskBudgetState] = []
            for task_key in sorted(self._tasks):
                task = self._tasks[task_key]
                spent = self._retry_used(now, task_key, pending=False)
                effective = self._effective_retries(now, task_key)
                remaining = (
                    None
                    if self.policy.max_task_retries < 0
                    else max(0, self.policy.max_task_retries - effective)
                )
                task_states.append(
                    TaskBudgetState(
                        task_key=task_key,
                        attempts=int(task["attempts"]),
                        retries_spent=spent,
                        retries_remaining=remaining,
                        outcome=task["outcome"],
                        last_error_type=task["last_error_type"],
                    )
                )

            return BudgetSnapshot(
                actor_key=self.actor_key,
                incarnation=self._incarnation,
                restart_spent=restart_spent,
                retry_spent=retry_spent,
                total_spent=total_spent,
                pending=pending,
                preemption_restarts=preemption_restarts,
                recovered_orphans=self._recovered_orphans,
                restarts_remaining=restarts_remaining,
                total_remaining=total_remaining,
                exhausted=self._exhausted,
                last_event_seq=self._last_event_seq,
                tasks=BudgetTaskSequence(task_states),
            )

    def _retry_spent_all(self, now: float) -> int:
        return sum(
            1
            for reservation in self._reservations_of_kind(BudgetKind.RETRY)
            if reservation.state == "committed"
            and self._in_window(reservation.committed_ts, now)
        )

    def events(self, since_seq: int = 0) -> tuple[BudgetEvent, ...]:
        with self._lock:
            return tuple(event for event in self._events if event.seq > since_seq)


_FORBIDDEN_ACTOR_OPTIONS = frozenset(
    {"max_restarts", "max_task_retries", "retry_exceptions", "name", "lifetime"}
)


@DeveloperAPI
class BudgetedActor:
    """A single logical actor whose restarts and retries use an explicit budget."""

    def __init__(
        self,
        actor_cls: Any,
        args: tuple[Any, ...] = (),
        kwargs: Mapping[str, Any] | None = None,
        *,
        policy: BudgetPolicy,
        actor_key: str | None = None,
        store: BudgetStore | str | os.PathLike[str] | None = None,
        actor_options: Mapping[str, Any] | None = None,
        sleeper: Callable[[float], None] | None = None,
        sleep: Callable[[float], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        options = dict(actor_options or {})
        forbidden = _FORBIDDEN_ACTOR_OPTIONS.intersection(options)
        if forbidden:
            raise ValueError(
                "actor_options contains forbidden keys: " + ", ".join(sorted(forbidden))
            )

        self._actor_cls = actor_cls
        self._init_args = tuple(args)
        self._init_kwargs = dict(kwargs or {})
        self._actor_options = options
        concurrency = options.get("max_concurrency", 1)
        try:
            self._max_workers = max(1, int(concurrency or 1))
        except (TypeError, ValueError):
            self._max_workers = 1

        if isinstance(store, (str, os.PathLike)):
            store = JsonlBudgetStore(store)

        if actor_key is None:
            class_name = getattr(actor_cls, "__name__", "BudgetedActor")
            actor_key = f"{class_name}:{uuid.uuid4().hex}"

        self.budget = RestartRetryBudget(actor_key, policy, store, clock=clock)
        self._policy = policy
        self._sleeper = sleeper or sleep or time.sleep

        self._state_lock = threading.RLock()
        self._task_sequence = 0
        self._failure_seq = self.budget.snapshot().last_event_seq
        self._closed = False
        self._fatal_validation_error: TypeError | None = None
        self._handle: Any = None

        # Create the first actor instance before exposing the supervised actor.
        self._handle = self._create_actor_handle()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._max_workers,
            thread_name_prefix=f"retry-budget-{actor_key}",
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """Submit one call and synchronously wait for its result."""
        return self.submit(method, *args, **kwargs).result()

    def submit(
        self, method: str, *args: Any, **kwargs: Any
    ) -> concurrent.futures.Future:
        """Submit one call to the worker pool and return a Future."""
        if not isinstance(method, str) or not method:
            raise ValueError("method must be a non-empty string")
        with self._state_lock:
            if self._closed:
                raise RuntimeError("BudgetedActor is closed")
            task_sequence = self._task_sequence
            self._task_sequence += 1
            task_key = f"{method}:{task_sequence}"
            return self._executor.submit(self._run_call, task_key, method, args, kwargs)

    def close(self, kill: bool = False) -> None:
        """Close the worker pool and optionally kill the current actor handle."""
        with self._state_lock:
            if self._closed and self._executor is None:
                return
            self._closed = True
            handle = self._handle
            self._handle = None
            executor = self._executor
            self._executor = None

        if kill and handle is not None:
            try:
                ray.kill(handle)
            except Exception as exc:  # shutdown must not mask caller state
                logger.warning("ray.kill failed while closing BudgetedActor: %s", exc)

        if executor is not None:
            executor.shutdown(wait=True)

    def __enter__(self) -> "BudgetedActor":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def snapshot(self) -> BudgetSnapshot:
        """Delegate to the budget ledger, including after close."""
        return self.budget.snapshot()

    def events(self, since_seq: int = 0) -> tuple[BudgetEvent, ...]:
        """Delegate event reads to the budget ledger."""
        return self.budget.events(since_seq)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _create_actor_handle(self) -> Any:
        options = dict(self._actor_options)
        # The budget supervisor owns restart and task-retry semantics.
        options["max_restarts"] = 0
        options["max_task_retries"] = 0
        actor_class = self._actor_cls.options(**options)
        return actor_class.remote(*self._init_args, **self._init_kwargs)

    def _bump_failure_seq(self) -> int:
        with self._state_lock:
            self._failure_seq += 1
            return self._failure_seq

    def _next_request_id(
        self,
        error: BaseException,
        task_key: str | None,
        attempt: int,
    ) -> str:
        failure_seq = self._bump_failure_seq()
        with self._state_lock:
            incarnation = self.budget.snapshot().incarnation
        error_type = _error_type_name(error) or "Error"
        summary = _error_summary(error, limit=80)
        safe_summary = "".join(
            char if char.isalnum() or char in "._-" else "_" for char in summary
        ).strip("_")
        digest = hashlib.sha1(
            f"{error_type}:{str(error)}".encode("utf-8", "replace")
        ).hexdigest()[:10]
        return (
            f"f{failure_seq}:inc{incarnation}:{task_key or 'actor'}:"
            f"a{attempt}:{error_type}:{safe_summary[:64]}:{digest}"
        )

    def _validate_method(self, handle: Any, method: str) -> None:
        with self._state_lock:
            if self._fatal_validation_error is not None:
                raise self._fatal_validation_error

        try:
            ray_method = getattr(handle, method)
        except AttributeError as exc:
            error = TypeError(
                f"Actor {type(self._actor_cls)!r} has no method {method!r}"
            )
            self._fail_invalid_method(error, handle)
            raise error from exc

        is_generator = bool(getattr(ray_method, "_is_generator", False))
        num_returns = getattr(ray_method, "_num_returns", 1)
        if num_returns is None:
            num_returns = 1
        if is_generator or num_returns != 1:
            error = TypeError(
                f"BudgetedActor method {method!r} must be a non-generator method "
                "with num_returns=1"
            )
            self._fail_invalid_method(error, handle)
            raise error

    def _fail_invalid_method(self, error: TypeError, handle: Any) -> None:
        with self._state_lock:
            if self._fatal_validation_error is None:
                self._fatal_validation_error = error
            current = self._handle
            self._handle = None
        victim = current if current is not None else handle
        try:
            ray.kill(victim)
        except Exception as kill_error:
            logger.warning("Failed to kill invalid BudgetedActor: %s", kill_error)

    def _current_handle(self) -> Any:
        with self._state_lock:
            if self._closed:
                raise RuntimeError("BudgetedActor is closed")
            if self._fatal_validation_error is not None:
                raise self._fatal_validation_error
            if self._handle is None:
                raise RuntimeError("BudgetedActor has no active actor handle")
            return self._handle

    def _safe_finish(
        self,
        task_key: str,
        outcome: str,
        attempt: int,
        error: Any = None,
    ) -> None:
        try:
            error_type = error if isinstance(error, str) else _error_type_name(error)
            self.budget.finish_task(task_key, outcome, attempt, error_type)
        except Exception as exc:
            logger.warning("Failed to persist task terminal state: %s", exc)

    def _charge_retry(
        self,
        *,
        error: BaseException,
        task_key: str,
        next_attempt: int,
    ) -> BudgetGrant:
        root: Any = getattr(error, "cause", None)
        if not isinstance(root, BaseException):
            root = error
        request_id = self._next_request_id(error, task_key, next_attempt)
        grant = self.budget.reserve_retry(
            request_id,
            task_key=task_key,
            attempt=next_attempt,
            error_type=root,
            error_message=_error_summary(root, limit=500),
        )
        try:
            self.budget.commit(grant.token)
        except BaseException:
            try:
                self.budget.release(grant.token, reason="commit_failed")
            except Exception:
                logger.warning("Failed to clean up pending retry grant")
            raise
        if grant.delay_s:
            self._sleeper(grant.delay_s)
        return grant

    def _restart_after_died(
        self,
        original_error: ActorDiedError,
        task_key: str,
        attempt: int,
    ) -> Any:
        current_error = original_error
        while True:
            request_id = self._next_request_id(current_error, task_key, attempt)
            try:
                grant = self.budget.reserve_restart(
                    request_id,
                    cause=current_error,
                    preempted=bool(getattr(current_error, "preempted", False)),
                )
            except RetryBudgetExhausted as budget_error:
                self._safe_finish(
                    task_key, "exhausted", attempt, budget_error.limit_kind
                )
                raise budget_error from original_error

            try:
                if grant.delay_s:
                    self._sleeper(grant.delay_s)
                new_handle = self._create_actor_handle()
                # Wait for __init__/scheduling failures so they are classified as
                # restart creation attempts rather than the next user invocation.
                # Keep lightweight test doubles working without __ray_ready__.
                if isinstance(new_handle, ActorHandle):
                    ray.get(new_handle.__ray_ready__.remote())
            except ActorUnschedulableError:
                # The scheduling attempt did not create a replacement.
                self.budget.release(grant.token, reason="actor_unschedulable")
                self._safe_finish(task_key, "failed", attempt, ActorUnschedulableError)
                raise
            except BaseException as create_error:
                # A restart cycle was attempted or interrupted. Conservative
                # spend is retained; BaseException must still propagate.
                self.budget._commit_failed_restart(grant.token)
                if isinstance(create_error, ActorDiedError):
                    current_error = create_error
                    continue
                raise

            committed = self.budget._commit_successful_restart(grant.token)
            if not committed:
                # Recovery or a duplicate transition resolved the token. This
                # should not happen in the normal supervisor path.
                logger.warning("Restart grant %s was already terminal", grant.token)

            with self._state_lock:
                if self._closed:
                    try:
                        ray.kill(new_handle)
                    except Exception as exc:
                        logger.warning(
                            "Failed to kill actor created after close: %s", exc
                        )
                    raise RuntimeError("BudgetedActor was closed during restart")
                old_handle = self._handle
                self._handle = new_handle
            if old_handle is not None and old_handle is not new_handle:
                # The old handle is no longer used by this supervisor. ray.kill
                # is intentionally not called here: ActorDiedError normally means
                # its process is already gone.
                pass
            return new_handle

    def _run_call(
        self,
        task_key: str,
        method: str,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> Any:
        attempt = 1
        call_args = args
        call_kwargs = dict(kwargs)

        while True:
            handle = self._current_handle()
            self._validate_method(handle, method)
            try:
                method_handle = getattr(handle, method)
                object_ref = method_handle.options(
                    max_task_retries=0,
                    retry_exceptions=False,
                ).remote(*call_args, **call_kwargs)
                result = ray.get(object_ref)
            except TaskCancelledError as exc:
                self._bump_failure_seq()
                if not self._closed:
                    self._safe_finish(task_key, "cancelled", attempt, exc)
                raise
            except RetryBudgetExhausted as budget_error:
                self._safe_finish(
                    task_key, "exhausted", attempt, budget_error.limit_kind
                )
                cause = budget_error.__cause__ or budget_error.__context__
                raise budget_error from cause
            except ActorDiedError as exc:
                if self._closed:
                    raise
                self._restart_after_died(exc, task_key, attempt)
                if method in self._policy.replay_safe_methods:
                    next_attempt = attempt + 1
                    try:
                        self._charge_retry(
                            error=exc,
                            task_key=task_key,
                            next_attempt=next_attempt,
                        )
                    except RetryBudgetExhausted as budget_error:
                        self._safe_finish(task_key, "exhausted", attempt)
                        raise budget_error from exc
                    attempt = next_attempt
                    # Re-invoke on the new handle with the same task identity/args.
                    continue

                self._safe_finish(task_key, "failed", attempt, exc)
                raise
            except RayTaskError as exc:
                if self._closed:
                    raise
                root_cause = getattr(exc, "cause", None)
                retry_type_match = bool(self._policy.retry_exception_types) and (
                    isinstance(exc, self._policy.retry_exception_types)
                    or (
                        isinstance(root_cause, BaseException)
                        and isinstance(root_cause, self._policy.retry_exception_types)
                    )
                )
                if not retry_type_match:
                    self._bump_failure_seq()
                    self._safe_finish(task_key, "failed", attempt, exc)
                    raise

                next_attempt = attempt + 1
                try:
                    self._charge_retry(
                        error=exc,
                        task_key=task_key,
                        next_attempt=next_attempt,
                    )
                except RetryBudgetExhausted as budget_error:
                    self._safe_finish(task_key, "exhausted", attempt)
                    cause = root_cause if isinstance(root_cause, BaseException) else exc
                    raise budget_error from cause
                attempt = next_attempt
                continue
            except ActorUnavailableError as exc:
                if self._closed:
                    raise
                if method not in self._policy.replay_safe_methods:
                    self._bump_failure_seq()
                    self._safe_finish(task_key, "failed", attempt, exc)
                    raise

                next_attempt = attempt + 1
                try:
                    self._charge_retry(
                        error=exc,
                        task_key=task_key,
                        next_attempt=next_attempt,
                    )
                except RetryBudgetExhausted as budget_error:
                    self._safe_finish(task_key, "exhausted", attempt)
                    raise budget_error from exc
                attempt = next_attempt
                continue
            except RayError as exc:
                self._bump_failure_seq()
                if not self._closed:
                    self._safe_finish(task_key, "failed", attempt, exc)
                raise
            except Exception as exc:
                self._bump_failure_seq()
                if not self._closed:
                    self._safe_finish(task_key, "failed", attempt, exc)
                raise

            if not self._closed:
                self.budget.finish_task(task_key, "succeeded", attempt)
            return result
