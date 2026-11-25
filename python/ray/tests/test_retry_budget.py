"""Unit and integration tests for explicit retry/restart budgets."""

from __future__ import annotations

import concurrent.futures
import json
import os
import pickle
import threading
import uuid
from pathlib import Path

import pytest

import ray
from ray.exceptions import ActorDiedError
from ray.util.retry_budget import (
    BudgetEvent,
    BudgetGrant,
    BudgetKind,
    BudgetPolicy,
    BudgetSnapshot,
    BudgetStoreCorruption,
    BudgetStoreError,
    BudgetTaskSequence,
    BudgetedActor,
    InMemoryBudgetStore,
    JsonlBudgetStore,
    RestartRetryBudget,
    RetryBudgetExhausted,
    TaskBudgetState,
)


def test_policy_validation_and_normalization() -> None:
    policy = BudgetPolicy(
        window_seconds=2,
        restart_backoff=[1, 2],  # type: ignore[arg-type]
        retry_backoff=(0,),
        retry_exception_types=[ValueError],  # type: ignore[arg-type]
        replay_safe_methods={"a"},  # type: ignore[arg-type]
    )
    assert policy.window_seconds == 2.0
    assert policy.restart_backoff == (1.0, 2.0)
    assert policy.retry_exception_types == (ValueError,)
    assert policy.replay_safe_methods == frozenset({"a"})

    invalid_values = [
        {"max_restarts": -2},
        {"max_task_retries": 1.2},
        {"max_total_spend": False},
        {"window_seconds": 0},
        {"window_seconds": -1},
        {"restart_backoff": ()},
        {"restart_backoff": (-0.1,)},
        {"retry_backoff": []},
        {"orphan_policy": "delete"},
        {"retry_exception_types": (int,)},
    ]
    for kwargs in invalid_values:
        with pytest.raises(ValueError):
            BudgetPolicy(**kwargs)


def test_restart_counting_rejection_and_events() -> None:
    budget = RestartRetryBudget(
        "actor-1", BudgetPolicy(max_restarts=1, max_total_spend=3)
    )
    grant = budget.reserve_restart("req-1", cause="boom")
    assert grant.kind is BudgetKind.RESTART
    assert grant.state == "pending"
    assert grant.spent_before == 0
    assert grant.remaining == 0
    assert budget.snapshot().pending == 1
    assert budget.commit(grant.token) is True

    snapshot = budget.snapshot()
    assert snapshot.restart_spent == 1
    assert snapshot.total_spent == 1
    assert snapshot.restarts_remaining == 0
    assert snapshot.pending == 0

    with pytest.raises(RetryBudgetExhausted) as exc_info:
        budget.reserve_restart("req-2", cause="boom again")
    error = exc_info.value
    assert error.limit_kind == "restarts"
    assert error.spent == 1
    assert error.actor_key == "actor-1"
    assert error.last_error

    snapshot = budget.snapshot()
    assert snapshot.exhausted == "restarts"
    exhausted_events = [e for e in budget.events() if e.phase == "exhausted"]
    assert len(exhausted_events) == 1
    assert exhausted_events[0].detail["limit_kind"] == "restarts"


def test_pending_reservation_blocks_until_released() -> None:
    budget = RestartRetryBudget("a", BudgetPolicy(max_restarts=1))
    first = budget.reserve_restart("r1", cause="x")
    with pytest.raises(RetryBudgetExhausted) as exc_info:
        budget.reserve_restart("r2", cause="y")
    assert exc_info.value.spent == 1

    assert budget.release(first.token, reason="give up") is True
    assert budget.snapshot().restarts_remaining == 1
    second = budget.reserve_restart("r3", cause="z")
    assert second.token != first.token


def test_commit_and_release_are_terminal_and_idempotent() -> None:
    store = InMemoryBudgetStore()
    budget = RestartRetryBudget("a", BudgetPolicy(max_restarts=2), store)
    grant = budget.reserve_restart("r1", cause="x")
    assert budget.commit(grant.token) is True
    assert budget.commit(grant.token) is False
    assert budget.release(grant.token, reason="late") is False

    events = budget.events()
    assert [e.phase for e in events] == ["granted", "committed"]
    assert budget.snapshot().last_event_seq == 2

    other = budget.reserve_restart("r2", cause="y")
    assert budget.release(other.token, reason="no longer needed") is True
    assert budget.release(other.token, reason="again") is False
    assert budget.commit(other.token) is False
    assert [e.phase for e in budget.events()][-2:] == ["granted", "released"]


def test_task_retry_limits_are_per_task() -> None:
    budget = RestartRetryBudget("a", BudgetPolicy(max_task_retries=1))
    first = budget.reserve_retry(
        "q1", task_key="echo:0", attempt=2, error_type="ValueError"
    )
    assert first.remaining == 0
    budget.commit(first.token)

    with pytest.raises(RetryBudgetExhausted) as exc_info:
        budget.reserve_retry(
            "q2", task_key="echo:0", attempt=3, error_type="ValueError"
        )
    assert exc_info.value.limit_kind == "task_retries"

    other = budget.reserve_retry(
        "q3", task_key="echo:1", attempt=2, error_type="ValueError"
    )
    assert other.remaining == 0
    budget.commit(other.token)
    snapshot = budget.snapshot()
    assert snapshot.tasks["echo:0"].retries_spent == 1
    assert snapshot.tasks["echo:1"].retries_spent == 1
    assert [task.task_key for task in snapshot.tasks] == ["echo:0", "echo:1"]


def test_total_budget_is_shared_and_order_is_restarts_then_total() -> None:
    budget = RestartRetryBudget(
        "a",
        BudgetPolicy(max_restarts=5, max_task_retries=5, max_total_spend=2),
    )
    restart = budget.reserve_restart("r", cause="x")
    retry = budget.reserve_retry(
        "q", task_key="t:0", attempt=1, error_type="ValueError"
    )
    budget.commit(restart.token)
    budget.commit(retry.token)

    with pytest.raises(RetryBudgetExhausted) as exc_info:
        budget.reserve_retry("q2", task_key="t:1", attempt=1, error_type="ValueError")
    assert exc_info.value.limit_kind == "total"
    assert exc_info.value.spent == 2

    tight = RestartRetryBudget("b", BudgetPolicy(max_restarts=0, max_total_spend=0))
    with pytest.raises(RetryBudgetExhausted) as exc_info:
        tight.reserve_restart("r", cause="x")
    assert exc_info.value.limit_kind == "restarts"


def test_window_replenishment_and_retry_after() -> None:
    current = 100.0
    budget = RestartRetryBudget(
        "a",
        BudgetPolicy(max_restarts=1, window_seconds=10.0),
        clock=lambda: current,
    )
    grant = budget.reserve_restart("r1", cause="x")
    budget.commit(grant.token)

    current = 105.0
    with pytest.raises(RetryBudgetExhausted) as exc_info:
        budget.reserve_restart("r2", cause="y")
    assert exc_info.value.retry_after_s == 5.0

    current = 110.001
    next_grant = budget.reserve_restart("r3", cause="z")
    assert next_grant.spent_before == 0
    assert budget.snapshot().restart_spent == 0


def test_backoff_sequence_and_cap() -> None:
    budget = RestartRetryBudget(
        "a",
        BudgetPolicy(
            max_restarts=10,
            restart_backoff=(0.1, 0.2),
            max_backoff_seconds=0.15,
        ),
    )
    delays = []
    for index in range(3):
        grant = budget.reserve_restart(f"r{index}", cause="x")
        delays.append(grant.delay_s)
        budget.commit(grant.token)
    assert delays == [0.1, 0.15, 0.15]


def test_request_id_is_idempotent_and_validates_arguments() -> None:
    budget = RestartRetryBudget("a", BudgetPolicy(max_restarts=2))
    first = budget.reserve_restart("same", cause="boom", preempted=True)
    second = budget.reserve_restart("same", cause="boom", preempted=True)
    assert first.token == second.token
    assert second.replayed is True
    assert second.state == "pending"

    with pytest.raises(ValueError):
        budget.reserve_restart("same", cause="different", preempted=True)
    with pytest.raises(ValueError):
        budget.reserve_restart("same", cause="boom", preempted=False)

    error = RuntimeError("fatal")
    error_grant = budget.reserve_restart("error-request", cause=error)
    replay_error_grant = budget.reserve_restart("error-request", cause=error)
    assert error_grant.token == replay_error_grant.token
    assert replay_error_grant.replayed is True


def test_finish_task_terminal_state_is_idempotent() -> None:
    budget = RestartRetryBudget("a", BudgetPolicy())
    budget.finish_task("t:0", "succeeded", 1)
    first_events = len(budget.events())
    budget.finish_task("t:0", "failed", 2, "ValueError")
    assert len(budget.events()) == first_events
    task = budget.snapshot().tasks["t:0"]
    assert task.outcome == "succeeded"
    assert task.attempts == 1

    with pytest.raises(ValueError):
        budget.finish_task("t:1", "not-an-outcome", 1)


def test_concurrent_reservations_never_oversubscribe() -> None:
    budget = RestartRetryBudget("a", BudgetPolicy(max_restarts=16, max_total_spend=-1))
    barrier = threading.Barrier(32)
    successes: list[BudgetGrant] = []
    failures: list[RetryBudgetExhausted] = []
    lock = threading.Lock()

    def worker(index: int) -> None:
        barrier.wait()
        try:
            grant = budget.reserve_restart(f"req-{index}", cause="x")
            with lock:
                successes.append(grant)
        except RetryBudgetExhausted as exc:
            with lock:
                failures.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(32)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(successes) == 16
    assert len(failures) == 16
    assert budget.snapshot().pending == 16
    assert len({grant.token for grant in successes}) == 16


def test_dataclasses_and_exception_are_picklable() -> None:
    task = TaskBudgetState("t:0", 2, 1, 1, "failed", "ValueError")
    snapshot = BudgetSnapshot(
        actor_key="a",
        incarnation=1,
        restart_spent=1,
        retry_spent=1,
        total_spent=2,
        pending=0,
        preemption_restarts=0,
        recovered_orphans=0,
        restarts_remaining=1,
        total_remaining=0,
        exhausted="total",
        last_event_seq=4,
        tasks=BudgetTaskSequence([task]),
    )
    event = BudgetEvent(1, 1.0, "restart", "granted", "r", "tok", None, 0, 0.0, None)
    grant = BudgetGrant(
        "tok", BudgetKind.RESTART, "r", None, 0, 0.0, 0, 1, "pending", False
    )
    error = RetryBudgetExhausted(
        actor_key="a", kind=BudgetKind.RESTART, limit_kind="restarts"
    )
    for value in (task, snapshot, event, grant, error):
        restored = pickle.loads(pickle.dumps(value))
        assert restored == value


def test_jsonl_store_format_empty_file_and_half_line(tmp_path: Path) -> None:
    path = tmp_path / "budget.jsonl"
    store = JsonlBudgetStore(path)
    budget = RestartRetryBudget("actor", BudgetPolicy(max_restarts=1), store)
    grant = budget.reserve_restart("r1", cause="x")
    budget.commit(grant.token)

    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    assert len(lines) == 2
    for line in lines:
        assert line.endswith("\n")
        record = json.loads(line)
        assert list(record.keys()) == sorted(record.keys())
        assert record["schema_version"] == 1
        assert record["actor_key"] == "actor"

    path.touch()
    assert JsonlBudgetStore(path).load() != ()  # existing records remain

    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"schema_version": 1, "partial":')
    reloaded = RestartRetryBudget(
        "actor", BudgetPolicy(max_restarts=1), JsonlBudgetStore(path)
    )
    assert reloaded.snapshot().last_event_seq == 2
    assert reloaded.snapshot().restart_spent == 1


def test_empty_file_recovery(tmp_path: Path) -> None:
    path = tmp_path / "empty.jsonl"
    path.write_text("", encoding="utf-8")
    budget = RestartRetryBudget("a", BudgetPolicy(), JsonlBudgetStore(path))
    snapshot = budget.snapshot()
    assert snapshot.last_event_seq == 0
    assert snapshot.tasks == ()
    assert budget.events() == ()

    missing = tmp_path / "missing.jsonl"
    missing_budget = RestartRetryBudget("a", BudgetPolicy(), JsonlBudgetStore(missing))
    assert missing_budget.snapshot().last_event_seq == 0


def test_jsonl_middle_corruption_reports_path_and_line(tmp_path: Path) -> None:
    path = tmp_path / "corrupt.jsonl"
    valid_record = {
        "schema_version": 1,
        "actor_key": "a",
        "seq": 1,
        "ts": 1.0,
        "kind": "restart",
        "phase": "granted",
        "request_id": "r",
        "token": "t",
        "task_key": None,
        "attempt": 0,
        "delay_s": 0.0,
        "error_type": None,
        "detail": None,
    }
    path.write_text(
        json.dumps(valid_record, sort_keys=True)
        + "\nthis-is-not-json\n"
        + json.dumps({**valid_record, "seq": 2}, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(BudgetStoreCorruption) as exc_info:
        JsonlBudgetStore(path).load()
    assert exc_info.value.path == str(path)
    assert exc_info.value.line == 2


def test_unknown_schema_and_actor_mismatch_are_corruption(tmp_path: Path) -> None:
    path = tmp_path / "schema.jsonl"
    record = {
        "schema_version": 2,
        "actor_key": "a",
        "seq": 1,
        "ts": 1.0,
        "kind": "restart",
        "phase": "granted",
        "request_id": "r",
        "token": "t",
        "task_key": None,
        "attempt": 0,
        "delay_s": 0.0,
        "error_type": None,
        "detail": None,
    }
    path.write_text(json.dumps(record) + "\n", encoding="utf-8")
    with pytest.raises(BudgetStoreCorruption):
        JsonlBudgetStore(path).load()

    record["schema_version"] = 1
    path.write_text(json.dumps(record) + "\n", encoding="utf-8")
    with pytest.raises(BudgetStoreCorruption):
        RestartRetryBudget("other", BudgetPolicy(), JsonlBudgetStore(path))


def test_seq_regression_is_corruption_without_repair(tmp_path: Path) -> None:
    store = JsonlBudgetStore(tmp_path / "seq.jsonl")
    budget = RestartRetryBudget("a", BudgetPolicy(max_restarts=3), store)
    for index in range(2):
        grant = budget.reserve_restart(f"r{index}", cause="x")
        budget.commit(grant.token)

    records = list(store.load())
    records[1]["seq"] = 1  # duplicate sequence
    corrupted = tmp_path / "dup.jsonl"
    corrupted.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )
    with pytest.raises(BudgetStoreCorruption):
        RestartRetryBudget("a", BudgetPolicy(), JsonlBudgetStore(corrupted))


def test_orphan_pending_is_spent_or_released(tmp_path: Path) -> None:
    spend_path = tmp_path / "spend.jsonl"
    first = RestartRetryBudget(
        "actor",
        BudgetPolicy(max_restarts=1, orphan_policy="spend"),
        JsonlBudgetStore(spend_path),
    )
    first.reserve_restart("orphan", cause="boom")

    recovered = RestartRetryBudget(
        "actor",
        BudgetPolicy(max_restarts=1, orphan_policy="spend"),
        JsonlBudgetStore(spend_path),
    )
    snapshot = recovered.snapshot()
    assert snapshot.recovered_orphans == 1
    assert snapshot.restart_spent == 1
    assert snapshot.pending == 0
    assert snapshot.restarts_remaining == 0

    release_path = tmp_path / "release.jsonl"
    releasing = RestartRetryBudget(
        "actor",
        BudgetPolicy(max_restarts=1, orphan_policy="release"),
        JsonlBudgetStore(release_path),
    )
    releasing.reserve_restart("orphan", cause="boom")
    recovered_release = RestartRetryBudget(
        "actor",
        BudgetPolicy(max_restarts=1, orphan_policy="release"),
        JsonlBudgetStore(release_path),
    )
    release_snapshot = recovered_release.snapshot()
    assert release_snapshot.recovered_orphans == 1
    assert release_snapshot.restart_spent == 0
    assert release_snapshot.restarts_remaining == 1


def test_store_append_failure_leaves_memory_unchanged() -> None:
    class FlakyStore(InMemoryBudgetStore):
        fail_append = False

        def append(self, record):  # type: ignore[override]
            if self.fail_append:
                raise OSError("disk full")
            super().append(record)

    store = FlakyStore()
    budget = RestartRetryBudget("a", BudgetPolicy(max_restarts=2), store)
    grant = budget.reserve_restart("r1", cause="x")
    before = budget.snapshot()

    store.fail_append = True
    with pytest.raises(BudgetStoreError):
        budget.commit(grant.token)
    after = budget.snapshot()
    assert after.last_event_seq == before.last_event_seq
    assert after.pending == 1
    assert len(budget.events()) == before.last_event_seq


def test_jsonl_recovery_restores_counts_tasks_and_seq(tmp_path: Path) -> None:
    path = tmp_path / "recovery.jsonl"
    policy = BudgetPolicy(max_restarts=3, max_task_retries=3, max_total_spend=10)
    budget = RestartRetryBudget("actor", policy, JsonlBudgetStore(path))
    restart = budget.reserve_restart("restart-1", cause="boom")
    budget.commit(restart.token)
    retry = budget.reserve_retry(
        "retry-1", task_key="echo:0", attempt=2, error_type="ValueError"
    )
    budget.commit(retry.token)
    budget.finish_task("echo:0", "succeeded", 2)
    before = budget.snapshot()
    before_events = budget.events()

    recovered = RestartRetryBudget("actor", policy, JsonlBudgetStore(path))
    after = recovered.snapshot()
    assert after.restart_spent == before.restart_spent
    assert after.retry_spent == before.retry_spent
    assert after.total_spent == before.total_spent
    assert after.tasks == before.tasks
    assert after.last_event_seq == before.last_event_seq

    after_events = recovered.events()
    assert [event.seq for event in after_events] == list(
        range(1, after.last_event_seq + 1)
    )
    assert after_events == before_events

    next_grant = recovered.reserve_restart("restart-2", cause="again")
    assert next_grant.token
    assert recovered.events()[-1].seq == after.last_event_seq + 1


def test_budgeted_actor_rejects_forbidden_options() -> None:
    for key in (
        "max_restarts",
        "max_task_retries",
        "retry_exceptions",
        "name",
        "lifetime",
    ):
        with pytest.raises(ValueError):
            BudgetedActor(
                object,
                policy=BudgetPolicy(),
                actor_options={key: "value"},
            )


# ----------------------------------------------------------------------
# Ray integration tests
# ----------------------------------------------------------------------


@ray.remote
class Crashy:
    def __init__(self, once_path: str | None = None, flaky_failures: int = 2):
        self.identity = uuid.uuid4().hex
        self.once_path = Path(once_path) if once_path else None
        self.calls = 0
        self.flaky_failures = flaky_failures

    def whoami(self) -> str:
        return self.identity

    def echo(self, value):
        return value

    def crash_once(self, value):
        if self.once_path is not None and not self.once_path.exists():
            self.once_path.write_text("crashed", encoding="utf-8")
            os._exit(1)
        return value

    def always_crash(self, value=None):
        os._exit(1)

    def flaky(self, value: int) -> int:
        self.calls += 1
        if self.calls <= self.flaky_failures:
            raise ValueError(f"flaky failure {self.calls} for {value}")
        return value + 1

    def generator(self):
        yield 1


@ray.remote
class FlakyInitCrashy:
    def __init__(self, marker_path: str):
        path = Path(marker_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        count = int(path.read_text()) if path.exists() else 0
        count += 1
        path.write_text(str(count), encoding="utf-8")
        if count == 2:
            os._exit(1)

    def echo(self, value):
        return value

    def die(self):
        os._exit(1)


@pytest.fixture(scope="module")
def ray_cluster():
    if not ray.is_initialized():
        ray.init(num_cpus=2, include_dashboard=False, log_to_driver=False)
    yield
    ray.shutdown()


def _make_actor(tmp_path: Path, **kwargs) -> BudgetedActor:
    root = Path(tmp_path)
    root.mkdir(parents=True, exist_ok=True)
    once_path = root / "crash-once.marker"
    store_path = root / "budget.jsonl"
    kwargs.setdefault("actor_key", "crashy")
    kwargs.setdefault("store", JsonlBudgetStore(store_path))
    kwargs.setdefault(
        "policy",
        BudgetPolicy(
            max_restarts=2,
            max_task_retries=2,
            replay_safe_methods=frozenset({"crash_once"}),
            retry_exception_types=(ValueError,),
        ),
    )
    return BudgetedActor(
        Crashy, args=(str(once_path),), kwargs={"flaky_failures": 2}, **kwargs
    )


def test_integration_crash_once_replays_and_counts(ray_cluster, tmp_path):
    actor = _make_actor(tmp_path)
    try:
        before = actor.call("whoami")
        assert actor.call("crash_once", 7) == 7
        snapshot = actor.snapshot()
        assert snapshot.restart_spent == 1
        assert snapshot.retry_spent == 1
        assert snapshot.incarnation == 1
        after = actor.call("whoami")
        assert before != after
    finally:
        actor.close(kill=True)


def test_integration_non_replayable_dies_then_new_instance_works(ray_cluster, tmp_path):
    actor = _make_actor(tmp_path)
    try:
        with pytest.raises(ActorDiedError):
            actor.call("always_crash")
        snapshot = actor.snapshot()
        assert snapshot.restart_spent == 1
        assert snapshot.retry_spent == 0
        assert snapshot.tasks["always_crash:0"].outcome == "failed"
        assert actor.call("echo", 1) == 1
    finally:
        actor.close(kill=True)


def test_integration_restart_limit_exhaustion_and_no_new_actor(
    ray_cluster, tmp_path, monkeypatch
):
    actor = _make_actor(
        tmp_path,
        policy=BudgetPolicy(max_restarts=1, max_task_retries=0),
        store=JsonlBudgetStore(tmp_path / "restart-limit.jsonl"),
        actor_key="restart-limit",
    )
    created = 0
    original_create = actor._create_actor_handle

    def counting_create():
        nonlocal created
        created += 1
        return original_create()

    actor._create_actor_handle = counting_create
    try:
        with pytest.raises(ActorDiedError):
            actor.call("always_crash")
        assert created == 1

        with pytest.raises(RetryBudgetExhausted) as exc_info:
            actor.call("always_crash")
        assert exc_info.value.limit_kind == "restarts"
        assert exc_info.value.last_error
        incarnation = actor.snapshot().incarnation
        assert incarnation == 1
        assert created == 1

        with pytest.raises(RetryBudgetExhausted):
            actor.call("always_crash")
        assert actor.snapshot().incarnation == incarnation
        assert created == 1
    finally:
        actor.close(kill=True)


def test_integration_task_retry_success_and_exhaustion(ray_cluster, tmp_path):
    success_actor = _make_actor(
        tmp_path / "success",
        policy=BudgetPolicy(max_task_retries=2, retry_exception_types=(ValueError,)),
        store=JsonlBudgetStore(tmp_path / "success" / "b.jsonl"),
        actor_key="flaky-success",
    )
    try:
        assert success_actor.call("flaky", 2) == 3
        snapshot = success_actor.snapshot()
        assert snapshot.retry_spent == 2
        flaky_task = snapshot.tasks["flaky:0"]
        assert flaky_task.outcome == "succeeded"
        assert flaky_task.last_error_type == "ValueError"
    finally:
        success_actor.close(kill=True)

    fail_dir = tmp_path / "failure"
    fail_actor = _make_actor(
        fail_dir,
        policy=BudgetPolicy(max_task_retries=1, retry_exception_types=(ValueError,)),
        store=JsonlBudgetStore(fail_dir / "b.jsonl"),
        actor_key="flaky-failure",
    )
    try:
        with pytest.raises(RetryBudgetExhausted) as exc_info:
            fail_actor.call("flaky", 2)
        assert exc_info.value.limit_kind == "task_retries"
        assert isinstance(exc_info.value.__cause__, ValueError)
    finally:
        fail_actor.close(kill=True)


def test_integration_submit_fifo_order_close_and_snapshot(ray_cluster, tmp_path):
    actor = _make_actor(
        tmp_path,
        policy=BudgetPolicy(),
        store=JsonlBudgetStore(tmp_path / "fifo.jsonl"),
        actor_key="fifo",
    )
    try:
        futures = [actor.submit("echo", index) for index in range(20)]
        assert all(isinstance(future, concurrent.futures.Future) for future in futures)
        assert [future.result() for future in futures] == list(range(20))
        actor.close()
        with pytest.raises(RuntimeError):
            actor.call("echo", 1)
        assert actor.snapshot().actor_key == "fifo"
    finally:
        # close() is idempotent; this ensures cleanup if assertions fail early.
        actor.close(kill=True)


def test_integration_failed_replacement_creation_consumes_restart(
    ray_cluster, tmp_path
):
    marker = tmp_path / "init-count"
    actor = BudgetedActor(
        FlakyInitCrashy,
        args=(str(marker),),
        policy=BudgetPolicy(max_restarts=2),
        actor_key="flaky-init",
        store=JsonlBudgetStore(tmp_path / "flaky-init.jsonl"),
    )
    try:
        with pytest.raises(ActorDiedError):
            actor.call("die")
        assert actor.call("echo", 9) == 9
        assert marker.read_text(encoding="utf-8") == "3"
        snapshot = actor.snapshot()
        assert snapshot.restart_spent == 2
        assert snapshot.incarnation == 1
    finally:
        actor.close(kill=True)


def test_integration_close_kill_preserves_committed_counts(ray_cluster, tmp_path):
    actor = _make_actor(
        tmp_path,
        policy=BudgetPolicy(max_task_retries=0),
        store=JsonlBudgetStore(tmp_path / "close.jsonl"),
        actor_key="close",
    )
    actor.call("echo", 1)
    before = actor.snapshot()
    actor.close(kill=True)
    after = actor.snapshot()
    assert after.restart_spent == before.restart_spent
    assert after.retry_spent == before.retry_spent
    assert after.total_spent == before.total_spent


def test_integration_generator_method_is_type_error_without_budget(
    ray_cluster, tmp_path
):
    actor = _make_actor(
        tmp_path,
        policy=BudgetPolicy(),
        store=JsonlBudgetStore(tmp_path / "generator.jsonl"),
        actor_key="generator",
    )
    try:
        with pytest.raises(TypeError):
            actor.call("generator")
        assert actor.snapshot().last_event_seq == 0
    finally:
        actor.close()
