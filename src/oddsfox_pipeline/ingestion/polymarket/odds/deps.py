"""Runtime dependencies for Polymarket odds planning/execution/writer/engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class PlanningRuntime:
    iter_markets_with_tokens: Callable[..., Any]
    iter_due_market_tokens: Callable[..., Any]
    count_due_market_token_exclusions: Callable[..., Any]
    count_candidate_market_tokens: Callable[..., Any]
    get_token_sync_snapshot: Callable[..., Any]
    token_sync_scheduler_state: type


@dataclass(frozen=True)
class ExecutionRuntime:
    fetch_window_with_auto_split_impl: Callable[..., Any]
    fetch_token_history_with_retry: Callable[..., Any]
    default_rate_limiter_factory: Callable[..., Any]
    sync_token_plan: Callable[..., Any]


@dataclass(frozen=True)
class WriterRuntime:
    get_connection: Callable[..., Any]
    refresh_token_odds_daily: Callable[..., Any]
    save_odds_bulk_upsert: Callable[..., Any]
    upsert_skipped_tokens_batch: Callable[..., Any]
    upsert_token_sync_state_batch: Callable[..., Any]
    dynamic_writer_flush_rows: Callable[..., Any]
    flush_writer_buffers: Callable[..., Any]
    apply_writer_item: Callable[..., Any]
    writer_loop: Callable[..., Any]


@dataclass(frozen=True)
class EngineRuntime:
    ensure_duck_db: Callable[..., Any]
    snapshot_raw_layer: Callable[..., Any]
    save_skipped_tokens: Callable[..., Any]
    save_sync_run_metrics: Callable[..., Any]
    reconcile_token_sync_ledger_from_history: Callable[..., Any]
    progress_guardrail: type
    no_progress_timeout_error: type
    thread_cls: type
    thread_pool_executor: type
    wait_fn: Callable[..., Any]
    tqdm_mod: Any
    time_mod: Any


@dataclass(frozen=True)
class OddsSyncRuntime:
    """Live callables and types passed through one odds-sync run."""

    planning: PlanningRuntime
    execution: ExecutionRuntime
    writer: WriterRuntime
    engine: EngineRuntime

    @property
    def iter_markets_with_tokens(self) -> Callable[..., Any]:
        return self.planning.iter_markets_with_tokens

    @property
    def iter_due_market_tokens(self) -> Callable[..., Any]:
        return self.planning.iter_due_market_tokens

    @property
    def count_candidate_market_tokens(self) -> Callable[..., Any]:
        return self.planning.count_candidate_market_tokens

    @property
    def count_due_market_token_exclusions(self) -> Callable[..., Any]:
        return self.planning.count_due_market_token_exclusions

    @property
    def get_token_sync_snapshot(self) -> Callable[..., Any]:
        return self.planning.get_token_sync_snapshot

    @property
    def token_sync_scheduler_state(self) -> type:
        return self.planning.token_sync_scheduler_state

    @property
    def fetch_window_with_auto_split_impl(self) -> Callable[..., Any]:
        return self.execution.fetch_window_with_auto_split_impl

    @property
    def fetch_token_history_with_retry(self) -> Callable[..., Any]:
        return self.execution.fetch_token_history_with_retry

    @property
    def default_rate_limiter_factory(self) -> Callable[..., Any]:
        return self.execution.default_rate_limiter_factory

    @property
    def sync_token_plan(self) -> Callable[..., Any]:
        return self.execution.sync_token_plan

    @property
    def get_connection(self) -> Callable[..., Any]:
        return self.writer.get_connection

    @property
    def refresh_token_odds_daily(self) -> Callable[..., Any]:
        return self.writer.refresh_token_odds_daily

    @property
    def save_odds_bulk_upsert(self) -> Callable[..., Any]:
        return self.writer.save_odds_bulk_upsert

    @property
    def upsert_skipped_tokens_batch(self) -> Callable[..., Any]:
        return self.writer.upsert_skipped_tokens_batch

    @property
    def upsert_token_sync_state_batch(self) -> Callable[..., Any]:
        return self.writer.upsert_token_sync_state_batch

    @property
    def dynamic_writer_flush_rows(self) -> Callable[..., Any]:
        return self.writer.dynamic_writer_flush_rows

    @property
    def flush_writer_buffers(self) -> Callable[..., Any]:
        return self.writer.flush_writer_buffers

    @property
    def apply_writer_item(self) -> Callable[..., Any]:
        return self.writer.apply_writer_item

    @property
    def writer_loop(self) -> Callable[..., Any]:
        return self.writer.writer_loop

    @property
    def ensure_duck_db(self) -> Callable[..., Any]:
        return self.engine.ensure_duck_db

    @property
    def snapshot_raw_layer(self) -> Callable[..., Any]:
        return self.engine.snapshot_raw_layer

    @property
    def save_skipped_tokens(self) -> Callable[..., Any]:
        return self.engine.save_skipped_tokens

    @property
    def save_sync_run_metrics(self) -> Callable[..., Any]:
        return self.engine.save_sync_run_metrics

    @property
    def reconcile_token_sync_ledger_from_history(self) -> Callable[..., Any]:
        return self.engine.reconcile_token_sync_ledger_from_history

    @property
    def progress_guardrail(self) -> type:
        return self.engine.progress_guardrail

    @property
    def no_progress_timeout_error(self) -> type:
        return self.engine.no_progress_timeout_error

    @property
    def thread_cls(self) -> type:
        return self.engine.thread_cls

    @property
    def thread_pool_executor(self) -> type:
        return self.engine.thread_pool_executor

    @property
    def wait_fn(self) -> Callable[..., Any]:
        return self.engine.wait_fn

    @property
    def tqdm_mod(self) -> Any:
        return self.engine.tqdm_mod

    @property
    def time_mod(self) -> Any:
        return self.engine.time_mod


__all__ = [
    "EngineRuntime",
    "ExecutionRuntime",
    "OddsSyncRuntime",
    "PlanningRuntime",
    "WriterRuntime",
]
