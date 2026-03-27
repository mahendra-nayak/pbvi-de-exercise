**Session:** S5 — Pipeline Orchestration and Sign-Off
**Date:** 2026-03-27
**Engineer:** Mahendra Nayak

---

## Task 5.1 — PID File Lifecycle

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 5

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-5.1.1 | PID file written before any processing | `cat data/pipeline/pipeline.pid` returns a valid PID | PASS — `data/pipeline/pipeline.pid` contained `7036` while `--smoke-test-sleep 5` process ran in background |
| TC-5.1.2 | PID file removed on clean exit | `ls data/pipeline/pipeline.pid` → No such file | PASS — `ls: cannot access 'data/pipeline/pipeline.pid': No such file or directory` after `--incremental` run completed cleanly |
| TC-5.1.3 | SIGTERM removes PID file | PID file absent after signal | NOT VERIFIED (live) — SIGTERM does not invoke Python signal handlers on Win32; process terminates via `TerminateProcess` without calling `_sigterm`. Verified by code review: handler removes PID file then calls `sys.exit(0)`. Expected to pass in Linux Docker container. |
| TC-5.1.4 | Concurrent invocation blocked | Second exits with "already running" to stderr | PASS — second process `exit_code=1`, `stderr: Error: pipeline already running (PID 35328)` |
| TC-5.1.5 | Stale PID recovered | Startup succeeds; stale file overwritten | NOT VERIFIED (live) — `os.kill(pid, 0)` raises `OSError: [WinError 87]` on Win32 instead of `ProcessLookupError`; `except ProcessLookupError` branch never fires. Verified by code review: branch calls `os.remove(PID_FILE)` and continues. Expected to pass in Linux Docker container. |

### Prediction Statement
- `run_id = str(uuid.uuid4())` is the first statement in `main()`. PID write follows immediately after the stale/live check — before `argparse.ArgumentParser()` and before any pipeline logic. Any concurrent invocation will see a valid PID file.
- `finally` block wraps all remaining `main()` logic including `raise SystemExit(0)`. `SystemExit` propagates through `finally`, so PID file is removed on every clean exit path.
- SIGTERM handler registered before the `try:` block. On Linux: handler fires, removes PID file, calls `sys.exit(0)`. On Win32: handler does not fire (platform limitation).
- Second concurrent invocation finds PID file present, calls `os.kill(pid, 0)`, process is alive → prints error to stderr, exits with code 1.
- Stale PID file: `os.kill(pid, 0)` raises `ProcessLookupError` on Linux → `os.remove(PID_FILE)` then startup continues normally.

### CC Challenge Output
1. `--smoke-test-sleep N` argument not in original task prompt — added to pipeline.py as a hidden (`argparse.SUPPRESS`) test-instrumentation argument required by TC-5.1.1 and TC-5.1.3. **Accepted.**
2. `PermissionError` branch (live process owned by another user) — same stderr message and exit code 1 as the alive-process path; not exercised separately. **Accepted** (code review confirms identical behaviour).
3. `os.makedirs(os.path.dirname(PID_FILE), exist_ok=True)` added before PID write — guards against fresh checkout where `data/pipeline/` does not exist. Not in task prompt. **Accepted** (correctness addition; no invariant conflict).
4. TC-5.1.3 and TC-5.1.5 not exercisable on Win32 development host — documented as scope decisions. **Accepted.**

### Code Review
Invariants touched: INV-41a, INV-41b, INV-41c, INV-41d, INV-41e
- INV-41a: `run_id = str(uuid.uuid4())` at `pipeline.py:11`. PID write at line 28 (`open(PID_FILE, 'w').write(str(os.getpid()))`). `parser = argparse.ArgumentParser(...)` at line 38. PID write precedes `parse_args()` — confirmed.
- INV-41b: `finally:` block at `pipeline.py:65–67`: `if os.path.exists(PID_FILE): os.remove(PID_FILE)`. Wraps `raise SystemExit(0)` and all pipeline logic. Confirmed by TC-5.1.2: file absent after clean exit.
- INV-41c: `signal.signal(signal.SIGTERM, _sigterm)` at `pipeline.py:35`, before the `try:` block at line 37. No Bronze/Silver/Gold invocation precedes line 35. Confirmed.
- INV-41d: `os.kill(pid, 0)` at line 17. No exception raised when process alive → falls through to `print(f"Error: pipeline already running (PID {pid})", file=sys.stderr)` + `sys.exit(1)`. Confirmed by TC-5.1.4: `exit_code=1`, stderr message matches exactly.
- INV-41e: `except ProcessLookupError: os.remove(PID_FILE)` at lines 20–21. No `sys.exit` in this branch — execution continues to PID write and startup. Confirmed by code review; live test not possible on Win32.

### Scope Decisions
- `--smoke-test-sleep N` added as a hidden argparse argument to enable TC-5.1.1 and TC-5.1.3. The argument sleeps N seconds then exits via `raise SystemExit(0)` inside `try`, ensuring `finally` fires and PID file is removed on clean exit.
- TC-5.1.3 (SIGTERM) and TC-5.1.5 (stale PID) verified by code review only. Both depend on POSIX semantics (`signal.signal` handler delivery, `ProcessLookupError` from `os.kill(pid, 0)`) not available on Win32. Operational target is the Linux Docker container (`python:3.11-slim`) where both behaviours work as specified.

### Verification Verdict
[Yes] TC-5.1.1 passed (live)
[Yes] TC-5.1.2 passed (live)
[Yes] TC-5.1.3 verified by code review (Win32 platform limitation documented)
[Yes] TC-5.1.4 passed (live)
[Yes] TC-5.1.5 verified by code review (Win32 platform limitation documented)
[Yes] CC challenge reviewed
[Yes] Code review complete (INV-41a through INV-41e)
[Yes] Scope decisions documented

**Status: Completed**

---

## Task 5.2 — Run Log Writer and `pipeline/run_log.py`

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 5

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-5.2.1 | First write creates file with 1 row | `read_parquet(RUN_LOG_PATH)` has 1 row | PASS — `rows=1`, `model=bronze_transactions`, `status=SUCCESS` |
| TC-5.2.2 | Second write appends, not overwrites | File has 2 rows, both present | PASS — `rows=2`, `models=['bronze_transactions', 'silver_transactions']` |
| TC-5.2.3 | error_message required on FAILED | ValueError raised | PASS — `ValueError: error_message is required when status is FAILED` |
| TC-5.2.4 | error_message forbidden on SUCCESS | ValueError raised | PASS — `ValueError: error_message must be None when status is 'SUCCESS'` |
| TC-5.2.5 | Filesystem path stripped from error | No '/' substring in output | PASS — `cleaned='dbt failed at  line 42'`, `has_slash=False` |
| TC-5.2.6 | Truncation at 500 chars | Output ≤ 500 chars, ends with '[truncated]' | PASS — `len=500`, `ends_truncated=True` |
| TC-5.2.7 | records_rejected forbidden on BRONZE layer | ValueError raised | PASS — `ValueError: records_rejected is only permitted for SILVER layer rows` |
| TC-5.2.8 | Atomic write: temp in same dir as canonical | `.tmp_run_log.parquet` in `data/pipeline/` | PASS — `temp_path=data/pipeline/.tmp_run_log.parquet`, `existed_before_replace=True` |

### Prediction Statement
- First `write_run_log_row` call: no existing file → `combined = new_row` → write to temp → replace canonical → 1-row file.
- Second call: reads existing 1-row file → concat → 2-row file written atomically.
- `status='FAILED'` with `error_message=None` raises `ValueError` before any DataFrame work — validation is the first block in the function.
- `status='SUCCESS'` with `error_message` set raises `ValueError` — only `FAILED` status permits a non-null error message.
- `sanitise_error` strips all `/\S*` matches (filesystem paths) before truncation; 600-char input → 500-char output ending with `[truncated]`.
- `records_rejected` on a non-SILVER layer raises `ValueError`.
- Temp file is `data/pipeline/.tmp_run_log.parquet` — same directory as canonical; `os.replace` atomically overwrites the canonical on success.

### CC Challenge Output
1. `SKIPPED` status with `error_message=None` — not separately tested; identical validation path as `SUCCESS`. **Accepted** (code path is `status != 'FAILED' and error_message is not None` — covers both SUCCESS and SKIPPED).
2. Invalid `pipeline_type` (e.g. `'incremental'` lowercase) raises `ValueError` — not live-tested. **Accepted** (frozenset membership check is the first validation line; code review confirms).
3. Invalid `layer` raises `ValueError` — not live-tested. **Accepted** (same frozenset pattern as pipeline_type).
4. `sanitise_error` with input that has no `/` — returns input unchanged (up to max_chars). **Accepted** (regex substitution produces no change when no match; not exercised).
5. `sanitise_error` with input that is entirely a path (e.g. `/app/run.py`) — result is `'unknown error'` due to `or 'unknown error'` fallback. **Accepted** (code review confirms the `or 'unknown error'` guard).
6. `os.replace` used instead of `os.rename` — not in original task prompt. **Accepted** (scope decision: `os.rename` raises `FileExistsError` on Windows when destination exists; `os.replace` is atomic on Linux and correct on Windows).

### Code Review
Invariants touched: INV-37, INV-37b, INV-39, INV-39b, INV-40b
- INV-37: Atomic append sequence in `write_run_log_row`: read existing → `pd.concat` → `to_parquet(TEMP_LOG_PATH)` → `os.replace(TEMP_LOG_PATH, RUN_LOG_PATH)`. Never writes directly to `RUN_LOG_PATH`. Confirmed at `run_log.py:63–70`.
- INV-37b: `TEMP_LOG_PATH = 'data/pipeline/.tmp_run_log.parquet'` — dot-prefixed, same directory as `RUN_LOG_PATH = 'data/pipeline/run_log.parquet'`. No `/tmp` or `tempfile` usage anywhere in file. Confirmed by TC-5.2.8: `temp_path=data/pipeline/.tmp_run_log.parquet`.
- INV-39: All six validation checks (`pipeline_type`, `layer`, `status`, FAILED/error_message, SUCCESS/error_message, non-SILVER/records_rejected) are `if` statements at lines 38–46, before `pd.DataFrame([{...}])` at line 48. Confirmed at `run_log.py:38–46`. TC-5.2.3, TC-5.2.4, TC-5.2.7 all confirm `ValueError` raised without any file being written.
- INV-39b: `records_rejected` validation at line 46 (`if layer != 'SILVER' and records_rejected is not None`) ensures only quarantine-count rows (hard rejections) can carry this field. `_is_resolvable=false` records are not quarantined and thus not counted here. Confirmed by TC-5.2.7.
- INV-40b: `pipeline_type in VALID_PIPELINE_TYPES` and `layer in VALID_LAYERS` checks at lines 38–41 use `frozenset` membership — not free-form string comparison. `VALID_PIPELINE_TYPES = frozenset({'HISTORICAL', 'INCREMENTAL'})`, `VALID_LAYERS = frozenset({'BRONZE', 'SILVER', 'GOLD'})`. Confirmed at `run_log.py:7–9`.

### Scope Decisions
- `os.replace` used instead of `os.rename` — `os.rename` raises `FileExistsError` on Windows when the destination already exists. `os.replace` is POSIX-atomic on Linux (the operational target) and also handles the overwrite case on Windows. Behaviour is identical on Linux; this is purely a cross-platform correctness fix.
- `pipeline_type` defaults to `'HISTORICAL'` in the function signature — callers in `silver_runner.py` that were written before the parameter existed do not pass it. Default ensures backward compatibility without changing caller code.

### Verification Verdict
[Yes] All planned cases passed (live)
[Yes] CC challenge reviewed
[Yes] Code review complete (INV-37, INV-37b, INV-39, INV-39b, INV-40b)
[Yes] Scope decisions documented

**Status: Completed**

---

## Task 5.3 — Watermark Control and `--reset-watermark`

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 5

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-5.3.1 | `read_watermark` returns None when no file | Returns None | |
| TC-5.3.2 | `write_watermark` creates 1-row file | File has exactly 1 row | |
| TC-5.3.3 | Second `write_watermark` replaces not appends | File still has 1 row | |
| TC-5.3.4 | `--reset-watermark` without `--confirm` exits 1 | Exit 1, "confirm required" message | |
| TC-5.3.5 | `--reset-watermark` touches only control.parquet | No Bronze/Silver/Gold/quarantine files modified | |
| TC-5.3.6 | Invalid date exits 1 | Exit 1, "invalid date" message | |

### Prediction Statement

### CC Challenge Output
[Paste CC's response to: 'What did you not test in this task?'
For each item: accepted (added case) / rejected (reason).]

### Code Review
Invariants touched: INV-33b, INV-36b
- INV-33b: Confirm `write_watermark` always writes a fresh single-row DataFrame — no `pandas.concat` in this function
- INV-36b: Confirm `--reset-watermark` handler contains no `os.remove`, `os.rmdir`, `to_parquet`, or `open(..., 'w')` for any path other than `CONTROL_PATH` and `TEMP_CONTROL_PATH`

### Scope Decisions

### Verification Verdict
[ ] All planned cases passed
[ ] CC challenge reviewed
[ ] Code review complete (invariant-touching)
[ ] Scope decisions documented

**Status:**

---

## Task 5.4 — Historical and Incremental Pipeline Orchestration

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 5

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-5.4.1 | Historical: 7 dates processed | Watermark = 2024-01-21 | |
| TC-5.4.2 | Crash-and-retry: resumes from watermark+1 | Resumes from 2024-01-18; no FM-4 | |
| TC-5.4.3 | Re-run completed history: no-op message | "nothing to do" printed; watermark unchanged | |
| TC-5.4.4 | Watermark not advanced on Silver failure | Watermark = last successfully completed date | |
| TC-5.4.5 | Incremental: no source file → no-op | Exit 0; watermark unchanged; no run log row added | |
| TC-5.4.6 | Silver TC SKIPPED in incremental run | Run log has SKIPPED row for silver_transaction_codes | |
| TC-5.4.7 | Historical: Bronze TC loaded once | `bronze_transaction_codes` in run log exactly once (not 7 times) | |

### Prediction Statement

### CC Challenge Output
[Paste CC's response to: 'What did you not test in this task?'
For each item: accepted (added case) / rejected (reason).]

### Code Review
Invariants touched: INV-33, INV-35, INV-49, INV-49b
- INV-33: Confirm `write_watermark(current, run_id)` is the last statement in the per-date loop body — no statement between it and `current += timedelta(days=1)`
- INV-35: Confirm `effective_start = max(start, watermark + 1)` — `start` is not used directly when watermark is set
- INV-49: Confirm layer calls are sequential — `run_silver_accounts` returns before `run_silver_transactions` is called; `run_silver_transactions` returns before `run_gold` is called; no threading
- INV-49b: Confirm path-existence check in `run_silver_transactions` and `run_silver_accounts` occurs before `subprocess.run(['dbt', ...])`

### Scope Decisions

### Verification Verdict
[ ] All planned cases passed
[ ] CC challenge reviewed
[ ] Code review complete (invariant-touching)
[ ] Scope decisions documented

**Status:**

---

## Task 5.5 — Phase 8 Sign-Off Verification

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 5

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-5.5.1 | §10.1: Bronze row counts match source CSVs | 7 dates with non-zero `bronze_rows` | |
| TC-5.5.2 | §10.2a: Silver + quarantine = Bronze for each date | `balanced` = true for all 7 dates | |
| TC-5.5.3 | §10.2b: No duplicate transaction_id across Silver | Duplicate query returns 0 rows | |
| TC-5.5.4 | §10.2c: No orphaned transaction codes in Silver | `orphan_codes` = 0 | |
| TC-5.5.5 | §10.2d: No null _signed_amount in Silver | `null_signed` = 0 | |
| TC-5.5.6 | §10.2e: All quarantine records have valid rejection reason | `null_reason` = 0; `invalid_reason` = 0 | |
| TC-5.5.7 | §10.3a: Gold daily_summary has one row per resolvable date | `silver_resolvable_dates` = `gold_daily_rows` | |
| TC-5.5.8 | §10.3b: Gold weekly total_purchases matches Silver PURCHASE count | Mismatch query returns 0 rows | |
| TC-5.5.9 | §10.4: Idempotency — second run counts match first run | All entity counts identical across both runs | |
| TC-5.5.10 | §10.5a: No null _pipeline_run_id in Bronze/Silver/Gold | All three `_null_run_id` columns = 0 | |
| TC-5.5.11 | §10.5b: Every Silver run_id has a SUCCESS row in run log | Orphaned Silver run_id query returns 0 rows | |

### Prediction Statement

### CC Challenge Output
[Paste CC's response to: 'What did you not test in this task?'
For each item: accepted (added case) / rejected (reason).]

### Code Review
No new code in this task — code review not applicable.

### Scope Decisions

### Verification Verdict
[ ] All planned cases passed
[ ] CC challenge reviewed
[ ] Code review complete (invariant-touching)
[ ] Scope decisions documented

**Status:**
