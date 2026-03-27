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
| TC-5.2.1 | First write creates file with 1 row | `read_parquet(RUN_LOG_PATH)` has 1 row | |
| TC-5.2.2 | Second write appends, not overwrites | File has 2 rows, both present | |
| TC-5.2.3 | error_message required on FAILED | ValueError raised | |
| TC-5.2.4 | error_message forbidden on SUCCESS | ValueError raised | |
| TC-5.2.5 | Filesystem path stripped from error | No '/' substring in output | |
| TC-5.2.6 | Truncation at 500 chars | Output ≤ 500 chars, ends with '[truncated]' | |
| TC-5.2.7 | records_rejected forbidden on BRONZE layer | ValueError raised | |
| TC-5.2.8 | Atomic write: temp in same dir as canonical | `.tmp_run_log.parquet` in `data/pipeline/` | |

### Prediction Statement

### CC Challenge Output
[Paste CC's response to: 'What did you not test in this task?'
For each item: accepted (added case) / rejected (reason).]

### Code Review
Invariants touched: INV-37, INV-37b, INV-39, INV-40b
- INV-37: Confirm atomic append uses `os.rename(TEMP_LOG_PATH, RUN_LOG_PATH)` — not `to_parquet(RUN_LOG_PATH)` directly
- INV-37b: Confirm temp path is `data/pipeline/.tmp_run_log.parquet` — same directory as canonical, no `/tmp` usage
- INV-39: Confirm FAILED/error_message validation raises before any DataFrame construction
- INV-40b: Confirm `pipeline_type` and `layer` are validated against frozensets before write

### Scope Decisions

### Verification Verdict
[ ] All planned cases passed
[ ] CC challenge reviewed
[ ] Code review complete (invariant-touching)
[ ] Scope decisions documented

**Status:**

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
