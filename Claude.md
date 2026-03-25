# Claude.md — v1.0 · FROZEN · 2026-03-24

---

## SECTION 1 — System Intent

This pipeline ingests credit card transaction, account, and transaction code CSV files from a read-only source directory, promotes them through Bronze → Silver → Gold Parquet layers via Python loaders and dbt models, and produces analyst-facing daily and weekly Gold aggregates backed by a fully traceable audit trail. It does not expose an API, serve a UI, perform outbound HTTP calls, or interact with any external database service. Success means: every source record is either promoted to Silver or formally quarantined; Gold aggregates are numerically correct, idempotent, and fully traceable to their source CSV through `_pipeline_run_id`; and the pipeline can be re-run safely against the same inputs without corrupting any layer.

---

## SECTION 2 — Hard Invariants

If a task prompt CC receives conflicts with any invariant, the invariant wins. CC must flag the conflict immediately and halt — never resolve it silently.

---

INVARIANT INV-01: Every field present in the source CSV must appear in the Bronze Parquet file with identical value and type. No filtering, transformation, enrichment, or defaulting of source fields may occur in the Bronze loader. This is never negotiable.
Enforcement: Bronze loader asserts all source columns present in output schema with matching types before write. Post-load test compares column names and sample values against source CSV.

---

INVARIANT INV-02: Malformed records, null values, and duplicate rows in the source CSV must be written to Bronze unchanged. The Bronze loader must not drop or correct any source record. This is never negotiable.
Enforcement: Bronze loader applies no filter predicates, no WHERE clauses, no conditional skips — reads all rows and writes all rows to Parquet. Post-load test: Bronze row count must equal source CSV row count including nulls and duplicate keys.

---

INVARIANT INV-03: Bronze row count for a given entity and date must equal the row count in the corresponding source CSV file, counting all delimited data rows and excluding only the header row. No rows added, no rows dropped. This is never negotiable.
Enforcement: Bronze loader reads back Bronze partition row count after write and compares to source CSV row count. Sign-off verification: DuckDB query against Bronze partition vs. `SELECT COUNT(*) FROM read_csv_auto('source/...')`.

---

INVARIANT INV-04: The source CSV files in `source/` must never be modified, moved, or deleted by the pipeline. They are read-only inputs. This is never negotiable.
Enforcement: Bronze loader opens source files with `open(path, 'r')` — never `'w'` or `'a'`. Docker Compose mounts `source/` as a read-only bind mount (`ro` flag).

---

INVARIANT INV-05a: Bronze Parquet files must be written atomically. The canonical partition path must not be visible to any reader until the write is complete and the rename has succeeded. This is never negotiable.
Enforcement: Bronze loader writes to `.tmp_{filename}` in the same directory, then `os.rename()` to the canonical path — never writes directly to the canonical path.

---

INVARIANT INV-05b: The temp file used for atomic rename of Bronze partitions must reside in the same directory as the target partition path — not in `/tmp` or any other mount point. This is never negotiable.
Enforcement: Bronze loader constructs temp path as `os.path.join(target_dir, f'.tmp_{filename}')`. Code review confirms no temp path uses `/tmp`, `tempfile.mktemp()`, or any path outside the target directory.

---

INVARIANT INV-05c: The temp file for Bronze atomic writes must be named using the pattern `.tmp_{filename}` (dot-prefixed, hidden file) in the same directory as the target. A non-hidden temp file (e.g., `tmp_{filename}`) may be picked up by DuckDB wildcard scans and treated as a data file. This is never negotiable.
Enforcement: Bronze loader enforces dot-prefixed temp filename via a named constant or helper function. Code review confirms temp filename construction produces a dot-prefixed name.

---

INVARIANT INV-06: Once written, a Bronze partition must never be overwritten or deleted. The path-existence check in `pipeline.py` must prevent any second write to an already-present canonical partition path. This is never negotiable.
Enforcement: `pipeline.py` path-existence check before every Bronze loader invocation — if partition exists, skip and log SKIPPED. Bronze loader raises an error if target canonical path exists at write time rather than overwriting.

---

INVARIANT INV-07: Running the Bronze loader twice against the same source file must produce identical Bronze partition content and row count. No duplicate records may be created by re-running. This is never negotiable.
Enforcement: `pipeline.py` path-existence check (INV-06) is the primary enforcement. Bronze loader, even if called directly, must not write if the canonical path exists.

---

INVARIANT INV-08: After a successful pipeline run, each Gold output entity must exist at exactly one canonical path. No versioned copies, dated suffixes, backup files, or alternative paths for the same Gold entity may exist alongside the canonical path. This is never negotiable.
Enforcement: `pipeline.py` cleans up any temp file if rename failed. Post-run test asserts each Gold directory contains exactly one `.parquet` file.

---

INVARIANT INV-09: Silver transaction promotion must only read from the Bronze partition for the target date. It must not read Bronze partitions from other dates or from source CSVs directly. This is never negotiable.
Enforcement: `silver_transactions` dbt model sources `bronze/transactions/date={{ var("target_date") }}/data.parquet` — no wildcard date glob, no direct CSV reference. Code review confirms no dbt model references `source/` paths.

---

INVARIANT INV-10: Silver transaction codes must be loaded from `silver/transaction_codes/data.parquet` — not from Bronze transaction codes and not from the source CSV — at promotion time. This is never negotiable.
Enforcement: `silver_transactions` dbt model joins to `silver/transaction_codes/data.parquet` — path must be the Silver canonical path, not the Bronze path.

---

INVARIANT INV-11a: Silver accounts promotion must read from the Bronze accounts partition for the target date. This is never negotiable.
Enforcement: `silver_accounts` dbt model sources `bronze/accounts/date={{ var("target_date") }}/data.parquet`.

---

INVARIANT INV-11b: Transaction account resolution at Silver promotion time must read from `silver/accounts/data.parquet` as it exists at the moment of promotion — not from Bronze accounts and not from the source CSV. This is never negotiable.
Enforcement: `silver_transactions` dbt model LEFT JOIN for account resolution references `silver/accounts/data.parquet`, not any Bronze path.

---

INVARIANT INV-12a: A Bronze transaction record with a null or empty value in any of the fields `transaction_id`, `account_id`, `transaction_date`, `amount`, `transaction_code`, or `channel` must be routed to quarantine with rejection reason `NULL_REQUIRED_FIELD`. It must not enter Silver. This is never negotiable.
Enforcement: `silver_transactions` dbt model pre-promotion filter checks all six required fields; failures routed to `silver_quarantine` with `_rejection_reason = 'NULL_REQUIRED_FIELD'`.

---

INVARIANT INV-12b: A Bronze transaction record where `amount` is zero, negative, or non-numeric must be routed to quarantine with rejection reason `INVALID_AMOUNT`. It must not enter Silver. This is never negotiable.
Enforcement: `silver_transactions` dbt model casts `amount` to DECIMAL with error handling; checks `amount > 0` after cast; routes failures to quarantine.

---

INVARIANT INV-12c: A Bronze transaction record where `transaction_id` already exists in any Silver transactions partition must be routed to quarantine with rejection reason `DUPLICATE_TRANSACTION_ID`. It must not enter Silver. This is never negotiable.
Enforcement: `silver_transactions` dbt model checks `transaction_id` against all existing Silver partitions via cross-partition scan before insert.

---

INVARIANT INV-12d: A Bronze transaction record where `transaction_code` is not present in `silver/transaction_codes/data.parquet` must be routed to quarantine with rejection reason `INVALID_TRANSACTION_CODE`. It must not enter Silver. This is never negotiable.
Enforcement: `silver_transactions` dbt model LEFT JOIN to `silver/transaction_codes/data.parquet` on `transaction_code`; no-match records route to quarantine before sign assignment runs.

---

INVARIANT INV-12e: A Bronze transaction record where `channel` is not exactly `ONLINE` or `IN_STORE` must be routed to quarantine with rejection reason `INVALID_CHANNEL`. It must not enter Silver. This is never negotiable.
Enforcement: `silver_transactions` dbt model uses exact case-sensitive string match — `CASE WHEN channel IN ('ONLINE', 'IN_STORE') THEN ... ELSE quarantine`.

---

INVARIANT INV-13: A transaction with an unresolvable `account_id` must enter Silver with `_is_resolvable = false`. It must not be quarantined and must not be silently dropped. It must be excluded from all Gold aggregations. This is never negotiable.
Enforcement: `silver_transactions` dbt model LEFT JOIN to `silver/accounts/data.parquet`; sets `_is_resolvable` based on join result — does not route to quarantine. Both Gold models apply `WHERE _is_resolvable = true` before all aggregations.

---

INVARIANT INV-14: `transaction_id` must be unique across all Silver transactions partitions, not just within a single date partition. A `transaction_id` already present in any prior Silver partition must be rejected as `DUPLICATE_TRANSACTION_ID` at the time of promotion. This is never negotiable.
Enforcement: `silver_transactions` dbt model uniqueness check scans all existing Silver partitions, not only the current target date. Sign-off verification: `SELECT transaction_id, COUNT(*) FROM read_parquet('silver/transactions/**/*.parquet') GROUP BY 1 HAVING COUNT(*) > 1` must return 0.

---

INVARIANT INV-15: The total of (Silver transaction rows + quarantine rows) for a given Bronze source date must equal the total Bronze transaction rows for that date. No record may be silently lost at the Bronze → Silver boundary. This is never negotiable.
Enforcement: `silver_transactions` dbt model — every branch in the promotion logic must write to Silver or to quarantine; no code path exits without doing one of the two. Sign-off verification: Bronze count = Silver count + quarantine count per date.

---

INVARIANT INV-16a: A Bronze account record with a null or empty value in any of the fields `account_id`, `open_date`, `credit_limit`, `current_balance`, `billing_cycle_start`, `billing_cycle_end`, or `account_status` must be routed to quarantine with rejection reason `NULL_REQUIRED_FIELD`. This is never negotiable.
Enforcement: `silver_accounts` dbt model null/empty check on all seven required fields before upsert; failures to quarantine with `_rejection_reason = 'NULL_REQUIRED_FIELD'`.

---

INVARIANT INV-16b: A Bronze account record where `account_status` is not exactly `ACTIVE`, `SUSPENDED`, or `CLOSED` must be routed to quarantine with rejection reason `INVALID_ACCOUNT_STATUS`. This is never negotiable.
Enforcement: `silver_accounts` dbt model uses exact case-sensitive match — `CASE WHEN account_status IN ('ACTIVE', 'SUSPENDED', 'CLOSED') THEN ... ELSE quarantine`.

---

INVARIANT INV-17: Silver accounts must retain only the latest record per `account_id`. When a delta record for an existing `account_id` arrives, the prior record must be replaced in full. No history is retained. This is never negotiable.
Enforcement: `silver_accounts` dbt model uses upsert strategy with `account_id` as merge key. Post-run test: `SELECT account_id, COUNT(*) FROM silver/accounts/data.parquet GROUP BY 1 HAVING COUNT(*) > 1` must return zero rows.

---

INVARIANT INV-18: Rejection reason codes written to quarantine must come exclusively from the pre-defined list: `NULL_REQUIRED_FIELD`, `INVALID_AMOUNT`, `DUPLICATE_TRANSACTION_ID`, `INVALID_TRANSACTION_CODE`, `INVALID_CHANNEL` (transactions); `NULL_REQUIRED_FIELD`, `INVALID_ACCOUNT_STATUS` (accounts). No ad-hoc or unlisted codes are permitted. This is never negotiable.
Enforcement: `silver_transactions` and `silver_accounts` dbt models assign rejection reason as a literal from a defined constant set — never free-form string. Post-run test: `SELECT DISTINCT _rejection_reason FROM read_parquet('silver/quarantine/**/*.parquet')` must be a subset of the defined list.

---

INVARIANT INV-18b: If the pipeline encounters a condition during Silver promotion that does not map to any pre-defined rejection reason code, it must raise a pipeline error (FAILED status in run log) rather than invent a new code or silently promote the record. This is never negotiable.
Enforcement: All conditional branches in `silver_transactions` and `silver_accounts` dbt models must have an explicit `ELSE` that raises an error. `pipeline.py` captures non-zero dbt exit code and records FAILED in run log; watermark must not advance.

---

INVARIANT INV-19: `_signed_amount` must be computed by joining to `silver_transaction_codes.debit_credit_indicator`. `DR` → positive (source amount × +1). `CR` → negative (source amount × −1). The result must equal the source `amount` in absolute magnitude — no scaling, rounding, or truncation is permitted during sign application. This is never negotiable.
Enforcement: `silver_transactions` dbt model: `_signed_amount = CASE WHEN debit_credit_indicator = 'DR' THEN amount ELSE -amount END` — uses exact source `amount` value, not a cast or rounded intermediate.

---

INVARIANT INV-20: No Silver transactions record may have a null `_signed_amount`. A record that cannot be joined to a valid transaction code must be rejected as `INVALID_TRANSACTION_CODE` before Silver promotion, not promoted with a null sign. This is never negotiable.
Enforcement: `silver_transactions` dbt model: `INVALID_TRANSACTION_CODE` check (INV-12d) executes before sign assignment; sign assignment never runs on a record with an unresolved transaction code. Sign-off verification: `SELECT COUNT(*) FROM read_parquet('silver/transactions/**/*.parquet') WHERE _signed_amount IS NULL` must return 0.

---

INVARIANT INV-21: Source `amount` values in Bronze are always positive. `_signed_amount` in Silver acquires its sign purely from `debit_credit_indicator`. The source `amount` field must not be modified or sign-adjusted before this join. This is never negotiable.
Enforcement: Bronze loader writes `amount` exactly as read from CSV — no sign manipulation. `silver_transactions` dbt model uses `amount` column from Bronze directly as magnitude input; no `ABS()`, no negation, no conditional sign logic before the `debit_credit_indicator` join.

---

INVARIANT INV-21b: `debit_credit_indicator` values must match exactly as `DR` or `CR` (case-sensitive, no variants). A record in Silver transaction codes with a value that is not exactly `DR` or `CR` must raise a pipeline error at Silver transaction codes promotion — not proceed with undefined sign assignment behaviour downstream. This is never negotiable.
Enforcement: `silver_transaction_codes` dbt model asserts `SELECT COUNT(*) FROM silver/transaction_codes/data.parquet WHERE debit_credit_indicator NOT IN ('DR', 'CR')` = 0 after promotion; raises FAILED if non-zero.

---

INVARIANT INV-22: Re-running Silver promotion for a date already present in Silver must produce no new records in `silver_transactions`. This idempotency must be enforced by the path-existence check in `pipeline.py` before dbt is invoked — not by relying on dbt internal deduplication. The step must be recorded as SKIPPED in the run log. This is never negotiable.
Enforcement: `pipeline.py` checks `os.path.exists('silver/transactions/date={target_date}/data.parquet')` before every `dbt run --select silver_transactions`; if true, writes SKIPPED to run log and returns without invoking dbt.

---

INVARIANT INV-23: The quarantine file for a given date is append-only. Records already written to quarantine must never be overwritten or deleted. Re-running promotion for an already-promoted date must not re-quarantine already-quarantined records. This is never negotiable.
Enforcement: `pipeline.py` path-existence check for Silver (INV-22) also prevents re-running quarantine writes. `silver_quarantine` dbt model uses `incremental` materialisation with append-only semantics — never `table`.

---

INVARIANT INV-24: Re-running accounts promotion for a date already processed must produce output identical to the first run. The upsert must be deterministic given the same Bronze input. When a single day's Bronze accounts partition contains more than one record for the same `account_id`, the upsert must apply a defined, deterministic tie-breaking rule (last record by source file row order) so the output is reproducible across re-runs. This is never negotiable.
Enforcement: `silver_accounts` dbt model applies `ORDER BY source_file_row_number DESC LIMIT 1` or equivalent deterministic selection when multiple Bronze records for the same `account_id` exist in a single partition.

---

INVARIANT INV-24b: Silver transaction codes must not be re-promoted during incremental pipeline runs. The incremental pipeline must skip the Silver transaction codes promotion step entirely — not merely produce identical output on re-promotion. This is never negotiable.
Enforcement: `pipeline.py` incremental pipeline invocation excludes `silver_transaction_codes` from the dbt model selection list. Path-existence check for `silver/transaction_codes/data.parquet` produces a SKIPPED log entry for incremental runs.

---

INVARIANT INV-24c: `silver/accounts/data.parquet` must be stored as a single non-partitioned file. The Silver accounts model must never write to a date-partitioned, status-partitioned, or otherwise subdivided path structure. This is never negotiable.
Enforcement: `silver_accounts` dbt model output path is the literal `silver/accounts/data.parquet` — no partition key in dbt model configuration. Post-run test: `ls silver/accounts/` must show exactly one file named `data.parquet` and no subdirectories.

---

INVARIANT INV-24d: `bronze/transaction_codes/data.parquet` must be stored at the non-date-partitioned path `bronze/transaction_codes/data.parquet`. The Bronze transaction codes loader must not write to a date-partitioned path. This is never negotiable.
Enforcement: Bronze transaction codes loader output path is the literal constant `bronze/transaction_codes/data.parquet` — not constructed from a date variable. Code review confirms no date variable in transaction codes output path construction.

---

INVARIANT INV-25: Gold models must read exclusively from Silver layer tables. No Gold model may read directly from Bronze or from source CSVs. This is never negotiable.
Enforcement: All Gold dbt models source references resolve only to `silver/` paths — confirmed by code review and dbt lineage graph.

---

INVARIANT INV-26: Gold aggregations must filter to `_is_resolvable = true` before aggregating. Records with `_is_resolvable = false` must be excluded from all Gold counts and sums. This is never negotiable.
Enforcement: `gold_daily_summary` and `gold_weekly_account_summary` dbt models apply `WHERE _is_resolvable = true` in the base CTE before any `GROUP BY` — not as a post-aggregation filter.

---

INVARIANT INV-27: `closing_balance` in the weekly account summary must use the Silver accounts record with the most recent `_record_valid_from` at or before `week_end_date`. If no such record exists for the account, `closing_balance` must be null — not zero, not a prior-week carry-forward, not an error value. This is never negotiable.
Enforcement: `gold_weekly_account_summary` dbt model selects `current_balance` from Silver accounts with `_record_valid_from <= week_end_date ORDER BY _record_valid_from DESC LIMIT 1`; result is NULL if no rows match.

---

INVARIANT INV-27b: `_record_valid_from` in Silver accounts must be populated with the timestamp at which `pipeline.py` promoted that record to Silver — not the Bronze `_ingested_at` timestamp, not the source file date, and not any wall-clock time captured before or after the Silver write completes. This is never negotiable.
Enforcement: `silver_accounts` dbt model sets `_record_valid_from = CURRENT_TIMESTAMP` at the moment the upsert executes — not `_bronze_ingested_at`, not a date derived from the source filename.

---

INVARIANT INV-28: Gold files must be written atomically. `pipeline.py` must rename the temp Gold file to the canonical path only after dbt exits with status 0. The prior complete Gold file must remain at the canonical path and be readable until the rename succeeds. The rename must be performed by `pipeline.py` after verifying dbt's exit code — not internally by dbt as part of the model execution. This is never negotiable.
Enforcement: `pipeline.py` dbt Gold run writes to a staging path; on dbt exit 0, calls `os.rename(staging_path, canonical_path)`; on non-zero exit, staging file is cleaned up and canonical path is untouched.

---

INVARIANT INV-29: `gold_daily_summary` must contain exactly one row per distinct `transaction_date` in Silver where at least one `_is_resolvable = true` record exists. Dates with zero resolvable transactions must be omitted entirely — no zero-count rows. This is never negotiable.
Enforcement: `gold_daily_summary` dbt model applies `GROUP BY transaction_date` after `WHERE _is_resolvable = true`; no COALESCE to zero, no zero-fill.

---

INVARIANT INV-30: `gold_weekly_account_summary` must contain exactly one row per account per ISO calendar week (Monday–Sunday, per ISO 8601) where the account has at least one resolvable transaction. `week_start_date` must be a Monday and `week_end_date` must be a Sunday, with `week_end_date = week_start_date + 6 days` exactly. This is never negotiable.
Enforcement: `gold_weekly_account_summary` dbt model uses `DATE_TRUNC('week', transaction_date)` in DuckDB (returns ISO Monday); `week_end_date = week_start_date + INTERVAL 6 DAYS`.

---

INVARIANT INV-31: Gold `total_signed_amount` for each day must equal the sum of `_signed_amount` from Silver transactions filtered to that `transaction_date` where `_is_resolvable = true`. No approximation, rounding drift, or double-counting is permitted. This is never negotiable.
Enforcement: `gold_daily_summary` dbt model: `SUM(_signed_amount) WHERE _is_resolvable = true GROUP BY transaction_date` — DECIMAL arithmetic throughout, no intermediate aggregation that could introduce rounding.

---

INVARIANT INV-32: Gold `total_purchases` count for a week and account must equal the count of PURCHASE-type Silver transactions for that account and week where `_is_resolvable = true`. This is never negotiable.
Enforcement: `gold_weekly_account_summary` dbt model joins to `silver_transaction_codes` to resolve `transaction_type = 'PURCHASE'`; counts only records where `_is_resolvable = true` with matching week boundary.

---

INVARIANT INV-32b: `avg_purchase_amount` in `gold_weekly_account_summary` must be null when `total_purchases = 0` for a given account-week. It must not be zero. This is never negotiable.
Enforcement: `gold_weekly_account_summary` dbt model uses `AVG(CASE WHEN transaction_type = 'PURCHASE' THEN _signed_amount END)` — DuckDB AVG over empty set returns NULL; never COALESCE to zero.

---

INVARIANT INV-33: The watermark in `control.parquet` must advance only after Bronze, Silver, and Gold all complete with `status = SUCCESS` for the target date. A failure at any layer must leave the watermark at its prior value. `control.parquet` must be written atomically (temp-rename pattern). This is never negotiable.
Enforcement: `pipeline.py` watermark update is the last action after all three dbt runs exit with status 0; any non-zero dbt exit prevents the watermark update. Watermark write uses temp-rename pattern to `pipeline/.tmp_control.parquet` then `os.rename()` to `pipeline/control.parquet`.

---

INVARIANT INV-33b: `control.parquet` must contain exactly one row at all times. The watermark advance must overwrite the single existing row — it must not append a new row. This is never negotiable.
Enforcement: `pipeline.py` watermark write constructs a new single-row Parquet file and renames it over the existing one — not append write mode. Post-run test: `SELECT COUNT(*) FROM read_parquet('pipeline/control.parquet')` must always return 1.

---

INVARIANT INV-34: The incremental pipeline must determine the next processing date as `watermark + 1 day`. It must not re-process dates at or before the current watermark. This is never negotiable.
Enforcement: `pipeline.py` incremental path: `target_date = last_processed_date + timedelta(days=1)` — read from `control.parquet`; no override mechanism other than `--force-full` with a prior watermark reset.

---

INVARIANT INV-35: The historical pipeline must resume from `max(start_date, watermark + 1 day)` when a watermark already exists. It must not reprocess already-watermarked dates without an explicit `--force-full` flag combined with a prior manual watermark reset. `--force-full` must not reset the watermark itself — it must refuse to proceed if the watermark has not already been manually reset to a date before `start_date`. This is never negotiable.
Enforcement: `pipeline.py` historical path: `effective_start = max(start_date, last_processed_date + timedelta(days=1))` when `control.parquet` exists. `--force-full` handler reads watermark; if `watermark >= start_date`, prints error and exits without proceeding.

---

INVARIANT INV-36: Running the incremental pipeline when no new source file is available must leave all layers and the watermark unchanged. No records may be written, no watermark advanced. This is never negotiable.
Enforcement: `pipeline.py` incremental path checks `os.path.exists(f'source/transactions_{target_date}.csv')` before any Bronze loader invocation; if absent, logs "no file available" and exits without modifying any layer or the watermark.

---

INVARIANT INV-36b: The `--reset-watermark` command must update only `pipeline/control.parquet`. It must not delete, modify, or truncate any Bronze partition, Silver partition, quarantine file, Gold file, or run log entry. This is never negotiable.
Enforcement: `pipeline.py` `--reset-watermark` handler writes only to `pipeline/control.parquet` via temp-rename; no directory traversal, no deletion of any other path. Code review confirms the handler function calls no delete, truncate, or rmdir operations.

---

INVARIANT INV-37: The run log must receive exactly one row per dbt model and per Bronze loader per pipeline invocation, including for steps that are SKIPPED due to the path-existence check. Rows are appended — the file is never overwritten. Prior run rows must never be modified. This is never negotiable.
Enforcement: `pipeline.py` writes a SKIPPED row to the run log for every path-existence check skip, with correct `model_name`, `run_id`, `started_at`, and `completed_at`. Run log writes use append mode — never reconstruct or overwrite the file.

---

INVARIANT INV-37b: `run_log.parquet` must be appended atomically for each row addition. A crash mid-append must not leave the Parquet file with a corrupt footer. This is never negotiable.
Enforcement: `pipeline.py` implements run log appends as read-all / append-row / write-new-file / atomic-rename: reads existing `run_log.parquet` into memory, appends new row, writes to `.tmp_run_log.parquet`, renames to `run_log.parquet`.

---

INVARIANT INV-38: For any `_pipeline_run_id` appearing in a Silver record, a corresponding row must exist in the run log with `status = SUCCESS`. The run log row must be written only after the Silver Parquet file is closed and the write is confirmed complete. This is never negotiable.
Enforcement: `pipeline.py` sequence: (1) dbt Silver model runs and writes Parquet, (2) dbt exits with status 0, (3) `pipeline.py` writes SUCCESS row to run log — never in reverse order. On dbt non-zero exit, writes FAILED row.

---

INVARIANT INV-39: `error_message` in the run log must be null when `status = SUCCESS` or `status = SKIPPED`. It must be non-null when `status = FAILED`, containing the exception type and a sanitised description. Maximum 500 characters, truncated with `[truncated]`. File paths, stack traces, credentials, and internal hostnames must be excluded. This is never negotiable.
Enforcement: `pipeline.py` run log writer: `error_message = None` for SUCCESS/SKIPPED; `error_message = sanitise(exception)` for FAILED. `sanitise()` strips all strings matching `^/`, limits to 500 chars, appends `[truncated]` if truncated.

---

INVARIANT INV-39b: `records_rejected` in the run log must count only records written to quarantine (hard rejections). Records that enter Silver with `_is_resolvable = false` must not be included in `records_rejected`. This is never negotiable.
Enforcement: `pipeline.py` or `silver_transactions` dbt model populates `records_rejected` from `COUNT(*) FROM silver/quarantine/date={date}/rejected.parquet` after promotion — not from a combined count that includes unresolvable records.

---

INVARIANT INV-40: Every Bronze, Silver, and Gold record must have a non-null `_pipeline_run_id`. A record without a run ID cannot be traced to its originating pipeline invocation. This is never negotiable.
Enforcement: Bronze loader asserts `_pipeline_run_id = run_id` (UUID from `pipeline.py`) non-null before write. All Silver and Gold dbt models populate `_pipeline_run_id` from the dbt variable passed by `pipeline.py`. Sign-off verification: `SELECT COUNT(*) FROM read_parquet('...') WHERE _pipeline_run_id IS NULL` must return 0 for Bronze, Silver, and Gold.

---

INVARIANT INV-40b: `pipeline_type` in the run log must be exactly `HISTORICAL` or `INCREMENTAL` (uppercase). `layer` must be exactly `BRONZE`, `SILVER`, or `GOLD` (uppercase). Values outside these enumerations must cause the run log write to fail rather than produce unqueryable entries. This is never negotiable.
Enforcement: `pipeline.py` run log writer assigns `pipeline_type` and `layer` from a Python `Enum` or constants, not free-form strings. Post-run test: `SELECT DISTINCT pipeline_type, layer FROM read_parquet('pipeline/run_log.parquet')` result must be a subset of the defined enumerations.

---

INVARIANT INV-41a: `pipeline.py` must write a PID file containing the OS process ID to `pipeline/pipeline.pid` as the first action at startup, before any pipeline work begins. This is never negotiable.
Enforcement: `pipeline.py` PID file write is literally the first statement in `main()`, before argument parsing, before watermark read, before any filesystem access.

---

INVARIANT INV-41b: `pipeline.py` must remove the PID file on clean exit (process completes normally). This is never negotiable.
Enforcement: `pipeline.py` `finally` block in `main()` calls `os.remove('pipeline/pipeline.pid')` — executed on both normal return and on handled exceptions.

---

INVARIANT INV-41c: `pipeline.py` must remove the PID file on SIGTERM. This is never negotiable.
Enforcement: `pipeline.py` registers `signal.signal(signal.SIGTERM, handler)` at startup where handler removes PID file and calls `sys.exit(0)`.

---

INVARIANT INV-41d: At startup, if a PID file exists and the process with that PID is currently alive, `pipeline.py` must exit immediately with a clear error message. It must not proceed with pipeline work. This is never negotiable.
Enforcement: `pipeline.py` startup reads `pipeline.pid`; calls `os.kill(pid, 0)`; if `os.kill` does not raise `ProcessLookupError`, prints error and calls `sys.exit(1)`.

---

INVARIANT INV-41e: At startup, if a PID file exists but the process with that PID is not alive (stale PID file from a prior SIGKILL or OOM kill), `pipeline.py` must delete the stale PID file and proceed normally. This is never negotiable.
Enforcement: `pipeline.py` startup: after `os.kill(pid, 0)` raises `ProcessLookupError`, calls `os.remove('pipeline/pipeline.pid')` and continues with normal startup — no error, no operator action required.

---

INVARIANT INV-42: Only `pipeline.py` may invoke dbt models as an operational action. Running dbt commands directly (outside `pipeline.py`) bypasses idempotency path-existence checks and will cause FM-4: all transactions for the target date rejected as duplicates with no error signal. This is never negotiable.
Enforcement: Model header comments in all Silver dbt models state that direct dbt invocation bypasses idempotency checks.

---

INVARIANT INV-43a: A single UUID (RFC 4122 format) must be generated exactly once per `pipeline.py` invocation at process startup. The same UUID must be used for all run log rows produced by that invocation. This is never negotiable.
Enforcement: `pipeline.py`: `run_id = str(uuid.uuid4())` at the top of `main()` using Python's `uuid` module. Validated with RFC 4122 regex before first use.

---

INVARIANT INV-43b: The UUID generated at startup must be passed to and used by all Bronze loaders and all dbt invocations within that pipeline run. All `_pipeline_run_id` audit columns in Bronze, Silver, and Gold records from a single invocation must share the same UUID. This is never negotiable.
Enforcement: `pipeline.py` passes `run_id` as a dbt variable (`--vars '{"run_id": "..."}'`) to every `dbt run` invocation, and as a parameter to every Bronze loader function call — no Bronze loader generates its own UUID.

---

INVARIANT INV-44: Bronze audit columns (`_source_file`, `_ingested_at`, `_pipeline_run_id`) must be added by the Bronze loader. They must not be present in source CSVs. They must not be modified during Silver promotion. This is never negotiable.
Enforcement: Bronze loader adds `_source_file`, `_ingested_at`, `_pipeline_run_id` as computed columns after reading the source CSV. `silver_transactions` dbt model SELECTs `_source_file` and `_ingested_at` from Bronze as-is — not recomputed.

---

INVARIANT INV-44b: `_source_file` in Bronze must contain the exact originating CSV filename (e.g., `transactions_2024-01-15.csv`) — not a constant, not a directory path, and not a value re-derived from filesystem context at Silver promotion time. This is never negotiable.
Enforcement: Bronze loader: `_source_file = os.path.basename(source_csv_path)` — filename component only. `silver_transactions` dbt model carries `_source_file` from `bronze._source_file` — not re-read from environment or dbt variables.

---

INVARIANT INV-45: Silver audit columns (`_source_file`, `_bronze_ingested_at`) must carry forward values read from the Bronze Parquet record's audit columns. They must not be re-derived from source files, filesystem paths, or any context outside the Bronze record at promotion time. This is never negotiable.
Enforcement: `silver_transactions` and `silver_accounts` dbt models: `_bronze_ingested_at = bronze._ingested_at` in the SELECT clause — no `CURRENT_TIMESTAMP`, no filename-derived date.

---

INVARIANT INV-46: At any point an analyst queries Gold via DuckDB CLI, the Gold file at the canonical path must be either the prior complete version or the new complete version — never a partial write. This is never negotiable.
Enforcement: Enforced by INV-28 — no additional enforcement point; INV-46 is a derived guarantee from the atomic write implementation.

---

INVARIANT INV-47: Every Gold record must carry a non-null `_pipeline_run_id` and `_computed_at` timestamp. An analyst must be able to trace any Gold aggregate back to its originating pipeline run through these columns joined to the run log. This is never negotiable.
Enforcement: All Gold dbt models include `_pipeline_run_id = '{{ var("run_id") }}'` and `_computed_at = CURRENT_TIMESTAMP` in every Gold model SELECT. Sign-off verification: `SELECT COUNT(*) FROM read_parquet('gold/**/*.parquet') WHERE _pipeline_run_id IS NULL OR _computed_at IS NULL` must return 0.

---

INVARIANT INV-48: Running the full pipeline twice against the same input must produce identical Bronze row counts, Silver row counts, quarantine row counts, and Gold output. Timestamps in audit columns and `_pipeline_run_id` values are excluded from the idempotency comparison. All data values and row counts must be identical. This is never negotiable.
Enforcement: Sign-off verification: run the full pipeline; capture row counts and key data values; run again; diff results excluding audit timestamp and run ID columns. Specifically verify `total_signed_amount` in Gold is identical across two runs.

---

INVARIANT INV-49: `pipeline.py` must execute layers in strict sequence for a given date: Bronze completes before Silver starts, Silver completes before Gold starts. Concurrent execution of Bronze and Silver, or Silver and Gold, for the same date is not permitted. This is never negotiable.
Enforcement: `pipeline.py` layer invocations are sequential Python calls — `run_bronze(date)` returns before `run_silver(date)` is called; no threading, no subprocess concurrency for the same date. Code review confirms no `threading.Thread`, `multiprocessing.Process`, or `asyncio` concurrency for same-date layer execution.

---

INVARIANT INV-49b: The idempotency path-existence check in `pipeline.py` must execute before dbt is invoked for each model, not inside dbt. If the path exists, `pipeline.py` must skip the dbt invocation entirely and write a SKIPPED row to the run log. This is never negotiable.
Enforcement: `pipeline.py`: before every `subprocess.run(['dbt', 'run', '--select', model_name, ...])`, calls `os.path.exists(expected_output_path)`; skips if true. Code review confirms no dbt model uses `unique_key` or `incremental` merge strategies as a substitute for the `pipeline.py` path check.

---

INVARIANT INV-50: `_signed_amount` absolute magnitude must equal the source `amount` value for every Silver transaction record. `ABS(_signed_amount)` must equal `amount` as read from Bronze. This is never negotiable.
Enforcement: `silver_transactions` dbt model uses DuckDB DECIMAL arithmetic throughout sign assignment; no FLOAT or DOUBLE intermediate cast. Sign-off verification: `SELECT COUNT(*) FROM silver/transactions/**/*.parquet WHERE ABS(_signed_amount) != amount` must return 0.

---

INVARIANT INV-51: The `transactions_by_type` STRUCT in `gold_daily_summary` must include an entry for each transaction type that has at least one resolvable transaction on that date. The STRUCT must not include entries for transaction types with zero occurrences on that date. This is never negotiable.
Enforcement: `gold_daily_summary` dbt model builds the STRUCT using a `GROUP BY transaction_type` approach that naturally includes only types with records — does not use a pivot with hard-coded type names that produces null entries for absent types.

---

INVARIANT INV-52: `affects_balance` in Silver transaction codes must be stored as a boolean type, not as the string values `"true"` / `"false"` or integers `0` / `1`. This is never negotiable.
Enforcement: `silver_transaction_codes` dbt model uses `CAST(affects_balance AS BOOLEAN)` in the SELECT — not implicit coercion. Post-promotion test: `SELECT typeof(affects_balance) FROM silver/transaction_codes/data.parquet LIMIT 1` must return `'boolean'`.

---

## SECTION 3 — Scope Boundary

### 3.1 File Manifest — Files CC May Create or Modify

CC is permitted to create or modify the following files during Phase 6 build sessions:

**Pipeline Python files:**
- `pipeline/bronze_utils.py`
- `pipeline/bronze_transactions.py`
- `pipeline/bronze_accounts.py`
- `pipeline/bronze_transaction_codes.py`
- `pipeline/silver_runner.py`
- `pipeline/run_log_writer.py`
- `pipeline/control.py`
- `pipeline/pipeline.py`

**dbt Silver models:**
- `dbt_project/models/silver/silver_transactions.sql`
- `dbt_project/models/silver/silver_accounts.sql`
- `dbt_project/models/silver/silver_transaction_codes.sql`
- `dbt_project/models/silver/silver_quarantine.sql`

**dbt Gold models:**
- `dbt_project/models/gold/gold_daily_summary.sql`
- `dbt_project/models/gold/gold_weekly_account_summary.sql`

**dbt configuration:**
- `dbt_project/dbt_project.yml`
- `dbt_project/profiles.yml`

**Tests:**
- `tests/*.py`

**Runtime-written data files (written at runtime by pipeline code, not created by CC editing files):**
- `data/pipeline/control.parquet`
- `data/pipeline/run_log.parquet`

**Infrastructure:**
- `docker-compose.yml`
- `Dockerfile`
- `requirements.txt`

CC must NOT create, modify, or delete:
- Any file in `source/` (read-only, enforced by Docker bind mount)
- Any Bronze, Silver, Gold, or quarantine Parquet files directly (these are written only by pipeline code, never by CC editing files)
- `ARCHITECTURE.md`, `INVARIANTS.md`, `EXECUTION_PLAN.md`, `Claude.md` (planning artifacts — frozen)

### 3.2 Pipeline Mode Coverage

| Model / Component | Historical Pipeline | Incremental Pipeline |
|---|---|---|
| Bronze loaders (transactions, accounts) | ✓ Called per date | ✓ Called for next date only |
| Bronze transaction_codes loader | ✓ Called once (first run only) | ✗ Never called |
| silver_transaction_codes dbt model | ✓ Run once (first date only) | ✗ Always SKIPPED (path-exists) |
| silver_accounts dbt model | ✓ Run per date | ✓ Run for next date |
| silver_transactions dbt model | ✓ Run per date | ✓ Run for next date |
| gold_daily_summary dbt model | ✓ Run per date | ✓ Run for next date |
| gold_weekly_account_summary dbt model | ✓ Run per date | ✓ Run for next date |
| Watermark advance | ✓ After Gold SUCCESS per date | ✓ After Gold SUCCESS |
| PID file lifecycle | ✓ Write at start / remove at exit | ✓ Write at start / remove at exit |

### 3.3 Silver Model Quality Check Order

**silver_transactions** — checks run in this exact sequence; first failure routes to quarantine, no further checks:
1. NULL_REQUIRED_FIELD — any of {transaction_id, account_id, transaction_date, amount, transaction_code, channel} is null or empty
2. INVALID_AMOUNT — amount is zero, negative, or non-numeric (after DECIMAL cast)
3. DUPLICATE_TRANSACTION_ID — transaction_id already exists in any Silver transactions partition (cross-partition scan)
4. INVALID_TRANSACTION_CODE — transaction_code not found in silver/transaction_codes/data.parquet
5. INVALID_CHANNEL — channel not exactly 'ONLINE' or 'IN_STORE' (case-sensitive)
6. PASS — record enters silver_transactions with _is_resolvable derived from LEFT JOIN to silver/accounts/data.parquet

**silver_accounts** — checks run in this exact sequence:
1. NULL_REQUIRED_FIELD — any of {account_id, open_date, credit_limit, current_balance, billing_cycle_start, billing_cycle_end, account_status} is null or empty
2. INVALID_ACCOUNT_STATUS — account_status not exactly 'ACTIVE', 'SUSPENDED', or 'CLOSED' (case-sensitive)
3. PASS — record upserts into silver/accounts/data.parquet using account_id as merge key, with deterministic tie-break on source file row order (last row wins) for within-batch duplicates

**silver_transaction_codes** — no rejection rules; promote all rows; cast affects_balance to BOOLEAN; validate debit_credit_indicator IN ('DR','CR') post-promotion, raise FAILED if any violation.

Any condition that does not map to a pre-defined rejection reason must raise a pipeline error (FAILED in run log). CC must never invent new rejection reason codes.

### 3.4 Idempotency Mechanism per Layer

| Layer | How CC detects a partition already exists | Action when exists |
|---|---|---|
| Bronze transactions | `os.path.exists('data/bronze/transactions/date={date}/data.parquet')` | Write SKIPPED to run log; do not invoke loader |
| Bronze accounts | `os.path.exists('data/bronze/accounts/date={date}/data.parquet')` | Write SKIPPED to run log; do not invoke loader |
| Bronze transaction_codes | `os.path.exists('data/bronze/transaction_codes/data.parquet')` | Write SKIPPED to run log; do not invoke loader |
| Silver transactions | `os.path.exists('data/silver/transactions/date={date}/data.parquet')` | Write SKIPPED to run log; do not invoke dbt |
| Silver accounts | `os.path.exists('data/silver/accounts/data.parquet')` — checked after accounts partition path for given date | Write SKIPPED to run log; do not invoke dbt |
| Silver transaction_codes | `os.path.exists('data/silver/transaction_codes/data.parquet')` | Write SKIPPED to run log; do not invoke dbt |
| Gold daily_summary | `os.path.exists('data/gold/daily_summary/data.parquet')` | Write SKIPPED to run log; do not invoke dbt |
| Gold weekly_account_summary | `os.path.exists('data/gold/weekly_account_summary/data.parquet')` | Write SKIPPED to run log; do not invoke dbt |

Path-existence check is performed by pipeline.py before every loader or dbt invocation — never inside the dbt model itself.

### 3.5 Watermark Advancement Sequence

The watermark advances once per date processed by the historical pipeline, and once per successful run of the incremental pipeline. The exact sequence within pipeline.py is:

1. Bronze loaders complete with status 0 (all three: transactions, accounts, transaction_codes where applicable)
2. Silver dbt models complete with dbt exit code 0 (silver_transactions, silver_accounts, and silver_transaction_codes where applicable)
3. Gold dbt models complete with dbt exit code 0 (both gold models)
4. **Only after step 3 succeeds:** call write_watermark(date, run_id)

If any step fails (non-zero exit or exception), write FAILED to run log and stop. Watermark must not advance. The exact line that triggers the watermark write in pipeline.py is the call to write_watermark() — it must not be called before that point, and must not be called conditionally based on any output other than dbt exit code 0 from both Gold models.

### 3.6 Conflict Rule

If a task prompt given to CC conflicts with any invariant in Section 2:
- CC must immediately flag the specific invariant ID and the nature of the conflict
- CC must halt and not proceed with the conflicting implementation
- CC must never resolve an invariant conflict silently by choosing an interpretation that satisfies the prompt while technically complying with the invariant text

---

## SECTION 4 — Fixed Stack

### 4.1 Technology Stack

| Component | Technology | Version / Constraint |
|---|---|---|
| Container runtime | Docker Compose | v2 |
| Base image | python:3.11-slim | Exact tag |
| Data transformation (Silver/Gold) | dbt-core + dbt-duckdb | 1.7.* |
| Embedded query engine | DuckDB | Latest compatible with dbt-duckdb 1.7 |
| Parquet I/O (Bronze loaders) | pyarrow + pandas | Latest compatible |
| Pipeline orchestrator | pipeline.py | Python 3.11, stdlib only + above deps |
| Test runner | pytest | Latest compatible |
| UUID generation | Python stdlib uuid | uuid.uuid4() — no alternatives |

No external services. No outbound HTTP. No database server.

### 4.2 Parquet Partition Path Patterns (canonical paths, no trailing slashes)

| Entity | Canonical Path |
|---|---|
| Bronze transactions | data/bronze/transactions/date={YYYY-MM-DD}/data.parquet |
| Bronze accounts | data/bronze/accounts/date={YYYY-MM-DD}/data.parquet |
| Bronze transaction_codes | data/bronze/transaction_codes/data.parquet |
| Silver transactions | data/silver/transactions/date={YYYY-MM-DD}/data.parquet |
| Silver accounts | data/silver/accounts/data.parquet |
| Silver transaction_codes | data/silver/transaction_codes/data.parquet |
| Silver quarantine (transactions) | data/silver/quarantine/date={YYYY-MM-DD}/rejected.parquet |
| Gold daily_summary | data/gold/daily_summary/data.parquet |
| Gold weekly_account_summary | data/gold/weekly_account_summary/data.parquet |
| Watermark control table | data/pipeline/control.parquet |
| Run log | data/pipeline/run_log.parquet |
| PID file | pipeline/pipeline.pid |

Temp file naming convention for atomic writes: .tmp_{filename} in the same directory as the target canonical path (e.g., data/bronze/transactions/date=2024-01-15/.tmp_data.parquet).

### 4.3 Audit Column Names and Types — Every Layer

**Bronze (all three entities):**
| Column | Type | Value |
|---|---|---|
| _source_file | STRING | os.path.basename(source_csv_path) — filename only |
| _ingested_at | TIMESTAMP (UTC) | pandas.Timestamp.utcnow() — captured once per loader call |
| _pipeline_run_id | STRING | UUID passed in from pipeline.py — never generated in loader |

**Silver transactions:**
| Column | Type | Value |
|---|---|---|
| _source_file | STRING | Carried from bronze._source_file — not recomputed |
| _bronze_ingested_at | TIMESTAMP | Carried from bronze._ingested_at — not recomputed |
| _pipeline_run_id | STRING | dbt var("run_id") passed by pipeline.py |
| _promoted_at | TIMESTAMP | CURRENT_TIMESTAMP at Silver promotion |
| _signed_amount | DECIMAL | amount × +1 (DR) or amount × −1 (CR) — no float intermediate |
| _is_resolvable | BOOLEAN | true if account_id found in silver/accounts/data.parquet at promotion time |
| _rejection_reason | STRING | NULL for Silver records; one of the pre-defined codes for quarantine records |

**Silver accounts:**
| Column | Type | Value |
|---|---|---|
| _source_file | STRING | Carried from bronze._source_file |
| _bronze_ingested_at | TIMESTAMP | Carried from bronze._ingested_at |
| _pipeline_run_id | STRING | dbt var("run_id") |
| _record_valid_from | TIMESTAMP | CURRENT_TIMESTAMP at the moment the Silver upsert executes |

**Silver transaction_codes:**
| Column | Type | Value |
|---|---|---|
| _source_file | STRING | Carried from bronze._source_file |
| _bronze_ingested_at | TIMESTAMP | Carried from bronze._ingested_at |
| _pipeline_run_id | STRING | dbt var("run_id") |
| affects_balance | BOOLEAN | CAST(affects_balance AS BOOLEAN) — must be boolean type, not string |

**Silver quarantine:**
| Column | Type | Value |
|---|---|---|
| (all source columns) | As-read from Bronze | Carried from the Bronze record |
| _rejection_reason | STRING | Exactly one of the pre-defined rejection reason codes |
| _rejected_at | TIMESTAMP | CURRENT_TIMESTAMP at rejection |
| _pipeline_run_id | STRING | dbt var("run_id") |

**Gold (both models):**
| Column | Type | Value |
|---|---|---|
| _pipeline_run_id | STRING | dbt var("run_id") |
| _computed_at | TIMESTAMP | CURRENT_TIMESTAMP |

### 4.4 Rejection Reason Codes — Fixed Enum (CC must not invent new codes)

For silver_transactions:
- NULL_REQUIRED_FIELD
- INVALID_AMOUNT
- DUPLICATE_TRANSACTION_ID
- INVALID_TRANSACTION_CODE
- INVALID_CHANNEL

For silver_accounts:
- NULL_REQUIRED_FIELD
- INVALID_ACCOUNT_STATUS

Any condition not matching one of the above must raise a pipeline error (FAILED status in run log). CC must never use a string not in this list as a _rejection_reason value.

### 4.5 Run Log Schema (data/pipeline/run_log.parquet)

Append-only. Each pipeline.py invocation appends rows — never overwrites prior rows. Written atomically per row addition (read-all / append-row / write-to-temp / rename).

| Field | Type | Notes |
|---|---|---|
| run_id | STRING | UUID from pipeline.py startup — same for all rows in one invocation |
| pipeline_type | STRING | Exactly 'HISTORICAL' or 'INCREMENTAL' — enum enforced |
| model_name | STRING | e.g. 'bronze_transactions', 'silver_transactions', 'gold_daily_summary' |
| layer | STRING | Exactly 'BRONZE', 'SILVER', or 'GOLD' — enum enforced |
| started_at | TIMESTAMP (UTC) | Time the step started |
| completed_at | TIMESTAMP (UTC) | Time the step completed |
| status | STRING | Exactly 'SUCCESS', 'FAILED', or 'SKIPPED' |
| records_processed | INTEGER or NULL | Rows read from source for this step |
| records_written | INTEGER or NULL | Rows written to output for this step |
| records_rejected | INTEGER or NULL | Non-null only for SILVER layer steps; count of quarantine rows; must NOT include _is_resolvable=false records |
| error_message | STRING or NULL | NULL when status is SUCCESS or SKIPPED; non-null sanitised string (≤500 chars, no filesystem paths) when status is FAILED |

### 4.6 Control Table Schema (data/pipeline/control.parquet)

Always exactly one row. Written atomically (temp-rename). Never appended.

| Field | Type | Notes |
|---|---|---|
| last_processed_date | DATE | The last date for which all three layers completed with SUCCESS |
| updated_at | TIMESTAMP (UTC) | utcnow() at time of watermark write |
| updated_by_run_id | STRING | run_id of the pipeline invocation that advanced the watermark, or 'manual-reset' for --reset-watermark operations |

Condition that triggers a watermark write: pipeline.py calls write_watermark(date, run_id) only after both Gold dbt models exit with code 0 for that date. This is the only trigger. A failure at any prior layer prevents this call.
