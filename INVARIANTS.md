# INVARIANTS.md
## Credit Card Financial Transactions Lake
**Version:** 2.0
**Sources:** Requirements Brief §1–10 · ARCHITECTURE.md §1–9 · Challenge analysis · Architecture gap analysis

---

## How to Read This Document

Each invariant is stated as a condition that must always be true, followed by:

- **Category** — `data correctness` | `operational` | `security`
- **Why this matters** — the concrete failure scenario if this invariant is violated
- **Enforcement points** — the specific locations in the system where this must be checked or enforced

Retired IDs (where the original set bundled two enforcement points and was split) are listed at the end.

---

## TP-1 · Capture — Source CSV → Bronze Loader

---

### INV-01
Every field present in the source CSV must appear in the Bronze Parquet file with identical value and type. No filtering, transformation, enrichment, or defaulting of source fields may occur in the Bronze loader.

**Category:** data correctness

**Why this matters:** Bronze is the only place the pipeline stores an exact copy of what the source system delivered. If a field is silently coerced, dropped, or defaulted at load time, the error becomes invisible — there is no downstream layer that can recover the original value. An analyst tracing a suspicious Gold aggregate back to Bronze will find data that looks clean but does not match the source file. The audit trail becomes unreliable at its root.

**Enforcement points:**
- Bronze loader (Python): before writing the Parquet file, assert that all source columns are present in the output schema with matching types
- Post-load test: for each Bronze partition, compare column names and sample values against the source CSV

---

### INV-02
Malformed records, null values, and duplicate rows in the source CSV must be written to Bronze unchanged. The Bronze loader must not drop or correct any source record.

**Category:** data correctness

**Why this matters:** Quality filtering belongs in Silver. If the Bronze loader silently drops malformed records, the Silver row count + quarantine row count will not reconcile to the Bronze row count (INV-15), but the discrepancy will be invisible — Bronze itself will look correct. The record is gone before any audit trail captures it. The quarantine layer, which is the system's formal record of data quality failures, will be missing entries.

**Enforcement points:**
- Bronze loader (Python): no filter predicates, no `WHERE` clauses, no conditional skips of any kind — read all rows from the CSV and write all rows to Parquet
- Post-load test: Bronze row count must equal source CSV row count including rows with nulls and duplicate keys

---

### INV-03
Bronze row count for a given entity and date must equal the row count in the corresponding source CSV file, counting all delimited data rows and excluding only the header row. No rows added, no rows dropped.

**Category:** data correctness

**Why this matters:** This is the foundational completeness check. If Bronze has fewer rows than the source, every downstream reconciliation is wrong. If Bronze has more rows, a duplicate has been introduced. Either condition corrupts all downstream counts and aggregations without any observable error signal at the Gold layer.

**Enforcement points:**
- Bronze loader (Python): after write completes, read back the Bronze partition row count and compare to `wc -l source_file - 1`
- Sign-off verification: DuckDB query against Bronze partition vs. `SELECT COUNT(*) FROM read_csv_auto('source/...')`

---

### INV-04
The source CSV files in `source/` must never be modified, moved, or deleted by the pipeline. They are read-only inputs.

**Category:** data correctness

**Why this matters:** Source files are the legal and operational record of what was delivered by the upstream system. Modifying them destroys the ability to re-verify Bronze completeness (INV-03) or re-run the historical pipeline. If `source/` is treated as mutable scratch space and a file is accidentally overwritten, the pipeline has no recovery path — Bronze becomes the only copy of the data, and Bronze was built to be a copy of something that no longer exists.

**Enforcement points:**
- Bronze loader (Python): open source files in read-only mode; use `open(path, 'r')` or equivalent — never `'w'` or `'a'`
- Docker Compose: mount `source/` as a read-only bind mount (`ro` flag)

---

## TP-2 · Storage Write — Bronze Loader Write Path (Temp → Rename)

---

### INV-05a
Bronze Parquet files must be written atomically. The canonical partition path must not be visible to any reader until the write is complete and the rename has succeeded.

**Category:** data correctness

**Why this matters:** The idempotency mechanism (D-2) checks whether the canonical path exists. If a partial write is visible at the canonical path — because the pipeline crashed mid-write — the path-existence check passes on re-run and the step is skipped. Bronze permanently contains fewer records than the source with no error and no detection. The pipeline reports success; the data is wrong forever.

**Enforcement points:**
- Bronze loader (Python): write to `.tmp_{filename}` in the same directory, then `os.rename()` to the canonical path — never write directly to the canonical path

---

### INV-05b
The temp file used for atomic rename of Bronze partitions must reside in the same directory as the target partition path — not in `/tmp` or any other mount point.

**Category:** operational

**Why this matters:** `os.rename()` in Python is only guaranteed atomic when source and destination are on the same filesystem. In Docker bind-mount configurations, `/tmp` and the data directory may be on different underlying mounts. A cross-mount rename becomes a copy-then-delete under the kernel, which is not atomic. A crash between the copy and the delete leaves the canonical path in an undefined state.

**Enforcement points:**
- Bronze loader (Python): construct temp path as `os.path.join(target_dir, f'.tmp_{filename}')` — the directory component must match the target directory
- Code review: confirm no temp path construction uses `/tmp`, `tempfile.mktemp()`, or any path outside the target directory

---

### INV-05c
The temp file for Bronze atomic writes must be named using the pattern `.tmp_{filename}` (dot-prefixed, hidden file) in the same directory as the target. A non-hidden temp file (e.g., `tmp_{filename}`) may be picked up by DuckDB wildcard scans and treated as a data file.

**Category:** data correctness

**Why this matters:** DuckDB queries against Bronze using a glob pattern (`SELECT * FROM 'bronze/transactions/date=2024-01-15/*.parquet'`) will match any `.parquet` file in the directory, including a non-hidden temp file that survived a crash. A non-hidden temp file from a prior failed run would inflate row counts and introduce partial or corrupt records into every downstream read until someone manually cleaned it up.

**Enforcement points:**
- Bronze loader (Python): temp filename must begin with `.` — enforce with a named constant or helper function, not ad-hoc string construction
- Code review: confirm temp filename construction produces a dot-prefixed name

---

### INV-06
Once written, a Bronze partition must never be overwritten or deleted. The path-existence check in `pipeline.py` must prevent any second write to an already-present canonical partition path.

**Category:** data correctness

**Why this matters:** Bronze immutability is the foundation of audit traceability. If a partition can be overwritten, a re-run can silently replace an older Bronze version with a newer one. Any Silver or Gold records built from the original partition now have `_pipeline_run_id` values that point to a partition with different content. The audit trail is broken: the run log says X records were written from file Y, but the file no longer reflects what was there when Silver was promoted.

**Enforcement points:**
- `pipeline.py`: path-existence check before every Bronze loader invocation — if `bronze/{entity}/date={date}/data.parquet` exists, skip and log SKIPPED; never invoke the loader
- Bronze loader (Python): secondary guard — if target canonical path exists at write time, raise an error rather than overwrite

---

### INV-07
Running the Bronze loader twice against the same source file must produce identical Bronze partition content and row count. No duplicate records may be created by re-running.

**Category:** data correctness

**Why this matters:** Idempotency is a system-wide requirement (Brief §8). If re-running Bronze doubles records, every Silver promotion and Gold aggregation that follows is wrong. The Silver row count check (INV-15) will fail because Bronze now has twice the rows Silver expects, but Silver itself may look superficially correct since it deduplicated on `transaction_id`.

**Enforcement points:**
- `pipeline.py`: path-existence check (INV-06) is the primary enforcement — if the partition exists, the loader is never called
- Bronze loader (Python): even if called directly (bypassing `pipeline.py`), must not write if the canonical path exists

---

### INV-08
After a successful pipeline run, each Gold output entity must exist at exactly one canonical path. No versioned copies, dated suffixes, backup files, or alternative paths for the same Gold entity may exist alongside the canonical path.

**Category:** data correctness

**Why this matters:** The system was built to solve the analyst trust problem caused by multiple file versions. If `gold/daily_summary/` contains `data.parquet` alongside `data_prev.parquet` or `data_2024-01-15.parquet`, an analyst running a wildcard query picks up both. There is no way to know which is current. The system recreates the exact problem it was built to eliminate.

**Enforcement points:**
- `pipeline.py`: after Gold atomic rename, delete the temp file if rename failed and it is still present — no cleanup step should leave residual files at Gold paths
- Post-run test: assert that each Gold directory contains exactly one `.parquet` file

---

## TP-3 · Retrieval — Bronze Parquet → Silver dbt Model

---

### INV-09
Silver transaction promotion must only read from the Bronze partition for the target date. It must not read Bronze partitions from other dates or from source CSVs directly.

**Category:** data correctness

**Why this matters:** Reading across date boundaries during Silver promotion would cause transactions from one day to be promoted under another day's Silver partition, producing incorrect aggregations in Gold and breaking the `transaction_date`-based partition key in Silver. Reading from source CSVs directly skips the Bronze layer entirely, eliminating the immutability guarantee and the audit audit columns Bronze adds.

**Enforcement points:**
- `silver_transactions` dbt model: source must be `bronze/transactions/date={{ var("target_date") }}/data.parquet` — no wildcard date glob, no direct CSV reference
- Code review: confirm no dbt model references `source/` paths

---

### INV-10
Silver transaction codes must be loaded from `silver/transaction_codes/data.parquet` — not from Bronze transaction codes and not from the source CSV — at promotion time.

**Category:** data correctness

**Why this matters:** Transaction code validation (INV-12d) and sign assignment (INV-19) both depend on the Silver transaction codes reference. Reading from Bronze rather than Silver means the quality-checked and audit-decorated reference layer is bypassed. Reading from source CSV entirely removes the Silver quality gate for the dimension itself.

**Enforcement points:**
- `silver_transactions` dbt model: join to `silver/transaction_codes/data.parquet` — path must be the Silver canonical path, not the Bronze path

---

### INV-11a
Silver accounts promotion must read from the Bronze accounts partition for the target date.

**Category:** data correctness

**Why this matters:** If Silver accounts promotion reads from any source other than the Bronze partition for its target date, the Bronze immutability guarantee is meaningless for accounts. The audit columns added in Bronze (`_source_file`, `_ingested_at`, `_pipeline_run_id`) must be carried forward from the Bronze record — they cannot be synthesised from other sources.

**Enforcement points:**
- `silver_accounts` dbt model: source must be `bronze/accounts/date={{ var("target_date") }}/data.parquet`

---

### INV-11b
Transaction account resolution at Silver promotion time must read from `silver/accounts/data.parquet` as it exists at the moment of promotion — not from Bronze accounts and not from the source CSV.

**Category:** data correctness

**Why this matters:** `_is_resolvable` is determined by whether `account_id` exists in Silver Accounts at promotion time. If the resolution check reads from Bronze accounts instead of Silver, it bypasses the account quality gate — a Bronze account record that failed `INVALID_ACCOUNT_STATUS` and was quarantined could still make a transaction appear resolvable. Gold would then include aggregations for an account that failed quality checks.

**Enforcement points:**
- `silver_transactions` dbt model: the LEFT JOIN for account resolution must reference `silver/accounts/data.parquet`, not any Bronze path

---

## TP-4 · Transformation — Silver Promotion Quality Checks

---

### INV-12a
A Bronze transaction record with a null or empty value in any of the fields `transaction_id`, `account_id`, `transaction_date`, `amount`, `transaction_code`, or `channel` must be routed to quarantine with rejection reason `NULL_REQUIRED_FIELD`. It must not enter Silver.

**Category:** data correctness

**Why this matters:** A transaction with a null `transaction_id` cannot be deduplicated. A null `account_id` cannot be resolved. A null `amount` cannot have sign applied. Any of these records entering Silver produces downstream failures that surface far from the root cause — a null `_signed_amount` in Gold, a missing aggregation, or a deduplication failure — with no trace back to the original quality issue.

**Enforcement points:**
- `silver_transactions` dbt model: pre-promotion filter checking all six required fields; records failing routed to `silver_quarantine` with `_rejection_reason = 'NULL_REQUIRED_FIELD'`
- Test: seed a Bronze partition with a record having a null in each required field; verify each produces a quarantine record with the correct code and no Silver record

---

### INV-12b
A Bronze transaction record where `amount` is zero, negative, or non-numeric must be routed to quarantine with rejection reason `INVALID_AMOUNT`. It must not enter Silver.

**Category:** data correctness

**Why this matters:** All source amounts are defined as always positive (Brief §2.1). A zero or negative amount reaching Silver would produce a `_signed_amount` that violates the sign convention — a DR transaction would appear as a credit or zero. Gold aggregations would be wrong in a way that is undetectable without re-examining every Silver record's source `amount`.

**Enforcement points:**
- `silver_transactions` dbt model: `CAST(amount AS DECIMAL)` with error handling; check `amount > 0` after cast; route failures to quarantine

---

### INV-12c
A Bronze transaction record where `transaction_id` already exists in any Silver transactions partition must be routed to quarantine with rejection reason `DUPLICATE_TRANSACTION_ID`. It must not enter Silver.

**Category:** data correctness

**Why this matters:** Allowing a duplicate `transaction_id` into Silver means the same real-world transaction is counted twice in Gold. `total_transactions`, `total_signed_amount`, and all type-level aggregations are inflated by exactly the duplicate amount, with no error signal. The Gold verification queries (Brief §10.3) would pass on a first run but fail after any re-run that introduces the duplicate — making the failure intermittent and hard to diagnose.

**Enforcement points:**
- `silver_transactions` dbt model: before insert, check `transaction_id` against all existing Silver partitions using a cross-partition scan or a maintained deduplcation index
- Test: seed a `transaction_id` into an earlier Silver partition, then re-attempt to promote it in a later Bronze batch; verify it routes to quarantine

---

### INV-12d
A Bronze transaction record where `transaction_code` is not present in `silver/transaction_codes/data.parquet` must be routed to quarantine with rejection reason `INVALID_TRANSACTION_CODE`. It must not enter Silver.

**Category:** data correctness

**Why this matters:** Sign assignment for `_signed_amount` requires a join to Silver transaction codes. A record with an unknown code that reaches Silver cannot have sign assigned, producing a null `_signed_amount` (violating INV-20). If the code is allowed through and sign assignment silently fails, the record reaches Gold with a null aggregation contribution that is invisible unless every single Gold value is independently verified.

**Enforcement points:**
- `silver_transactions` dbt model: LEFT JOIN to `silver/transaction_codes/data.parquet` on `transaction_code`; records with no match route to quarantine before sign assignment runs

---

### INV-12e
A Bronze transaction record where `channel` is not exactly `ONLINE` or `IN_STORE` must be routed to quarantine with rejection reason `INVALID_CHANNEL`. It must not enter Silver.

**Category:** data correctness

**Why this matters:** Gold splits transactions into `online_transactions` and `instore_transactions` counts. A record with an invalid channel value that reaches Silver either falls into neither bucket (producing a silent undercount in both Gold columns) or triggers a runtime error in the Gold model. Either failure is silent at the Silver promotion step.

**Enforcement points:**
- `silver_transactions` dbt model: `CASE WHEN channel IN ('ONLINE', 'IN_STORE') THEN ... ELSE quarantine` — exact string match, case-sensitive

---

### INV-13
A transaction with an unresolvable `account_id` must enter Silver with `_is_resolvable = false`. It must not be quarantined and must not be silently dropped. It must be excluded from all Gold aggregations.

**Category:** data correctness

**Why this matters:** An unresolvable account may be a timing issue — the account delta file may not have arrived yet. Quarantining the transaction permanently destroys it; no backfill pipeline can recover a quarantined transaction without re-reading Bronze. Silently dropping it loses the record entirely. Only flagging preserves both the transaction and the audit trail, while keeping Gold correct by excluding it from aggregations until it can be resolved.

**Enforcement points:**
- `silver_transactions` dbt model: LEFT JOIN to `silver/accounts/data.parquet`; set `_is_resolvable = (account_id IS NOT NULL in accounts join result)` — do not route to quarantine
- `gold_daily_summary` and `gold_weekly_account_summary` dbt models: `WHERE _is_resolvable = true` filter applied before all aggregations

---

### INV-14
`transaction_id` must be unique across all Silver transactions partitions, not just within a single date partition. A `transaction_id` already present in any prior Silver partition must be rejected as `DUPLICATE_TRANSACTION_ID` at the time of promotion — not merely absent from the final output by coincidence.

**Category:** data correctness

**Why this matters:** The source system is append-only, but source files may contain retry submissions that duplicate a `transaction_id` across calendar days. If the uniqueness check is only within the current partition, a cross-date duplicate enters Silver undetected. Both records contribute to Gold. An analyst querying by `transaction_id` finds two records. The sign-off query "no `transaction_id` appears more than once across all Silver partitions" fails in production after passing in development.

**Enforcement points:**
- `silver_transactions` dbt model: uniqueness check must scan all existing Silver partitions, not only `date={{ var("target_date") }}`; a dedicated deduplication query or a pre-built set of known `transaction_id`s must be maintained and consulted at promotion time
- Sign-off verification: `SELECT transaction_id, COUNT(*) FROM read_parquet('silver/transactions/**/*.parquet') GROUP BY 1 HAVING COUNT(*) > 1`

---

### INV-15
The total of (Silver transaction rows + quarantine rows) for a given Bronze source date must equal the total Bronze transaction rows for that date. No record may be silently lost at the Bronze → Silver boundary.

**Category:** data correctness

**Why this matters:** This is the conservation invariant for the Bronze → Silver promotion. If a record is neither in Silver nor in quarantine, it has been silently discarded. There is no audit trail for it. Gold aggregations are understated, quarantine counts do not reflect the real quality failure rate, and the run log `records_rejected` count is wrong. The system's core promise — that every source record is either promoted or formally rejected — is broken.

**Enforcement points:**
- `silver_transactions` dbt model: every branch in the promotion logic must either write to Silver or write to quarantine — no code path may exit without doing one of the two
- Sign-off verification: `SELECT COUNT(*) FROM read_parquet('bronze/transactions/date=.../data.parquet')` must equal `SELECT COUNT(*) FROM read_parquet('silver/transactions/date=.../data.parquet') + COUNT(*) FROM read_parquet('silver/quarantine/date=.../rejected.parquet')`

---

### INV-16a
A Bronze account record with a null or empty value in any of the fields `account_id`, `open_date`, `credit_limit`, `current_balance`, `billing_cycle_start`, `billing_cycle_end`, or `account_status` must be routed to quarantine with rejection reason `NULL_REQUIRED_FIELD`.

**Category:** data correctness

**Why this matters:** An account record with a null `account_id` cannot be upserted into Silver (there is no key to upsert on). A null `current_balance` would propagate to `closing_balance` in Gold, producing a null that appears structurally identical to "no account record exists" (INV-27) — masking a data quality failure as a legitimate data absence.

**Enforcement points:**
- `silver_accounts` dbt model: null/empty check on all seven required fields before upsert; failures to quarantine with `_rejection_reason = 'NULL_REQUIRED_FIELD'`

---

### INV-16b
A Bronze account record where `account_status` is not exactly `ACTIVE`, `SUSPENDED`, or `CLOSED` must be routed to quarantine with rejection reason `INVALID_ACCOUNT_STATUS`.

**Category:** data correctness

**Why this matters:** Downstream queries filtering on `account_status` (e.g., risk team queries filtering to `ACTIVE` accounts) rely on a closed enumeration. A record with a status value like `"active"` (lowercase), `"DELINQUENT"`, or `""` that reaches Silver would match no filter and be silently excluded from every status-based query, without any indication that the record exists.

**Enforcement points:**
- `silver_accounts` dbt model: `CASE WHEN account_status IN ('ACTIVE', 'SUSPENDED', 'CLOSED') THEN ... ELSE quarantine` — exact string match, case-sensitive

---

### INV-17
Silver accounts must retain only the latest record per `account_id`. When a delta record for an existing `account_id` arrives, the prior record must be replaced in full. No history is retained.

**Category:** data correctness

**Why this matters:** Silver accounts is the account resolution reference used by transaction promotion. If multiple records for the same `account_id` accumulate, the account resolution join becomes non-deterministic — which record's `current_balance` feeds `closing_balance`? The explicit design decision (Architecture D-10) accepts the simplification of latest-only in exchange for a deterministic, queryable current state.

**Enforcement points:**
- `silver_accounts` dbt model: upsert strategy with `account_id` as the merge key; DELETE existing row then INSERT new row, or use DuckDB `INSERT OR REPLACE`
- Post-run test: `SELECT account_id, COUNT(*) FROM silver/accounts/data.parquet GROUP BY 1 HAVING COUNT(*) > 1` must return zero rows

---

### INV-18
Rejection reason codes written to quarantine must come exclusively from the pre-defined list: `NULL_REQUIRED_FIELD`, `INVALID_AMOUNT`, `DUPLICATE_TRANSACTION_ID`, `INVALID_TRANSACTION_CODE`, `INVALID_CHANNEL` (transactions); `NULL_REQUIRED_FIELD`, `INVALID_ACCOUNT_STATUS` (accounts). No ad-hoc or unlisted codes are permitted.

**Category:** data correctness

**Why this matters:** The rejection reason code is the primary signal for data quality monitoring. If implementations invent codes, any query filtering `_rejection_reason` against the defined list silently misses the ad-hoc records. The sign-off query "every quarantine record has a non-null `_rejection_reason` from the pre-defined code list" passes structurally but the counts are wrong.

**Enforcement points:**
- `silver_transactions` and `silver_accounts` dbt models: rejection reason must be assigned as a literal from a defined constant set — never as a free-form string variable
- Post-run test: `SELECT DISTINCT _rejection_reason FROM read_parquet('silver/quarantine/**/*.parquet')` result must be a subset of the defined list

---

### INV-18b
If the pipeline encounters a condition during Silver promotion that does not map to any pre-defined rejection reason code, it must raise a pipeline error (FAILED status in run log) rather than invent a new code or silently promote the record.

**Category:** operational

**Why this matters:** An unanticipated condition that silently promotes a record bypasses the quality gate entirely. An unanticipated condition that invents a new code breaks INV-18. The only safe behaviour is to halt with a FAILED status so the operator knows the promotion logic has reached a case it was not designed to handle, and can decide how to classify it.

**Enforcement points:**
- `silver_transactions` and `silver_accounts` dbt models: all conditional branches must have an explicit `ELSE` that raises an error — no fall-through to promotion for unclassified conditions
- `pipeline.py`: non-zero dbt exit code must be captured and recorded as FAILED in the run log; watermark must not advance

---

## TP-16 · Transformation — `_signed_amount` Derivation

---

### INV-19
`_signed_amount` must be computed by joining to `silver_transaction_codes.debit_credit_indicator`. `DR` → positive (source amount × +1). `CR` → negative (source amount × −1). The result must equal the source `amount` in absolute magnitude — no scaling, rounding, or truncation is permitted during sign application.

**Category:** data correctness

**Why this matters:** `_signed_amount` is the authoritative financial value for every transaction. An incorrect sign makes a charge appear as a credit. A rounding error during sign application (e.g., multiplying a DECIMAL by `-1.0` in a context that causes precision loss) produces Gold aggregations that are wrong by a small but non-zero amount — a silent financial data error in a system whose purpose is to produce trustworthy financial numbers.

**Enforcement points:**
- `silver_transactions` dbt model: `_signed_amount = CASE WHEN debit_credit_indicator = 'DR' THEN amount ELSE -amount END` — use the exact source `amount` value, not a cast or rounded intermediate
- Test: verify `ABS(_signed_amount) = amount` for every promoted Silver record

---

### INV-20
No Silver transactions record may have a null `_signed_amount`. A record that cannot be joined to a valid transaction code must be rejected as `INVALID_TRANSACTION_CODE` before Silver promotion, not promoted with a null sign.

**Category:** data correctness

**Why this matters:** A null `_signed_amount` in Silver means `SUM(_signed_amount)` in Gold silently excludes that record's financial contribution (DuckDB treats NULL as excluded from aggregate functions). The Gold `total_signed_amount` is understated with no error signal. The sign-off query "no Silver transactions record has a null `_signed_amount`" exists precisely to catch this.

**Enforcement points:**
- `silver_transactions` dbt model: `INVALID_TRANSACTION_CODE` check (INV-12d) must execute before sign assignment; sign assignment must never run on a record with an unresolved transaction code
- Sign-off verification: `SELECT COUNT(*) FROM read_parquet('silver/transactions/**/*.parquet') WHERE _signed_amount IS NULL` must return 0

---

### INV-21
Source `amount` values in Bronze are always positive. `_signed_amount` in Silver acquires its sign purely from `debit_credit_indicator`. The source `amount` field must not be modified or sign-adjusted before this join.

**Category:** data correctness

**Why this matters:** If the Bronze loader or an early Silver transformation pre-applies a sign to `amount` based on its own heuristics (e.g., treating PAYMENT transactions as negative before the join), then the `debit_credit_indicator` join double-applies the sign. A payment of £100 becomes `_signed_amount = +100` instead of `-100`. The transaction codes dimension is the sole authoritative source for sign assignment (Brief §2.1 note).

**Enforcement points:**
- Bronze loader (Python): write `amount` exactly as read from the CSV — no sign manipulation
- `silver_transactions` dbt model: use `amount` column from Bronze directly as the magnitude input; no `ABS()`, no negation, no conditional sign logic before the `debit_credit_indicator` join

---

### INV-21b
`debit_credit_indicator` values must match exactly as `DR` or `CR` (case-sensitive, no variants). A record in Silver transaction codes with a value that is not exactly `DR` or `CR` must raise a pipeline error at Silver transaction codes promotion — not proceed with undefined sign assignment behaviour downstream.

**Category:** data correctness

**Why this matters:** Sign assignment is a CASE statement on exact string equality. A value of `'dr'`, `'Debit'`, or `'D'` matches neither branch, and the CASE either returns NULL (producing INV-20 violations for every transaction of that type) or falls through to an ELSE that assigns a wrong sign. The error first becomes visible at Gold as incorrect aggregations — far removed from the transaction codes file that caused it, and affecting every transaction of the relevant type.

**Enforcement points:**
- `silver_transaction_codes` dbt model: after promotion, assert `SELECT COUNT(*) FROM silver/transaction_codes/data.parquet WHERE debit_credit_indicator NOT IN ('DR', 'CR')` = 0; raise FAILED if non-zero
- Bronze transaction codes loader: optionally validate at load time as an early-warning check, though the Silver model test is the authoritative gate

---

## TP-5 / TP-6 / TP-7 · Storage Write — Silver Layer Outputs

---

### INV-22
Re-running Silver promotion for a date already present in Silver must produce no new records in `silver_transactions`. This idempotency must be enforced by the path-existence check in `pipeline.py` before dbt is invoked — not by relying on dbt internal deduplication. The step must be recorded as SKIPPED in the run log.

**Category:** data correctness

**Why this matters:** If `pipeline.py` invokes dbt for a date already in Silver, every transaction for that date is rejected as `DUPLICATE_TRANSACTION_ID` (INV-12c) and routed to quarantine. Silver for that date now has zero records. Gold for that date shows zero or disappears. The run log records SUCCESS. This is FM-4 — silent total data loss with no error signal. The path-existence check in `pipeline.py` must be the sole prevention mechanism.

**Enforcement points:**
- `pipeline.py`: before every `dbt run --select silver_transactions`, check `os.path.exists('silver/transactions/date={target_date}/data.parquet')`; if true, write SKIPPED to run log and return without invoking dbt
- Test: run Silver promotion for a date; run it again; verify quarantine count does not increase and Silver row count does not change

---

### INV-23
The quarantine file for a given date is append-only. Records already written to quarantine must never be overwritten or deleted. Re-running promotion for an already-promoted date must not re-quarantine already-quarantined records.

**Category:** data correctness

**Why this matters:** Quarantine is the system's permanent audit record of data quality failures. If quarantine records can be deleted or overwritten, the audit trail is falsifiable — a bad batch can be made to disappear. The row count reconciliation (INV-15) also depends on quarantine being stable: re-quarantining records from a prior run would inflate the quarantine count, making the Bronze = Silver + quarantine equation fail for dates that were processed correctly.

**Enforcement points:**
- `pipeline.py`: the path-existence check for Silver (INV-22) also prevents re-running quarantine writes, since quarantine is written during the same dbt model execution
- `silver_quarantine` dbt model: materialisation strategy must be `incremental` with append-only semantics — never `table` (which would overwrite)

---

### INV-24
Re-running accounts promotion for a date already processed must produce output identical to the first run. The upsert must be deterministic given the same Bronze input. When a single day's Bronze accounts partition contains more than one record for the same `account_id`, the upsert must apply a defined, deterministic tie-breaking rule (e.g., last record by source file row order) so the output is reproducible across re-runs.

**Category:** data correctness

**Why this matters:** If the accounts upsert is non-deterministic — for example, because DuckDB processes rows in an undefined order when multiple records share an `account_id` — then two runs of the same Bronze input produce different Silver accounts states. The `closing_balance` lookup in Gold (INV-27) will return different values on different runs, violating the idempotency sign-off condition (INV-48).

**Enforcement points:**
- `silver_accounts` dbt model: when multiple Bronze records for the same `account_id` exist in a single partition, apply `ORDER BY source_file_row_number DESC LIMIT 1` or equivalent deterministic selection before upsert
- Test: seed a Bronze accounts partition with two records for the same `account_id`; run promotion twice; verify identical Silver accounts output both times

---

### INV-24b
Silver transaction codes must not be re-promoted during incremental pipeline runs. The incremental pipeline must skip the Silver transaction codes promotion step entirely — not merely produce identical output on re-promotion.

**Category:** operational

**Why this matters:** Re-promoting transaction codes during incremental runs would update `_pipeline_run_id` on the reference records, creating a false audit signal that the dimension was touched by the incremental run. An analyst tracing a `_pipeline_run_id` from a transaction codes record would find an incremental run entry in the run log, not the historical init entry that actually loaded the data. The provenance record for the dimension is corrupted.

**Enforcement points:**
- `pipeline.py`: incremental pipeline invocation must not include `silver_transaction_codes` in the dbt model selection list
- `pipeline.py`: the path-existence check for `silver/transaction_codes/data.parquet` must cause a SKIPPED log entry for incremental runs, confirming the step was intentionally skipped

---

### INV-24c
`silver/accounts/data.parquet` must be stored as a single non-partitioned file. The Silver accounts model must never write to a date-partitioned, status-partitioned, or otherwise subdivided path structure.

**Category:** data correctness

**Why this matters:** The transaction account resolution join (INV-11b) reads from `silver/accounts/data.parquet` as a single file. If Silver accounts is stored in a partitioned directory, the join reads from a directory path that must be a glob, which changes the read semantics. More critically, the upsert logic (INV-17) depends on a single authoritative file — partitioned storage makes "latest record per `account_id`" non-trivial to enforce and potentially non-deterministic.

**Enforcement points:**
- `silver_accounts` dbt model: output path must be the literal `silver/accounts/data.parquet` — no partition key in the dbt model configuration
- Post-run test: `ls silver/accounts/` must show exactly one file named `data.parquet` and no subdirectories

---

### INV-24d
`bronze/transaction_codes/data.parquet` must be stored at the non-date-partitioned path `bronze/transaction_codes/data.parquet`. The Bronze transaction codes loader must not write to a date-partitioned path.

**Category:** data correctness

**Why this matters:** The Silver transaction codes promotion reads from the fixed path `bronze/transaction_codes/data.parquet`. If the Bronze loader writes to `bronze/transaction_codes/date=2024-01-01/data.parquet` instead, the Silver promotion step reads an empty path, produces an empty Silver transaction codes reference, and every transaction in every Silver batch fails `INVALID_TRANSACTION_CODE` — sending all transactions to quarantine with no obvious cause.

**Enforcement points:**
- Bronze transaction codes loader (Python): output path must be the literal constant `bronze/transaction_codes/data.parquet` — not constructed from a date variable
- Code review: confirm no date variable appears in the transaction codes output path construction

---

## TP-8 · Retrieval — Silver → Gold Aggregation

---

### INV-25
Gold models must read exclusively from Silver layer tables. No Gold model may read directly from Bronze or from source CSVs.

**Category:** data correctness

**Why this matters:** Gold reading from Bronze would bypass the quality gate entirely — malformed records, unresolvable accounts, and duplicate transactions would all contribute to Gold aggregations. Gold reading from source CSVs bypasses both Bronze immutability and Silver quality checks. Either shortcut destroys the audit chain: a Gold aggregate could not be traced through Silver back to Bronze.

**Enforcement points:**
- All Gold dbt models: source references must only resolve to `silver/` paths — confirmed by code review and dbt lineage graph
- `dbt_project.yml`: consider using dbt sources to formally declare Silver as the only permitted upstream for Gold models

---

### INV-26
Gold aggregations must filter to `_is_resolvable = true` before aggregating. Records with `_is_resolvable = false` must be excluded from all Gold counts and sums.

**Category:** data correctness

**Why this matters:** Unresolvable transactions have no verified account association. Including them in Gold would aggregate financial values for accounts that may not exist or may have arrived with incorrect data. The Gold totals would be wrong in a way that is invisible to analysts — the numbers look valid but include contributions from transactions whose account context is unverified.

**Enforcement points:**
- `gold_daily_summary` and `gold_weekly_account_summary` dbt models: `WHERE _is_resolvable = true` must appear in the base CTE before any `GROUP BY` — not as a post-aggregation filter
- Sign-off verification: recompute Gold totals manually from Silver with the filter applied and verify they match

---

## TP-18 · Transformation — `closing_balance` Derivation

---

### INV-27
`closing_balance` in the weekly account summary must use the Silver accounts record with the most recent `_record_valid_from` at or before `week_end_date`. If no such record exists for the account, `closing_balance` must be null — not zero, not a prior-week carry-forward, not an error value.

**Category:** data correctness

**Why this matters:** A zero `closing_balance` is a valid and meaningful financial value — it indicates an account with no outstanding balance. Returning zero when no record exists conflates "zero balance" with "no data", misleading analysts into believing an account had a known zero balance on a date when the system had no record of it. Null is the correct and unambiguous signal for data absence.

**Enforcement points:**
- `gold_weekly_account_summary` dbt model: `SELECT current_balance FROM silver/accounts/data.parquet WHERE account_id = ... AND _record_valid_from <= week_end_date ORDER BY _record_valid_from DESC LIMIT 1` — result is NULL if no rows match
- Test: verify an account with no Silver accounts record produces a null `closing_balance`, not zero

---

### INV-27b
`_record_valid_from` in Silver accounts must be populated with the timestamp at which `pipeline.py` promoted that record to Silver — not the Bronze `_ingested_at` timestamp, not the source file date, and not any wall-clock time captured before or after the Silver write completes.

**Category:** data correctness

**Why this matters:** The `closing_balance` lookup (INV-27) uses `_record_valid_from` to determine which account record was current at `week_end_date`. If `_record_valid_from` is set to the Bronze `_ingested_at` timestamp (which records when the source file was loaded, not when Silver was updated), the lookup is anchored to Bronze ingestion history rather than Silver promotion history. For systems where Bronze ingestion and Silver promotion run at different times or are replayed, the `closing_balance` values would be wrong.

**Enforcement points:**
- `silver_accounts` dbt model: `_record_valid_from = CURRENT_TIMESTAMP` at the moment the upsert executes — not `_bronze_ingested_at`, not a date derived from the source filename

---

## TP-9 / TP-10 · Storage Write — Gold Layer Outputs

---

### INV-28
Gold files must be written atomically. `pipeline.py` must rename the temp Gold file to the canonical path only after dbt exits with status 0. The prior complete Gold file must remain at the canonical path and be readable until the rename succeeds. The rename must be performed by `pipeline.py` after verifying dbt's exit code — not internally by dbt as part of the model execution.

**Category:** data correctness

**Why this matters:** Gold is the analyst-facing layer. If Gold is overwritten in place and the pipeline crashes mid-write, an analyst reading Gold at that moment receives a partially written file with no error — the worst possible failure mode because it is silent and the reader has no signal. The prior complete file must remain accessible until the replacement is ready. If dbt performs its own internal rename, a dbt crash mid-model could leave the canonical path in a partial state.

**Enforcement points:**
- `pipeline.py`: dbt Gold run must write to a staging path; `pipeline.py` checks dbt exit code; on exit 0, calls `os.rename(staging_path, canonical_path)`; on non-zero exit, staging file is cleaned up and canonical path is untouched
- Test: simulate a dbt Gold model failure (non-zero exit); verify the prior canonical Gold file is unchanged

---

### INV-29
`gold_daily_summary` must contain exactly one row per distinct `transaction_date` in Silver where at least one `_is_resolvable = true` record exists. Dates with zero resolvable transactions must be omitted entirely — no zero-count rows.

**Category:** data correctness

**Why this matters:** A zero-count row for a date looks like a valid data point — a slow day. A missing row looks like a data gap. They communicate different things to an analyst. If the system produces a zero-count row for a date where all transactions were unresolvable (a data quality failure), the analyst sees a quiet day rather than a pipeline problem. The omission is the correct signal.

**Enforcement points:**
- `gold_daily_summary` dbt model: `GROUP BY transaction_date` after `WHERE _is_resolvable = true` — the WHERE clause naturally omits dates with no qualifying records; no COALESCE to zero, no zero-fill
- Sign-off verification: `SELECT COUNT(DISTINCT transaction_date) FROM read_parquet('silver/transactions/**/*.parquet') WHERE _is_resolvable = true` must equal `SELECT COUNT(*) FROM read_parquet('gold/daily_summary/data.parquet')`

---

### INV-30
`gold_weekly_account_summary` must contain exactly one row per account per ISO calendar week (Monday–Sunday, per ISO 8601) where the account has at least one resolvable transaction. Weeks with zero resolvable transactions for an account are omitted. `week_start_date` must be a Monday and `week_end_date` must be a Sunday, with `week_end_date = week_start_date + 6 days` exactly.

**Category:** data correctness

**Why this matters:** If week boundaries are computed using a Sunday-start convention rather than ISO 8601 Monday-start, every weekly aggregate is assigned to the wrong week. Analysts comparing Gold weekly data to external weekly reports (which use ISO weeks) will see systematic off-by-one errors in every row. The error is consistent, making it easy to miss in spot-checks.

**Enforcement points:**
- `gold_weekly_account_summary` dbt model: use `DATE_TRUNC('week', transaction_date)` in DuckDB, which returns ISO Monday; `week_end_date = week_start_date + INTERVAL 6 DAYS`
- Test: verify `week_start_date` is always a Monday (DayOfWeek = 1 in ISO) and `week_end_date - week_start_date = 6 days` for every row

---

### INV-31
Gold `total_signed_amount` for each day must equal the sum of `_signed_amount` from Silver transactions filtered to that `transaction_date` where `_is_resolvable = true`. No approximation, rounding drift, or double-counting is permitted.

**Category:** data correctness

**Why this matters:** `total_signed_amount` is the primary financial aggregate in Gold. If it drifts from the Silver source due to floating-point arithmetic, double-counting from a misconfigured GROUP BY, or silent exclusion of records from a secondary filter, the Gold output cannot be trusted for financial reconciliation. The verification query (Brief §10.3) exists precisely to catch this.

**Enforcement points:**
- `gold_daily_summary` dbt model: `SUM(_signed_amount) WHERE _is_resolvable = true GROUP BY transaction_date` — no intermediate aggregation that could introduce rounding; use DECIMAL arithmetic throughout
- Sign-off verification: recompute directly from Silver and compare to Gold row by row

---

### INV-32
Gold `total_purchases` count for a week and account must equal the count of PURCHASE-type Silver transactions for that account and week where `_is_resolvable = true`.

**Category:** data correctness

**Why this matters:** `total_purchases` is used by risk and product teams to assess account spending behaviour. An incorrect count — caused by joining the wrong transaction type filter, double-counting, or off-by-one week boundary — produces wrong purchase frequency metrics. The sign-off query verifies this directly.

**Enforcement points:**
- `gold_weekly_account_summary` dbt model: join to `silver_transaction_codes` to resolve `transaction_type = 'PURCHASE'`; count only records where `_is_resolvable = true` and the week boundary matches
- Sign-off verification: cross-check `total_purchases` against a direct Silver count for at least one account-week

---

### INV-32b
`avg_purchase_amount` in `gold_weekly_account_summary` must be null when `total_purchases = 0` for a given account-week. It must not be zero.

**Category:** data correctness

**Why this matters:** Zero is a meaningful value for `avg_purchase_amount` — it would imply the account made purchases but the average value was zero. Null correctly communicates "no purchases were made in this week, so there is no average to report." An analyst computing average purchase value across accounts who encounters a zero rather than null will include that account in a denominator, computing a misleadingly lower average.

**Enforcement points:**
- `gold_weekly_account_summary` dbt model: use `AVG(CASE WHEN transaction_type = 'PURCHASE' THEN _signed_amount END)` — DuckDB AVG over an empty set returns NULL, not zero; never COALESCE to zero

---

## TP-11 / TP-12 · Storage — Watermark (`control.parquet`)

---

### INV-33
The watermark in `control.parquet` must advance only after Bronze, Silver, and Gold all complete with `status = SUCCESS` for the target date. A failure at any layer must leave the watermark at its prior value. `control.parquet` must be written atomically (temp-rename pattern) so a crash during the watermark advance cannot leave the file in a partial or corrupt state.

**Category:** operational

**Why this matters:** A prematurely advanced watermark is unrecoverable without manual intervention. If the watermark advances after Bronze and Silver succeed but before Gold completes, the next incremental run skips Bronze and Silver (paths exist) and attempts Gold for the next date, leaving the current date without a Gold output permanently. If the watermark file itself is corrupt (partial write), the incremental pipeline cannot determine where to resume.

**Enforcement points:**
- `pipeline.py`: watermark update must be the last action after all three dbt runs exit with status 0; if any dbt run returns non-zero, watermark update must not execute
- `pipeline.py`: watermark write must use the temp-rename pattern — write to `pipeline/.tmp_control.parquet`, then `os.rename()` to `pipeline/control.parquet`

---

### INV-33b
`control.parquet` must contain exactly one row at all times. The watermark advance must overwrite the single existing row — it must not append a new row. A reader determining the current watermark must be able to read the single row without any `MAX()` or ordering logic.

**Category:** operational

**Why this matters:** If the watermark advance appends rather than overwrites, `control.parquet` grows by one row per pipeline run. After N runs it has N rows. A reader using `SELECT last_processed_date FROM control.parquet` without a `LIMIT 1 ORDER BY` reads all N rows and returns N dates — or causes a runtime error if a scalar is expected. The watermark is no longer reliably readable without defensive query logic.

**Enforcement points:**
- `pipeline.py`: watermark write must construct a new single-row Parquet file and rename it over the existing one — not use an append write mode
- Post-run test: `SELECT COUNT(*) FROM read_parquet('pipeline/control.parquet')` must always return 1

---

### INV-34
The incremental pipeline must determine the next processing date as `watermark + 1 day`. It must not re-process dates at or before the current watermark.

**Category:** operational

**Why this matters:** Re-processing a watermarked date triggers FM-4 (INV-22 failure mode) — Silver promotion for an already-promoted date routes all transactions to quarantine as duplicates. Gold for that date goes to zero. The system reports success. The only protection is the watermark preventing the incremental pipeline from ever targeting an already-processed date.

**Enforcement points:**
- `pipeline.py` incremental path: `target_date = last_processed_date + timedelta(days=1)` — read from `control.parquet`; no override mechanism other than `--force-full` with a prior watermark reset

---

### INV-35
The historical pipeline must resume from `max(start_date, watermark + 1 day)` when a watermark already exists. It must not reprocess already-watermarked dates without an explicit `--force-full` flag combined with a prior manual watermark reset. `--force-full` must not reset the watermark itself — it must refuse to proceed if the watermark has not already been manually reset to a date before `start_date`.

**Category:** operational

**Why this matters:** If a historical pipeline run crashes after processing five of seven days, re-running from `start_date` would trigger FM-4 for the five already-processed dates. Resuming from `watermark + 1` makes crash-and-retry safe. The `--force-full` flag without a prior watermark reset would allow a re-run from `start_date` without clearing Silver first, producing the same FM-4 outcome as the case it was meant to handle.

**Enforcement points:**
- `pipeline.py` historical path: `effective_start = max(start_date, last_processed_date + timedelta(days=1))` when `control.parquet` exists
- `pipeline.py` `--force-full` handler: read watermark; if `watermark >= start_date`, print error and exit — do not proceed; only proceed if `watermark < start_date` (i.e., a prior manual reset has been performed)

---

### INV-36
Running the incremental pipeline when no new source file is available must leave all layers and the watermark unchanged. No records may be written, no watermark advanced.

**Category:** operational

**Why this matters:** Idempotent no-op behaviour is required for safe scheduled invocation (Brief §3.2). If the incremental pipeline advances the watermark even when no file is present, the next run targets `watermark + 2 days` and skips the missing date permanently, creating a gap in Silver and Gold that cannot be recovered without manual watermark reset.

**Enforcement points:**
- `pipeline.py` incremental path: before any Bronze loader invocation, check `os.path.exists(f'source/transactions_{target_date}.csv')`; if absent, log "no file available" and exit without modifying any layer or the watermark

---

### INV-36b
The `--reset-watermark` command must update only `pipeline/control.parquet`. It must not delete, modify, or truncate any Bronze partition, Silver partition, quarantine file, Gold file, or run log entry.

**Category:** operational

**Why this matters:** A watermark reset is a deliberate operational action to allow re-processing from a specific date. It must not have side effects on data layers. If `--reset-watermark` also deleted Silver partitions, a mistyped date argument could destroy weeks of promoted Silver data. The reset command is a metadata-only operation.

**Enforcement points:**
- `pipeline.py` `--reset-watermark` handler: the only filesystem write must be to `pipeline/control.parquet` via temp-rename; no directory traversal, no deletion of any other path
- Code review: confirm the handler function calls no delete, truncate, or rmdir operations

---

## TP-13 · Storage Write — Run Log (`run_log.parquet`)

---

### INV-37
The run log must receive exactly one row per dbt model and per Bronze loader per pipeline invocation, including for steps that are SKIPPED due to the path-existence check. Rows are appended — the file is never overwritten. Prior run rows must never be modified.

**Category:** operational

**Why this matters:** The run log is the connective tissue between Gold aggregates and their source records. If SKIPPED steps produce no run log entry, an operator auditing a re-run cannot distinguish "this run processed this date" from "this run skipped it because it was already done." The audit trail has a gap precisely at the re-run boundary, which is when operators are most likely to need it.

**Enforcement points:**
- `pipeline.py`: every path-existence check that results in a skip must immediately write a SKIPPED row to the run log with the correct `model_name`, `run_id`, `started_at`, and `completed_at`
- `pipeline.py`: run log writes must use append mode — never reconstruct or overwrite the file

---

### INV-37b
`run_log.parquet` must be appended atomically for each row addition. A crash mid-append must not leave the Parquet file with a corrupt footer, as this would make the entire run log unreadable.

**Category:** operational

**Why this matters:** Parquet files are read from the footer. A partial write that corrupts the footer makes the entire `run_log.parquet` unreadable — every historical run record is lost. This is the most severe possible run log failure because it is both silent (the pipeline may not detect it) and total (it affects all historical records, not just the current run's entry).

**Enforcement points:**
- `pipeline.py`: implement run log appends using read-all / append-row / write-new-file / atomic-rename: read existing `run_log.parquet` into memory, append the new row, write to `.tmp_run_log.parquet`, rename to `run_log.parquet`
- Alternative: maintain an in-memory list of run log rows per invocation and write all rows in a single Parquet write at pipeline exit

---

### INV-38
For any `_pipeline_run_id` appearing in a Silver record, a corresponding row must exist in the run log with `status = SUCCESS`. The run log row must be written only after the Silver Parquet file is closed and the write is confirmed complete — not before or during the write.

**Category:** data correctness

**Why this matters:** The run log is the audit mechanism for tracing Gold values back to their source. If a run log SUCCESS row is written before the Silver write completes, and the pipeline crashes between the run log write and the Silver write completing, the run log claims success for a Silver partition that either doesn't exist or is partial. An analyst tracing that `_pipeline_run_id` finds a SUCCESS log entry but corrupted or absent Silver data.

**Enforcement points:**
- `pipeline.py`: the sequence must be: (1) dbt Silver model runs and writes Parquet, (2) dbt exits with status 0, (3) `pipeline.py` writes SUCCESS row to run log — never in reverse order
- `pipeline.py`: on dbt non-zero exit, write FAILED row to run log; do not write SUCCESS

---

### INV-39
`error_message` in the run log must be null when `status = SUCCESS` or `status = SKIPPED`. It must be non-null when `status = FAILED`, containing the exception type and a sanitised description. Maximum 500 characters, truncated with `[truncated]`. File paths (strings beginning with `/`), stack traces, credentials, and internal hostnames must be excluded.

**Category:** operational

**Why this matters:** A non-null `error_message` on a SUCCESS row misleads operators — a query for `WHERE status = 'SUCCESS' AND error_message IS NOT NULL` should return zero rows as a health check, and any matches indicate a bug in the run log writer. A null `error_message` on a FAILED row makes the failure undiagnosable without re-running, which may not be possible if the source file is no longer available.

**Enforcement points:**
- `pipeline.py` run log writer: `error_message = None` for SUCCESS and SKIPPED status; `error_message = sanitise(exception)` for FAILED status
- `sanitise()` function: strip all strings matching `^/` (filesystem paths), limit to 500 chars, append `[truncated]` if truncated

---

### INV-39b
`records_rejected` in the run log must count only records written to quarantine (hard rejections). Records that enter Silver with `_is_resolvable = false` must not be included in `records_rejected`.

**Category:** data correctness

**Why this matters:** The run log `records_rejected` count is the primary observable signal for Silver rejection rates (Architecture R-3 mitigation). If it conflates quarantined records with `_is_resolvable = false` records, the count is wrong in both directions: an operator sees a high rejection count and investigates the quarantine, but the quarantine row count does not match the log — the discrepancy is confusing and the underlying data quality signal is lost. Additionally, the reconciliation `Bronze rows = Silver rows + quarantine rows` must hold using the actual quarantine file counts — polluting `records_rejected` with non-quarantine entries does not affect this equation but creates a misleading mismatch between the log and the quarantine file.

**Enforcement points:**
- `pipeline.py` or `silver_transactions` dbt model: `records_rejected` is populated from `COUNT(*) FROM silver/quarantine/date={date}/rejected.parquet` after the promotion run — not from a combined count that includes unresolvable records
- Separate tracking: `_is_resolvable = false` count should be tracked separately (proposed as `unresolvable_count` in OQ-1) but must never be added to `records_rejected`

---

### INV-40
Every Bronze, Silver, and Gold record must have a non-null `_pipeline_run_id`. A record without a run ID cannot be traced to its originating pipeline invocation.

**Category:** data correctness

**Why this matters:** `_pipeline_run_id` is the connective tissue of the audit trail. Given a suspicious Gold aggregate, the trace is: `_pipeline_run_id` → run log row → Silver promotion records → Bronze source. If any record in any layer has a null `_pipeline_run_id`, the trace is broken at that record. The system's core promise of full traceability fails silently.

**Enforcement points:**
- Bronze loader (Python): `_pipeline_run_id = run_id` where `run_id` is the UUID passed in from `pipeline.py`; assert non-null before write
- All Silver dbt models: `_pipeline_run_id` must be populated from the dbt variable passed by `pipeline.py`; assert non-null in a dbt test
- All Gold dbt models: same as Silver
- Sign-off verification: `SELECT COUNT(*) FROM read_parquet('...') WHERE _pipeline_run_id IS NULL` must return 0 for Bronze, Silver, and Gold

---

### INV-40b
`pipeline_type` in the run log must be exactly `HISTORICAL` or `INCREMENTAL` (uppercase). `layer` must be exactly `BRONZE`, `SILVER`, or `GOLD` (uppercase). Values outside these enumerations must cause the run log write to fail rather than produce unqueryable entries.

**Category:** operational

**Why this matters:** Run log queries that filter by `pipeline_type` or `layer` (e.g., "show me all Silver failures from incremental runs") depend on exact string matching. A value of `"historical"` or `"bronze_loader"` silently matches no rows. The operator sees an empty result and may conclude there are no failures, when in fact there are failures under a non-standard label.

**Enforcement points:**
- `pipeline.py` run log writer: `pipeline_type` and `layer` must be assigned from a Python `Enum` or constants, not free-form strings — the assignment site cannot produce an out-of-enum value
- Post-run test: `SELECT DISTINCT pipeline_type, layer FROM read_parquet('pipeline/run_log.parquet')` result must be a subset of the defined enumerations

---

## TP-14 · Process Guard — PID File

---

### INV-41a
`pipeline.py` must write a PID file containing the OS process ID to `pipeline/pipeline.pid` as the first action at startup, before any pipeline work begins.

**Category:** operational

**Why this matters:** If any pipeline work executes before the PID file is written, a concurrent invocation starting in the window between pipeline startup and PID file write will not detect the running process. Two concurrent invocations will both proceed past the guard, both attempt to write the same Bronze partitions, and both attempt to advance the watermark — producing the undefined concurrent-write state that the PID guard was designed to prevent.

**Enforcement points:**
- `pipeline.py`: PID file write must be literally the first statement in `main()`, before argument parsing, before watermark read, before any filesystem access

---

### INV-41b
`pipeline.py` must remove the PID file on clean exit (process completes normally).

**Category:** operational

**Why this matters:** A stale PID file from a cleanly-exited process blocks all future invocations until it is manually removed (see INV-41e for the mitigation). Even with INV-41e, a clean-exit PID file that is not removed leaves residual state that requires the stale-PID detection logic to fire on every subsequent startup — adding unnecessary complexity to the startup path.

**Enforcement points:**
- `pipeline.py`: `finally` block in `main()` that calls `os.remove('pipeline/pipeline.pid')` — executed on both normal return and on handled exceptions

---

### INV-41c
`pipeline.py` must remove the PID file on SIGTERM.

**Category:** operational

**Why this matters:** Container orchestration systems (Docker, Kubernetes) send SIGTERM before SIGKILL. If `pipeline.py` does not handle SIGTERM, the process exits without removing the PID file, leaving a stale PID that blocks the next startup. In a containerised environment, SIGTERM-on-stop is the expected shutdown mechanism — not handling it means every container restart requires manual PID file cleanup.

**Enforcement points:**
- `pipeline.py`: `signal.signal(signal.SIGTERM, handler)` where `handler` removes the PID file and then calls `sys.exit(0)` — registered at startup

---

### INV-41d
At startup, if a PID file exists and the process with that PID is currently alive, `pipeline.py` must exit immediately with a clear error message. It must not proceed with pipeline work.

**Category:** operational

**Why this matters:** DuckDB embedded mode provides no cross-process write locking. Two concurrent `pipeline.py` invocations reading the same watermark, passing the same path-existence check for the same date, and both writing to the same partition path produces undefined results — potentially a partial write at the canonical path that passes subsequent idempotency checks but contains corrupt data.

**Enforcement points:**
- `pipeline.py` startup: read `pipeline.pid`; call `os.kill(pid, 0)` (signal 0 checks liveness without sending a signal); if `os.kill` does not raise `ProcessLookupError`, the process is alive — print error and `sys.exit(1)`

---

### INV-41e
At startup, if a PID file exists but the process with that PID is not alive (stale PID file from a prior SIGKILL or OOM kill), `pipeline.py` must delete the stale PID file and proceed normally.

**Category:** operational

**Why this matters:** After a SIGKILL or OOM kill, Python signal handlers do not run and the PID file is not removed. Without stale-PID recovery, the pipeline can never restart after a crash without manual intervention — the operator must SSH into the container, find the PID file, and delete it. In a training system this is acceptable friction; in any production-adjacent system it is an unacceptable operational burden that will be hit on the very first pipeline failure.

**Enforcement points:**
- `pipeline.py` startup: after `os.kill(pid, 0)` raises `ProcessLookupError`, call `os.remove('pipeline/pipeline.pid')` and continue with normal startup — no error, no operator action required

---

### INV-42
Only `pipeline.py` may invoke dbt models as an operational action. Running dbt commands directly (outside `pipeline.py`) bypasses idempotency path-existence checks and will cause FM-4: all transactions for the target date rejected as duplicates with no error signal.

**Category:** operational

**Why this matters:** FM-4 is the highest-severity silent failure mode in the system. An operator running `dbt run --select silver_transactions` directly for a date already in Silver causes every transaction for that date to be rejected as `DUPLICATE_TRANSACTION_ID` and routed to quarantine. Silver for that date has zero records. Gold shows zero or disappears. The run log records SUCCESS. Nothing in the output indicates that anything went wrong.

**Enforcement points:**
- Documentation: model header comments in all Silver dbt models must state that direct dbt invocation bypasses idempotency checks
- Detection (post-hoc): a spike in `records_rejected` in the run log for a specific date, with `records_written = 0` in Silver and no corresponding source-file quality issue, is the observable signal for FM-4

---

## TP-15 / TP-20 · Transformation — Audit Columns and Run ID Threading

---

### INV-43a
A single UUID (RFC 4122 format) must be generated exactly once per `pipeline.py` invocation at process startup. The same UUID must be used for all run log rows produced by that invocation.

**Category:** data correctness

**Why this matters:** UUID is a global correlation key. If a non-UUID format is used (e.g., a timestamp string or an incrementing integer), re-runs produce IDs that sort chronologically rather than being unambiguously unique. A deterministic ID (e.g., `{date}_{pipeline_type}`) produces the same ID on re-run, creating two run log rows with the same `run_id` — one SUCCESS and one SKIPPED — that are indistinguishable without inspecting timestamps.

**Enforcement points:**
- `pipeline.py`: `run_id = str(uuid.uuid4())` at the top of `main()` — use Python's `uuid` module, not a custom generator
- Validation: assert `re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', run_id)` before first use

---

### INV-43b
The UUID generated at startup must be passed to and used by all Bronze loaders and all dbt invocations within that pipeline run. All `_pipeline_run_id` audit columns in Bronze, Silver, and Gold records from a single invocation must share the same UUID.

**Category:** data correctness

**Why this matters:** The audit lineage query — "given this Gold record, which Bronze files fed it?" — works by joining on `_pipeline_run_id` across the run log and all layer audit columns. If any model uses a different `run_id` (e.g., generates its own at invocation time), records from that model are invisible to the lineage join. A Gold aggregate computed from Silver records with two different `_pipeline_run_id` values traces back to only one of them.

**Enforcement points:**
- `pipeline.py`: pass `run_id` as a dbt variable (`--vars '{"run_id": "..."}'`) to every `dbt run` invocation
- `pipeline.py`: pass `run_id` as a parameter to every Bronze loader function call — no Bronze loader may generate its own UUID

---

### INV-44
Bronze audit columns (`_source_file`, `_ingested_at`, `_pipeline_run_id`) must be added by the Bronze loader. They must not be present in source CSVs. They must not be modified during Silver promotion.

**Category:** data correctness

**Why this matters:** Bronze audit columns establish provenance at the point of ingestion. If they were present in source CSVs, the source system could inject false provenance claims. If they were modified during Silver promotion, the Bronze record of when and by which pipeline run the data arrived would be lost — an analyst reading Silver would see the Silver run's `_pipeline_run_id`, not the original Bronze ingestion run.

**Enforcement points:**
- Bronze loader (Python): add `_source_file`, `_ingested_at`, `_pipeline_run_id` as computed columns after reading the source CSV — these columns must not exist in the CSV schema
- `silver_transactions` dbt model: `_source_file` and `_ingested_at` must be `SELECT`ed from Bronze as-is, not recomputed

---

### INV-44b
`_source_file` in Bronze must contain the exact originating CSV filename (e.g., `transactions_2024-01-15.csv`) — not a constant, not a directory path, and not a value re-derived from filesystem context at Silver promotion time.

**Category:** data correctness

**Why this matters:** Backward traceability — "given this Silver record, which source file did it come from?" — depends on `_source_file` containing the actual filename. A constant like `"extract"` or a full path like `"/data/source/transactions_2024-01-15.csv"` both break the canonical backward trace. Re-deriving from filesystem context at Silver promotion time is worse: if the source file is renamed or moved between Bronze load and Silver promotion, the value in Silver would differ from the Bronze record it was allegedly carried forward from.

**Enforcement points:**
- Bronze loader (Python): `_source_file = os.path.basename(source_csv_path)` — the filename component only, not the full path
- `silver_transactions` dbt model: `_source_file` is carried from `bronze._source_file` — not re-read from environment variables or dbt variables

---

### INV-45
Silver audit columns (`_source_file`, `_bronze_ingested_at`) must carry forward values read from the Bronze Parquet record's audit columns. They must not be re-derived from source files, filesystem paths, or any context outside the Bronze record at promotion time.

**Category:** data correctness

**Why this matters:** If `_bronze_ingested_at` is re-derived at Silver promotion time, it reflects when Silver ran, not when Bronze ingested. The audit column name says "bronze ingested at" — an analyst reading this value expects it to tell them when the data arrived in Bronze. A re-derived value is semantically wrong and quietly breaks the temporal audit trail.

**Enforcement points:**
- `silver_transactions` and `silver_accounts` dbt models: `_bronze_ingested_at = bronze._ingested_at` in the SELECT clause — no `CURRENT_TIMESTAMP`, no filename-derived date

---

## TP-19 · Rendering — Gold Output Read by Analyst (DuckDB CLI)

---

### INV-46
At any point an analyst queries Gold via DuckDB CLI, the Gold file at the canonical path must be either the prior complete version or the new complete version — never a partial write.

**Category:** data correctness

**Why this matters:** This is the read-side statement of INV-28. The atomic rename guarantees that the canonical path always points to a complete file. An analyst who queries Gold during a pipeline run will read the prior complete Gold output rather than a partial new one. The constraint is enforced by INV-28; INV-46 documents the analyst-facing guarantee that results from it.

**Enforcement points:**
- Enforced by INV-28 — no additional enforcement point; INV-46 is a derived guarantee from the atomic write implementation

---

### INV-47
Every Gold record must carry a non-null `_pipeline_run_id` and `_computed_at` timestamp. An analyst must be able to trace any Gold aggregate back to its originating pipeline run through these columns joined to the run log.

**Category:** data correctness

**Why this matters:** Gold is the analyst-facing layer. If an analyst questions a `total_signed_amount` value, the investigation path is: `_pipeline_run_id` → run log → Silver records → Bronze partition → source CSV. Without `_pipeline_run_id` on Gold records, the trace starts at Silver — the analyst knows which Silver records were used but not which pipeline run computed the Gold aggregate from them. The `_computed_at` timestamp provides a second independent check.

**Enforcement points:**
- All Gold dbt models: `_pipeline_run_id = '{{ var("run_id") }}'` and `_computed_at = CURRENT_TIMESTAMP` in every Gold model SELECT
- Sign-off verification: `SELECT COUNT(*) FROM read_parquet('gold/**/*.parquet') WHERE _pipeline_run_id IS NULL OR _computed_at IS NULL` must return 0

---

### INV-48
Running the full pipeline twice against the same input must produce identical Bronze row counts, Silver row counts, quarantine row counts, and Gold output. Timestamps in audit columns (`_ingested_at`, `_promoted_at`, `_computed_at`) are expected to differ between runs and are excluded from the idempotency comparison. `_pipeline_run_id` values will differ between runs (UUID per invocation) and are likewise excluded. All data values and row counts must be identical.

**Category:** data correctness

**Why this matters:** Idempotency is the system's guarantee against data corruption on re-run. If a second run produces different row counts or different data values, one of the runs is wrong — and without knowing which, neither can be trusted. The exclusion of timestamps and `_pipeline_run_id` from the comparison is deliberate: these are expected to differ and their difference does not affect data correctness.

**Enforcement points:**
- Sign-off verification: run the full pipeline, capture row counts and key data values; run again; diff the results excluding audit timestamp and run ID columns
- Test: specifically verify that `total_signed_amount` in Gold is identical across two runs, not just that row counts match

---

## TP-21 · Execution Sequencing — Layer Ordering and Concurrency

---

### INV-49
`pipeline.py` must execute layers in strict sequence for a given date: Bronze completes before Silver starts, Silver completes before Gold starts. Concurrent execution of Bronze and Silver, or Silver and Gold, for the same date is not permitted.

**Category:** operational

**Why this matters:** Silver transaction promotion reads from Bronze. If Bronze is still writing while Silver starts, Silver may read a partial Bronze partition and promote fewer records than the source contains. Gold reads from Silver — the same argument applies. Under DuckDB embedded mode, there is no cross-process write locking; concurrent writes to overlapping Parquet path hierarchies produce undefined results.

**Enforcement points:**
- `pipeline.py`: layer invocations must be sequential Python calls — `run_bronze(date)` returns before `run_silver(date)` is called; no threading, no subprocess concurrency for the same date
- Code review: confirm no `threading.Thread`, `multiprocessing.Process`, or `asyncio` concurrency is used for same-date layer execution

---

### INV-49b
The idempotency path-existence check in `pipeline.py` must execute before dbt is invoked for each model, not inside dbt. If the path exists, `pipeline.py` must skip the dbt invocation entirely and write a SKIPPED row to the run log.

**Category:** operational

**Why this matters:** This is the enforcement location for the entire idempotency architecture (D-2). An implementation that invokes dbt and relies on dbt-internal logic (e.g., `unique_key` incremental) to prevent re-promotion is effectively Option C, which was rejected (Architecture D-2). Under the rejected Option C, Bronze partitions are rewritten on every run, violating INV-06. The path-existence check in `pipeline.py` is not just one implementation choice — it is the chosen mechanism, and the failure mode of getting it wrong is FM-4.

**Enforcement points:**
- `pipeline.py`: before every `subprocess.run(['dbt', 'run', '--select', model_name, ...])`, call `os.path.exists(expected_output_path)`; skip if true
- Code review: confirm no dbt model uses `unique_key` or `incremental` merge strategies as a substitute for the `pipeline.py` path check

---

## TP-22 · Data Model Integrity — Column and Schema Constraints

---

### INV-50
`_signed_amount` absolute magnitude must equal the source `amount` value for every Silver transaction record. `ABS(_signed_amount)` must equal `amount` as read from Bronze.

**Category:** data correctness

**Why this matters:** This is a precision invariant that complements INV-19 (sign direction) and INV-20 (non-null). A sign assignment implementation that rounds or truncates the magnitude — for example, by multiplying through a floating-point intermediate — produces `_signed_amount` values that are close to but not equal to the source `amount`. Gold `total_signed_amount` accumulates these rounding errors across all transactions, producing a Gold financial total that diverges from the correct value by a small but non-zero and unbounded amount.

**Enforcement points:**
- `silver_transactions` dbt model: use DuckDB `DECIMAL` arithmetic throughout sign assignment; no `FLOAT` or `DOUBLE` intermediate cast
- Sign-off verification: `SELECT COUNT(*) FROM silver/transactions/**/*.parquet WHERE ABS(_signed_amount) != amount` must return 0

---

### INV-51
The `transactions_by_type` STRUCT in `gold_daily_summary` must include an entry for each transaction type that has at least one resolvable transaction on that date. The STRUCT must not include entries for transaction types with zero occurrences on that date.

**Category:** data correctness

**Why this matters:** An inconsistent STRUCT schema across rows — some dates having all five type entries, others having only two — makes the Gold output non-queryable without defensive null handling. Analysts writing `transactions_by_type.PURCHASE.count` against a row where that key is absent get a null or an error. A consistent behaviour (include if present, omit if absent) must be chosen and enforced uniformly.

**Enforcement points:**
- `gold_daily_summary` dbt model: build the STRUCT using a `GROUP BY transaction_type` approach that naturally includes only types with records; do not use a pivot with hard-coded type names that produces null entries for absent types

---

### INV-52
`affects_balance` in Silver transaction codes must be stored as a boolean type, not as the string values `"true"` / `"false"` or integers `0` / `1`.

**Category:** data correctness

**Why this matters:** DuckDB reads `affects_balance` from CSV as a string. If the Silver transaction codes promotion does not explicitly cast this field to boolean, any downstream query filtering `WHERE affects_balance = true` silently returns no rows (because `'true' != true` in DuckDB's type system). The correct records appear to not exist. This is a type-mismatch failure that is invisible unless the filter is tested explicitly.

**Enforcement points:**
- `silver_transaction_codes` dbt model: `CAST(affects_balance AS BOOLEAN)` in the SELECT — not implicit coercion; not string comparison
- Post-promotion test: `SELECT typeof(affects_balance) FROM silver/transaction_codes/data.parquet LIMIT 1` must return `'boolean'`

---

## Retired IDs

The following IDs from the original set have been split into sub-IDs. Do not reference these IDs in test cases or review comments.

| Retired ID | Replaced By | Reason for Split |
|------------|-------------|-----------------|
| INV-05 | INV-05a, INV-05b, INV-05c | Bundled atomic write constraint with temp file location constraint with temp file naming convention — three independently testable and independently violable conditions |
| INV-11 | INV-11a, INV-11b | Bundled the accounts Silver model read-path with the transaction account resolution read-path — different models, different enforcement points |
| INV-12 | INV-12a through INV-12e | Bundled five independent rejection checks — a test suite can pass four of five while violating the fifth; each needs its own test case |
| INV-16 | INV-16a, INV-16b | Bundled the NULL field check with the status enum check for accounts — different conditions, different rejection codes, different test data |
| INV-41 | INV-41a through INV-41e | Bundled four distinct PID file lifecycle behaviours; the stale-PID recovery case (INV-41e) was missing from the original entirely |
| INV-43 | INV-43a, INV-43b | Bundled UUID generation (once per invocation) with UUID propagation (to all callers) — an implementation can generate correctly but fail to pass the UUID to one Bronze loader |
