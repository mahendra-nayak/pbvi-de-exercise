# ARCHITECTURE.md
## Credit Card Financial Transactions Lake
**Version:** 1.0  
**Status:** Decided — Option A selected  
**Stack:** Docker Compose · dbt-core 1.7.x · dbt-duckdb 1.7.x · DuckDB (embedded) · Parquet on local filesystem · Python 3.11  

---

## 1. Problem Framing

### What the system solves

A financial services team processes credit card transactions every day. Analysts currently go directly to raw CSV extract files to do their work. This creates three compounding problems.

**No shared reference point.** Different analysts can work from different file versions — there is no single copy that everyone agrees is current. Two people running the same query on different days get different numbers, and neither can tell who is right.

**No audit trail.** When a number gets questioned, there is no way to trace where it came from. Was it from yesterday's file or last week's? Was it the raw version or a filtered one? The derivation path is invisible.

**No quality gate.** Malformed records, duplicate transactions, and missing account references flow directly to the analyst with no interception. Bad data becomes a bad decision before anyone notices.

The underlying need is a trust and governance problem, not a technical one. The system must produce a single authoritative data surface where every analyst-facing number is traceable back to its source file, every transformation is logged, and the raw source is protected from direct use.

The solution is a Medallion architecture — three layers moving data from raw to analyst-ready:
- **Bronze** — exact copy of what arrived in the source file, untouched
- **Silver** — cleaned, validated, and conformed records
- **Gold** — analyst-facing aggregations computed exclusively from Silver

The pipeline runs daily, must be re-runnable without producing duplicates or incorrect aggregations, and must leave a complete audit trail at every layer boundary.

### What the system explicitly does not solve

- **Credit decisions or risk scoring** — the system surfaces pre-existing transaction data. It does not compute risk, make credit decisions, or modify source system records.
- **Backfill of unresolvable transactions** — transactions referencing unknown accounts are flagged and excluded from Gold. Correcting them requires a dedicated backfill pipeline that is out of scope.
- **SCD Type 2 for Accounts** — Silver retains only the latest record per account. Historical credit limit changes and status transitions cannot be reconstructed.
- **Transaction code dimension changes** — the dimension is treated as static. Adding or modifying codes during pipeline operation is out of scope.
- **Streaming or near-realtime ingestion** — this is a daily batch pipeline only.
- **Schema evolution** — the CSV schema is fixed for this exercise.
- **Production deployment, monitoring, or alerting infrastructure** — this is a training vehicle.
- **Resolution of `_is_resolvable = false` records** — these accumulate in Silver permanently until a backfill pipeline (out of scope) is built.

---

## 2. Key Design Decisions

### D-1: Medallion Architecture (Bronze → Silver → Gold)

**What was decided:** Data moves through three distinct layers. Bronze is immutable raw landing. Silver is the authoritative clean layer. Gold is computed exclusively from Silver. No layer skips, no shortcuts.

**Rationale:** The core problem is analyst trust. The Medallion pattern creates a documented, auditable promotion path. An analyst seeing a Gold number can follow the `_pipeline_run_id` backwards through Silver to Bronze to the source file. Without distinct layers, that trace is impossible. Gold being computed exclusively from Silver — never from Bronze — means there is exactly one quality gate, and it is always in the same place.

**Alternatives rejected:**
- *Single-layer ETL (CSV → queryable table directly):* Eliminates the audit trail. Analysts are back to the same trust problem — the number exists but its lineage does not.
- *Two-layer (Raw + Curated):* Loses the separation between "what arrived" and "what passed quality checks." A corrupted raw record and a clean record would live in the same layer.

---

### D-2: Idempotency via Filesystem Path-Existence Check (Option A)

**What was decided:** `pipeline.py` checks whether the expected output path exists before invoking any Bronze loader or dbt model. If the path exists, the step is skipped and a `SKIPPED` row is written to the run log. If absent, the step executes. This is the sole idempotency mechanism — the filesystem is the source of truth.

**Rationale (in the engineer's words):** Option A is the only one of the three where every output is an unambiguously immutable Parquet file, the brief's constraints are satisfied without interpretation, and the idempotency mechanism is simple enough to read off the filesystem with `ls`. The failure mode it doesn't cover — corrupt-but-existing partitions — is one the environment cannot produce.

**Alternatives rejected:**

- *Option B — Manifest-Driven Checksum Verification:* B's distinguishing feature is detecting a Bronze partition that exists on disk but has the wrong row count. That matters in production where source systems deliver truncated or malformed extracts. On this system, the source data is static pre-seeded CSVs that don't change between runs — the corrupt-partition scenario cannot occur. B buys protection against a failure mode the environment makes impossible, and costs a manifest file (new mutable state that can drift out of sync with the filesystem), ~200 extra lines of `pipeline.py` logic, and a new class of failure to test.

- *Option C — dbt-Native unique_key Idempotency:* C has two genuine constraint gaps. First, the brief requires Bronze partitions to never be overwritten after initial write. Under C, the partition Parquet file is re-exported from a DuckDB table on every run — the content is identical but the file is rewritten. Logically correct but not what the brief says. Second, C needs a persistent `.duckdb` file to maintain unique constraint state across process invocations. The brief says "no intermediate databases." The Gap Check resolved this as a permitted interpretation, but it is still an interpretation, not a clean yes. Option A needs no such interpretation.

**Known limitation accepted:** Option A does not detect a Bronze partition that exists but contains wrong data. This is documented rather than hidden. On static seed data it cannot occur. If this were a production system receiving live extracts from an unreliable source system, Option B would become the right answer.

---

### D-3: Bronze Writes Are Atomic (Temp-Rename Pattern)

**What was decided:** Bronze loaders write to a temporary path, then rename atomically to the final partition path. The canonical path is only visible to readers after the write is complete.

**Rationale:** Idempotency requires that a re-run produces identical output to the first run. If a partial write is visible at the canonical path, the path-existence check passes on re-run, the step is skipped, and Bronze permanently contains fewer records than the source — with no error and no detection. Atomicity is a necessary condition for idempotency to hold. This is implied constraint IC-16.

**Alternatives rejected:**
- *In-place write to the canonical path:* A pipeline crash mid-write leaves a visible but incomplete partition. Re-run skips it. Bronze is silently wrong forever.
- *Write and verify after:* Writing then checking the row count catches the error but does not prevent a window where readers see the partial file.

---

### D-4: Single-Writer Enforcement (PID File)

**What was decided:** `pipeline.py` writes a PID file to `pipeline/pipeline.pid` at startup and removes it on clean exit. If a PID file exists and its process is alive on startup, the new invocation exits immediately with a clear error message.

**Rationale:** DuckDB embedded mode provides no cross-process write locking. Without a process guard, two concurrent `pipeline.py` invocations would both read the same watermark, both pass the path-existence check for the same date, and both attempt to write to the same partition path. The result is undefined. The brief's idempotency requirement presupposes single-writer semantics — concurrent execution violates it regardless of which architecture is chosen (implied constraint IC-17).

**Alternatives rejected:**
- *OS-level `flock`:* More robust but adds complexity and is less visible to an operator who needs to understand why a pipeline didn't start.
- *No guard (rely on scheduling:* Acceptable in production with a well-configured scheduler, not acceptable in a system where the scheduler is not part of this scope and manual re-runs are explicitly supported.

---

### D-5: `pipeline.py` Is the Sole Operational Invoker of dbt

**What was decided:** Running dbt commands directly outside `pipeline.py` is not a supported operational path. This is an operational constraint documented here and in the model headers, not a structural enforcement.

**Rationale:** Under Option A, idempotency is enforced by `pipeline.py`'s path-existence check before invoking dbt. If dbt is run directly, those gates are bypassed. A Silver model run directly against a date already in Silver will re-attempt promotion and reject every record as `DUPLICATE_TRANSACTION_ID`, writing all records to quarantine — silent data loss with a `SUCCESS` status (failure mode FM-4). The constraint does not prevent development-time use of dbt commands; it defines the production operational boundary (implied constraint IC-18).

**Alternatives rejected:**
- *Structural enforcement (Option C's approach):* Would require embedding idempotency inside dbt models via `unique_key`, which is Option C — rejected for separate reasons (D-2 above).
- *No constraint, accept the risk:* FM-4 produces no error signal. An operator who runs dbt directly will not know they've corrupted Silver until they notice Gold is wrong.

---

### D-6: Gold Writes Are Atomic (Temp-Rename Pattern)

**What was decided:** Gold dbt models write output to a staging location and `pipeline.py` renames to the final path atomically after dbt exits with status 0. The prior complete Gold file remains at the canonical path until the new computation is fully written.

**Rationale:** Gold is the analyst-facing layer. If Gold is overwritten in place and the pipeline crashes mid-write, analysts read a partially written file with no observable error — the worst possible failure mode because it is silent and the reader has no signal. Atomic write ensures the prior complete file is always readable until its replacement is ready (missing information item M-9, resolved here).

**Alternatives rejected:**
- *In-place overwrite:* Gold failure mid-write leaves analysts reading partial aggregates with no error.
- *Versioned Gold outputs (Gold/date=.../):* Adds complexity for analysts who must now discover which version is current. Out of scope.

---

### D-7: Gold Omits Dates with Zero Resolvable Transactions

**What was decided:** `gold_daily_summary` produces no row for a date where every transaction has `_is_resolvable = false`. It does not produce a zero-count row.

**Rationale:** Section 10.3 of the brief states "one row per distinct `transaction_date` in Silver transactions where `_is_resolvable = true`." A `WHERE _is_resolvable = true` filter before `GROUP BY transaction_date` naturally omits dates with no qualifying records. This matches the verification query semantics (missing information item M-10, resolved here).

**Alternatives rejected:**
- *Zero-count row for the date:* Would require a left join or explicit zero-fill. Produces a row that looks like data coverage but represents a quality failure. An analyst seeing a zero-count row and a missing row will interpret them differently — zero looks like a slow day, missing looks like a gap. The brief's verification semantics imply omission.

---

### D-8: Historical Pipeline Resumes from Watermark+1, Not from `start_date`

**What was decided:** When the historical pipeline is invoked and a watermark exists, it processes from `max(start_date, watermark + 1 day)` through `end_date`. It does not reprocess already-watermarked dates. A `--force-full` flag exists for disaster recovery but requires a manual watermark reset before use.

**Rationale:** If the historical pipeline always reprocesses from `start_date`, a crash-and-retry triggers FM-4 — Silver promotion for already-promoted dates rejects every transaction as `DUPLICATE_TRANSACTION_ID`, writing all records to quarantine. This is a silent data loss failure. Resuming from watermark+1 makes re-run safe and consistent with the incremental pipeline's semantics (missing information item M-11, resolved here).

**Alternatives rejected:**
- *Always process from `start_date` regardless of watermark:* Triggers FM-4 on every historical re-run. Unacceptable.
- *Destructive re-initialise (clear and reprocess):* Requires deleting Silver and Gold — violates Bronze immutability (you cannot easily reset Silver without also invalidating Bronze's promotion record) and loses the quarantine audit trail.

---

### D-9: `pipeline_run_id` Is a UUID Generated Once Per `pipeline.py` Invocation

**What was decided:** `pipeline.py` generates one UUID at process startup. This UUID is passed to all Bronze loaders and dbt invocations and appears in every audit column and run log row from that invocation.

**Rationale:** UUID is the standard practice. A deterministic run_id (e.g. `{date}_{pipeline_type}`) creates name collision risk on re-run and makes it ambiguous whether a run_id in the run log refers to the first run or the re-run. UUID makes every invocation uniquely identifiable. Audit lineage queries — "given this Gold record, which Bronze files fed it?" — are answered by joining on run_id across the run log and layer audit columns (missing information item M-12, resolved here).

**Alternatives rejected:**
- *Deterministic run_id:* Re-runs produce the same ID as first runs. The run log then has two rows with the same run_id — one SUCCESS (first run) and one SKIPPED (re-run). Distinguishing them requires querying by timestamp, defeating the purpose of run_id as a primary correlation key.

---

### D-10: Silver Accounts Is a Full Upsert Replacement (No SCD Type 2)

**What was decided:** `silver_accounts` retains only the latest record per `account_id`. Delta records from Bronze are upserted — existing records are replaced if a newer version arrives. No history is retained.

**Rationale:** SCD Type 2 is explicitly out of scope in the brief. This is a conscious simplification with a documented cost: there is no way to reconstruct what an account's credit limit or status was on a historical date. The Gold `closing_balance` column uses the most recent available Silver Accounts record for the week end date.

**Alternatives rejected:**
- *SCD Type 2:* Out of scope. Noted as a production pattern that would require significant additional model complexity and a different upsert key strategy.

---

### D-11: `UNRESOLVABLE_ACCOUNT_ID` Records Enter Silver with `_is_resolvable = false`, Not Quarantine

**What was decided:** Transactions referencing an `account_id` not present in Silver Accounts at promotion time are flagged, not rejected. They enter Silver with `_is_resolvable = false` and are excluded from all Gold aggregations.

**Rationale:** An unknown account at promotion time may be a timing issue — the account delta file for that day may not yet have been received — rather than a genuine data error. Quarantining these records permanently would mean valid transactions are never recoverable without a backfill pipeline. Flagging keeps them in Silver where they are auditable and could theoretically be resolved by a future backfill. This is the brief's explicit design decision.

**Alternatives rejected:**
- *Quarantine `UNRESOLVABLE_ACCOUNT_ID` records:* Permanently discards potentially valid transactions. Backfill becomes impossible without re-reading Bronze.
- *Block Silver promotion until all accounts resolve:* Creates a hard dependency between account delta timing and transaction processing. One late account file stalls the entire pipeline.

---

### D-12: `closing_balance` Uses Most Recent Silver Accounts Record at or Before `week_end_date`

**What was decided:** The Gold weekly account summary's `closing_balance` is taken from the Silver Accounts record with the most recent `_record_valid_from` at or before `week_end_date`. If no record exists for the account at all, `closing_balance` is `null`.

**Rationale:** The brief defines `closing_balance` as "current_balance from Silver Accounts as of week_end_date (or most recent available)." The fallback to most recent available is explicit — the question is how far back to look and what to do if nothing exists. Null is the correct signal for a genuine absence of account data rather than zero (which implies a known zero balance) or an error (missing information item M-7, resolved here).

**Alternatives rejected:**
- *Use the account record from exactly `week_end_date`:* Fails silently for any week where no account delta arrived on the last day of the week. Most weeks will have no delta on Sunday.
- *Return zero:* Misleads analysts — zero is a meaningful balance value. Null communicates "we don't have this" unambiguously.

---

### D-13: Error Messages Are Sanitised — No Paths, No Stack Traces

**What was decided:** The `error_message` column in the run log contains: exception type + sanitised message. Maximum 500 characters, truncated with `[truncated]`. Excluded: file paths, stack traces, credentials, internal hostnames.

**Rationale:** The brief explicitly states error messages "must not include file paths, credentials, or internal system detail." Minimum content for operator diagnostics requires the exception type and a description of what failed. A 500-character cap prevents log bloat while allowing enough context to diagnose without re-running (missing information item M-8, resolved here).

---

## 3. Challenge My Decisions

### D-2: Path-existence idempotency is too weak for production

**Strongest argument against:** A Bronze partition that exists with fewer rows than the source CSV — caused by a bug now fixed, a truncated write that beat the temp-rename guard, or a DuckDB version issue — will be silently skipped forever. The only recovery is manual intervention: delete the partition, reset the watermark, re-run. There is no automated detection, no alert, and no operator signal. For a system whose entire purpose is data trust, the idempotency mechanism itself has an undetectable failure mode.

**Verdict: Valid challenge — accepted as a known limitation for this scope, not valid as a blocker.** The source data is static pre-seeded CSVs. They cannot be truncated, corrupted, or modified between runs. The failure mode is real in production; it is not reachable in this training vehicle. If this were a production system, Option B's manifest with row-count verification would be the correct answer and this decision would be reversed. That distinction is documented explicitly rather than papered over.

---

### D-3 / D-6: Atomic temp-rename may not be atomic across filesystem boundaries

**Strongest argument against:** `os.rename()` in Python is only guaranteed atomic when source and temp file are on the same filesystem. In a Docker bind-mount setup, if the temp file and the output directory are on different underlying mounts, the rename becomes a copy-then-delete — not atomic. A crash between copy and delete leaves the target path in an undefined state.

**Verdict: Valid challenge — mitigated by implementation constraint.** The temp file must be written to the same directory as the target output, not to `/tmp` or a different mount. This must be enforced in the Bronze loader implementation — write temp file to `{target_dir}/.tmp_{filename}`, then rename. This is a build-phase implementation detail, not an architecture reversal. Added to open questions.

---

### D-5: Documenting `pipeline.py` as sole invoker is not enforcement

**Strongest argument against:** A comment in a model file does not prevent anyone from running `dbt run --select silver_transactions` directly during incident response. When something is wrong at 2am, engineers reach for the most direct tool available. FM-4 — all transactions rejected as duplicates after a direct dbt run — produces no error signal. The "documentation as enforcement" pattern fails precisely when it is most likely to be violated.

**Verdict: Valid challenge — accepted as a scope trade-off, not a design reversal.** Structural enforcement requires Option C's unique_key approach, which was rejected for separate constraint compliance reasons. The mitigation available under Option A is: (a) the operational constraint is documented here, in Claude.md, and in model header comments; (b) the `_pipeline_run_id` audit trail makes the violation detectable after the fact; (c) Silver's quarantine record count is an observable signal that something unusual happened. These are detection tools, not prevention tools. Prevention without Option C requires a wrapper script around dbt that refuses direct invocation — feasible but not specified.

---

### D-8: Resuming from watermark+1 makes historical re-runs inflexible

**Strongest argument against:** If the historical pipeline processed a date range with a bug that produced incorrect Silver records, the only correction path requires: (a) manually deleting the affected Silver partitions, (b) manually resetting the watermark, then (c) re-running. Step (b) requires direct Parquet file manipulation by a human. There is no `pipeline.py` command for "reset watermark to date X." This is a significant operational gap — in any real system, the ability to correct a processing error is a basic operational requirement, not an edge case.

**Verdict: Valid challenge — accepted as a scope trade-off consistent with the brief.** The brief explicitly states backfill capability is out of scope. The `--force-full` flag provides an escape hatch that requires manual watermark reset — that friction is intentional. The alternative (historical pipeline always processes from `start_date`) triggers FM-4 on normal re-run, which is a worse default. The watermark reset procedure must be documented in the operational runbook. Added to open questions.

---

### D-11: `_is_resolvable = false` records accumulate silently

**Strongest argument against:** Gold excludes `_is_resolvable = false` records with no visible signal to the analyst. The daily summary for a date where 30% of transactions are unresolvable looks identical to one where 100% are resolvable — same format, same columns, just smaller numbers. An analyst has no way to know the denominator is incomplete. This is not a theoretical risk; it is failure mode FM-3 identified in Interrogate. A data governance system that silently understates transaction counts is undermining its own stated purpose.

**Verdict: Valid challenge — partially mitigated, residual risk accepted.** The run log's `records_rejected` count is the observable signal for Silver rejection rates. An analyst who knows to check the run log can detect the accumulation. The brief's design — flag rather than quarantine — is explicit, and the backfill resolution path is explicitly out of scope. The mitigation available within scope: add an `unresolvable_count` column to `gold_daily_summary` alongside `total_transactions`. This makes the incomplete denominator visible at the Gold layer without requiring analysts to join to the run log. Added to open questions for Phase 3 decision.

---

## 4. Key Risks

**R-1: FM-4 — Silver phantom deduplication after a direct dbt run**
An operator runs `dbt run --select silver_transactions` directly (bypassing `pipeline.py`) for a date already in Silver. Every transaction for that date is rejected as `DUPLICATE_TRANSACTION_ID`. All records go to quarantine. Silver for that date now has zero records. Gold for that date shows zero or is absent. No error is raised. Detection requires noticing the Gold anomaly and tracing it through the run log.
*Mitigation:* Operational constraint (D-5). Model header comments. `_pipeline_run_id` audit trail enables post-hoc detection. Observable via quarantine record count spike in run log.

**R-2: Watermark advancement after Bronze/Silver success but before Gold completes**
If the pipeline crashes between Silver completion and Gold completion, and the watermark was advanced prematurely, the next run skips Bronze and Silver (paths exist) but Gold was never written. Gold is stale or absent. The watermark points to a date that is not fully reflected in Gold.
*Mitigation:* Watermark advances only after dbt exits with status 0 for the Gold run (D-2). The PID file and process guard prevent the scenario where a background crash goes unnoticed (D-4). This risk is eliminated by the sequencing constraint on watermark advancement.

**R-3: `_is_resolvable = false` accumulation producing silently understated Gold**
Transactions for real accounts accumulate as unresolvable if account delta files arrive late or out of order. Gold aggregates for those accounts are permanently understated with no visible error — only smaller numbers.
*Mitigation:* Run log `records_rejected` count is observable. Proposed: `unresolvable_count` column in `gold_daily_summary` (open question). Full resolution requires backfill pipeline (out of scope).

**R-4: Cross-filesystem rename breaking Bronze atomicity**
If the temp file and the output directory are on different Docker bind-mount points, `os.rename()` is not atomic. A crash between the copy and delete leaves the partition in an undefined state.
*Mitigation:* Implementation constraint — temp file must be written to the same directory as the target output. Enforced in code review. Added to build-phase open questions.

---

## 5. Key Assumptions

**A-1:** Source CSV files are complete and correct on delivery. A file that exists at `source/transactions_YYYY-MM-DD.csv` contains all transactions for that date with no truncation or corruption. This is the basis for rejecting Option B's row-count verification.

**A-2:** The source directory and data directory are on the same filesystem mount inside the Docker container. Required for atomic rename (see R-4).

**A-3:** The transaction codes dimension does not change during the exercise. It is loaded once at historical init and not updated during incremental runs.

**A-4:** `debit_credit_indicator` values in the transaction codes file are exactly `DR` or `CR` with no variants. Sign assignment in Silver depends on exact string matching.

**A-5:** `billing_cycle_start` and `billing_cycle_end` values in accounts are always integers in the range 1–28. Values 29, 30, 31 are not valid per the brief.

**A-6:** The pre-seeded seed data in the scaffold repository contains intentional quality issues (per the brief) but all intentional issues fall within the defined rejection reason codes. No new rejection codes are required.

**A-7:** DuckDB's incremental materialisation in the dbt-duckdb 1.7.x adapter uses partition-level semantics that prevent re-promotion of already-present records when the date partition predicate is applied. This must be verified against the actual adapter version before the Silver model is written.

---

## 6. Open Questions

**OQ-1 (build-phase):** Must the `unresolvable_count` column be added to `gold_daily_summary` to make incomplete denominators visible to analysts? The challenge to D-11 argues it should be. This is a one-column addition that does not require architectural change. Decision deferred to Phase 3.

**OQ-2 (build-phase):** Does the temp-rename pattern work correctly with the specific Docker bind-mount configuration in this scaffold? Specifically: are the `source/` and `data/` directories on the same filesystem mount? Must be verified before the Bronze loader is written (see R-4, A-2).

**OQ-3 (build-phase):** Verify dbt-duckdb 1.7.x adapter incremental materialisation semantics. Specifically: does `unique_key` on `transaction_id` with a `WHERE transaction_date = '{{ var("target_date") }}'` source filter correctly skip already-promoted records without a full cross-partition scan? Or does the adapter's MERGE implementation scan all existing Silver records?

**OQ-4 (operational):** What is the manual watermark reset procedure? It must be documented before the first production-equivalent run. Minimally: `python pipeline.py --reset-watermark YYYY-MM-DD --confirm`. The `--confirm` flag prevents accidental resets. This command must not delete any layer data — it only updates `pipeline/control.parquet`.

**OQ-5 (operational):** What constitutes sign-off for Phase 8 verification? The brief lists DuckDB CLI commands as the verification mechanism. The exact expected values depend on the scaffold's seed data. These values must be derived and locked before Phase 8 — deriving them during Phase 8 creates a circular verification problem.

---

## 7. Future Enhancements (Parking Lot)

These are conscious deferrals. Each has a rationale for why it is not in scope now and what would need to change for it to become in scope.

**PE-1: Backfill pipeline for `_is_resolvable = false` records**
Transactions with unknown accounts are permanently excluded from Gold. A backfill pipeline would re-attempt promotion for flagged records after new account data arrives. Deferred because: requires watermark guard logic to prevent future-date processing, requires a separate invocation interface, and adds significant complexity to Silver promotion sequencing. The brief explicitly calls this out of scope.

**PE-2: SCD Type 2 for Accounts**
Silver currently retains only the latest account record. A Type 2 implementation would preserve the full history of credit limit changes, status transitions, and balance snapshots. Deferred because: requires a surrogate key, valid_from/valid_to columns, and a fundamentally different upsert strategy. The brief acknowledges this as a known simplification.

**PE-3: Option B manifest with row-count verification**
If this system is ever adapted for a production environment where source files can be truncated or corrupted, Option B's manifest-driven idempotency should replace Option A's path-existence check. The switch does not require architectural changes to Bronze or Silver — only `pipeline.py` grows. Deferred because: the failure mode it addresses (corrupt-but-existing partition) cannot occur on static seed data.

**PE-4: Structural idempotency outside `pipeline.py` (Option C approach)**
If the operational constraint that `pipeline.py` must be the sole dbt invoker proves unenforceable in practice — particularly during incident response — Option C's `unique_key` incremental strategy should be adopted for Silver models. Deferred because: requires resolving the `.duckdb` file constraint interpretation and accepting non-filesystem-level Bronze immutability. Not required for training vehicle scope.

**PE-5: `unresolvable_count` in Gold daily summary**
Making the incomplete-denominator problem visible at the Gold layer rather than requiring analysts to join to the run log. Deferred to Phase 3 decision (OQ-1) — may be promoted to in-scope without architectural change.

**PE-6: Operational runbook**
A documented procedure for: watermark reset, manual Bronze partition deletion, quarantine inspection, and run log querying. Deferred because: out of scope for training vehicle. Required before any production use.

---

## 8. Data Model

### Source Entities (read-only inputs)

**transactions** (`source/transactions_YYYY-MM-DD.csv`)
One file per calendar day. Append-only fact entity — transactions are never updated or deleted in the source system. Fields: `transaction_id` (PK), `account_id` (FK → accounts), `transaction_date`, `amount` (always positive in source), `transaction_code` (FK → transaction_codes), `merchant_name` (nullable, PURCHASE only), `channel` (ONLINE | IN_STORE).

**transaction_codes** (`source/transaction_codes.csv`)
Single reference file, static. Fields: `transaction_code` (PK), `transaction_type` (PURCHASE | PAYMENT | FEE | INTEREST | REFUND), `description`, `debit_credit_indicator` (DR | CR), `affects_balance`.

**accounts** (`source/accounts_YYYY-MM-DD.csv`)
Daily delta — new and changed records only. Fields: `account_id` (PK), `open_date`, `credit_limit`, `current_balance`, `billing_cycle_start` (1–28), `billing_cycle_end` (1–28), `account_status` (ACTIVE | SUSPENDED | CLOSED).

---

### Bronze Layer (`bronze/`)

**bronze_transactions** (`bronze/transactions/date=YYYY-MM-DD/data.parquet`)
Exact copy of source transactions file plus audit columns. Written once. Never modified.
Audit columns: `_source_file`, `_ingested_at`, `_pipeline_run_id`.

**bronze_accounts** (`bronze/accounts/date=YYYY-MM-DD/data.parquet`)
Exact copy of source accounts delta file plus audit columns. Written once.
Audit columns: `_source_file`, `_ingested_at`, `_pipeline_run_id`.

**bronze_transaction_codes** (`bronze/transaction_codes/data.parquet`)
Exact copy of transaction codes reference file. Written once at historical init.
Audit columns: `_source_file`, `_ingested_at`, `_pipeline_run_id`.

---

### Silver Layer (`silver/`)

**silver_transactions** (`silver/transactions/date=YYYY-MM-DD/data.parquet`)
Clean, validated transactions promoted from Bronze. Sign applied via transaction_codes join.
Key computed column: `_signed_amount` — source `amount` multiplied by +1 (DR) or -1 (CR) per `debit_credit_indicator`.
Key flag: `_is_resolvable` — false if `account_id` not found in Silver Accounts at promotion time.
Audit columns: `_source_file`, `_bronze_ingested_at`, `_pipeline_run_id`, `_promoted_at`, `_is_resolvable`, `_signed_amount`.
Deduplication: `transaction_id` must be unique across all partitions.

**silver_accounts** (`silver/accounts/data.parquet`)
Latest record per `account_id`. Full upsert replacement on each run — no history.
Upsert key: `account_id`.
Audit columns: `_source_file`, `_bronze_ingested_at`, `_pipeline_run_id`, `_record_valid_from`.

**silver_transaction_codes** (`silver/transaction_codes/data.parquet`)
Reference data promoted from Bronze at historical init. Static thereafter.
Audit columns: `_source_file`, `_bronze_ingested_at`, `_pipeline_run_id`.

**silver_quarantine** (`silver/quarantine/date=YYYY-MM-DD/rejected.parquet`)
Records rejected during Silver promotion. Append-only. Never promoted without a backfill pipeline.
Rejection reason codes: `NULL_REQUIRED_FIELD`, `INVALID_AMOUNT`, `DUPLICATE_TRANSACTION_ID`, `INVALID_TRANSACTION_CODE`, `INVALID_CHANNEL` (transactions); `NULL_REQUIRED_FIELD`, `INVALID_ACCOUNT_STATUS` (accounts).
Audit columns: `_source_file`, `_pipeline_run_id`, `_rejected_at`, `_rejection_reason`.

---

### Gold Layer (`gold/`)

**gold_daily_summary** (`gold/daily_summary/data.parquet`)
One row per calendar day where at least one resolvable transaction exists. Full recompute on each run. Atomic overwrite.
Key columns: `transaction_date`, `total_transactions` (resolvable only), `total_signed_amount`, `transactions_by_type` (STRUCT), `online_transactions`, `instore_transactions`.
Audit columns: `_computed_at`, `_pipeline_run_id`, `_source_period_start`, `_source_period_end`.

**gold_weekly_account_summary** (`gold/weekly_account_summary/data.parquet`)
One row per account per ISO calendar week (Monday–Sunday) where the account has at least one resolvable transaction. Full recompute on each run. Atomic overwrite.
Key columns: `week_start_date`, `week_end_date`, `account_id`, `total_purchases`, `avg_purchase_amount`, `total_payments`, `total_fees`, `total_interest`, `closing_balance` (most recent Silver Accounts record at or before `week_end_date`; null if no record exists).
Audit columns: `_computed_at`, `_pipeline_run_id`.

---

### Pipeline Control (`pipeline/`)

**pipeline/control.parquet**
Single-row watermark tracker. Atomic overwrite on advance.
Columns: `last_processed_date`, `updated_at`, `updated_by_run_id`.
Constraint: advances only after Bronze, Silver, and Gold all complete with status SUCCESS for a given date.

**pipeline/run_log.parquet**
Append-only execution audit. One row per dbt model (or Bronze loader) per pipeline invocation.
Columns: `run_id` (UUID, shared across all rows from same invocation), `pipeline_type` (HISTORICAL | INCREMENTAL), `model_name`, `layer` (BRONZE | SILVER | GOLD), `started_at`, `completed_at`, `status` (SUCCESS | FAILED | SKIPPED), `records_processed`, `records_written`, `records_rejected` (Silver models only), `error_message` (null on success; sanitised, max 500 chars, no paths or credentials).

**pipeline/pipeline.pid**
Written at `pipeline.py` startup. Removed on clean exit and SIGTERM. Contains the OS PID of the running process. Checked at startup — if file exists and process is alive, new invocation exits immediately.

---

## 9. Sign-off Conditions (from brief Section 10)

These must be expressible as exact DuckDB CLI commands before Phase 3 is complete. Exact expected values depend on the scaffold seed data and must be derived before Phase 8.

- Bronze row counts across all 7 date partitions equal total rows across all 7 source CSV files (per entity)
- Total Silver transactions rows + quarantine rows = total Bronze transactions rows
- No `transaction_id` appears more than once across all Silver transactions partitions
- Every Silver transactions record has a valid `transaction_code` in Silver transaction_codes
- No Silver transactions record has a null `_signed_amount`
- Every quarantine record has a non-null `_rejection_reason` from the pre-defined code list
- Gold `daily_summary` contains exactly one row per distinct `transaction_date` in Silver where `_is_resolvable = true`
- Gold `weekly_account_summary total_purchases` matches Silver PURCHASE count for the corresponding week and account
- Running the full pipeline twice produces identical Bronze, Silver, quarantine, and Gold output
- Every Bronze, Silver, and Gold record has a non-null `_pipeline_run_id`
- For any `_pipeline_run_id` in Silver, a corresponding run log row exists with `status = SUCCESS`
