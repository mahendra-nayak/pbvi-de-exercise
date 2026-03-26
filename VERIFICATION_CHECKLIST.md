# Verification Checklist
**Project:** Credit Card Financial Transactions Lake
**Engineer:** Mahendra Nayak

---

## Silver Baseline Counts
**Recorded:** 2026-03-26 (Session 3, Task 3.4)
**Purpose:** Locked values for Phase 5 sign-off. Do not re-derive during sign-off — compare Phase 5 output against these verbatim.

---

### Query 1 — Silver transactions per date

```sql
SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date, COUNT(*) AS silver_rows
FROM read_parquet('data/silver/transactions/**/*.parquet', filename=true)
GROUP BY 1 ORDER BY 1;
```

| date       | silver_rows |
|------------|-------------|
| 2024-01-01 | 4           |
| 2024-01-02 | 4           |
| 2024-01-03 | 4           |
| 2024-01-04 | 4           |
| 2024-01-05 | 4           |
| 2024-01-06 | 4           |
| 2024-01-07 | 4           |

---

### Query 2 — Quarantine rows per date (transactions only)

```sql
SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date, COUNT(*) AS quarantine_rows
FROM read_parquet('data/silver/quarantine/**/*.parquet', filename=true)
WHERE filename NOT LIKE '%rejected_accounts%'
GROUP BY 1 ORDER BY 1;
```

| date       | quarantine_rows |
|------------|-----------------|
| 2024-01-01 | 1               |
| 2024-01-02 | 1               |
| 2024-01-03 | 1               |
| 2024-01-04 | 1               |
| 2024-01-05 | 1               |
| 2024-01-06 | 1               |
| 2024-01-07 | 1               |

---

### Query 3 — Silver accounts totals

```sql
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT account_id) AS distinct_accounts
FROM read_parquet('data/silver/accounts/data.parquet');
```

| total_rows | distinct_accounts |
|------------|-------------------|
| 3          | 3                 |

---

### Query 4 — Cross-partition uniqueness (expected: 0 rows)

```sql
SELECT transaction_id, COUNT(*) AS cnt
FROM read_parquet('data/silver/transactions/**/*.parquet')
GROUP BY 1 HAVING COUNT(*) > 1;
```

| transaction_id | cnt |
|----------------|-----|
| (0 rows)       |     |

---

### Query 5 — affects_balance type

```sql
SELECT typeof(affects_balance) AS type
FROM read_parquet('data/silver/transaction_codes/data.parquet') LIMIT 1;
```

| type    |
|---------|
| BOOLEAN |

---

### Query 6 — Distinct rejection reasons

```sql
SELECT DISTINCT _rejection_reason
FROM read_parquet('data/silver/quarantine/**/*.parquet');
```

| _rejection_reason |
|-------------------|
| INVALID_CHANNEL   |

---
