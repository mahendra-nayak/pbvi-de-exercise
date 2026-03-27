import argparse
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

PID_FILE = 'data/pipeline/pipeline.pid'


def run_historical(start_date: str, end_date: str, run_id: str) -> None:
    from pipeline.bronze_loader import (
        load_bronze_accounts,
        load_bronze_transaction_codes,
        load_bronze_transactions,
    )
    from pipeline.control import read_watermark, write_watermark
    from pipeline.gold_runner import run_gold
    from pipeline.run_log import write_run_log_row
    from pipeline.silver_runner import (
        run_silver_accounts,
        run_silver_transaction_codes,
        run_silver_transactions,
    )

    watermark = read_watermark()
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end   = datetime.strptime(end_date,   '%Y-%m-%d').date()

    if watermark is not None and watermark >= end:
        print(f"nothing to do: all dates already processed (watermark={watermark})")
        return

    effective_start = max(start, watermark + timedelta(days=1)) if watermark else start

    # Bronze TC — once only (INV-24d)
    bronze_tc_path = 'data/bronze/transaction_codes/data.parquet'
    if not os.path.exists(bronze_tc_path):
        started_at = datetime.now(timezone.utc)
        rows = load_bronze_transaction_codes(run_id)
        completed_at = datetime.now(timezone.utc)
        write_run_log_row(run_id, 'bronze_transaction_codes', 'BRONZE',
                          started_at=started_at, completed_at=completed_at,
                          pipeline_type='HISTORICAL', status='SUCCESS',
                          records_written=rows)
    else:
        write_run_log_row(run_id, 'bronze_transaction_codes', 'BRONZE',
                          pipeline_type='HISTORICAL', status='SKIPPED')

    # Silver TC — once only (INV-24b)
    run_silver_transaction_codes(run_id)

    # Per-date loop
    current = effective_start
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')

        # Bronze accounts
        bronze_acct_path = f'data/bronze/accounts/date={date_str}/data.parquet'
        if not os.path.exists(bronze_acct_path):
            started_at = datetime.now(timezone.utc)
            rows = load_bronze_accounts(date_str, run_id)
            completed_at = datetime.now(timezone.utc)
            write_run_log_row(run_id, 'bronze_accounts', 'BRONZE',
                              started_at=started_at, completed_at=completed_at,
                              pipeline_type='HISTORICAL', status='SUCCESS',
                              records_written=rows)
        else:
            write_run_log_row(run_id, 'bronze_accounts', 'BRONZE',
                              pipeline_type='HISTORICAL', status='SKIPPED')

        # Bronze transactions
        bronze_txn_path = f'data/bronze/transactions/date={date_str}/data.parquet'
        if not os.path.exists(bronze_txn_path):
            started_at = datetime.now(timezone.utc)
            rows = load_bronze_transactions(date_str, run_id)
            completed_at = datetime.now(timezone.utc)
            write_run_log_row(run_id, 'bronze_transactions', 'BRONZE',
                              started_at=started_at, completed_at=completed_at,
                              pipeline_type='HISTORICAL', status='SUCCESS',
                              records_written=rows)
        else:
            write_run_log_row(run_id, 'bronze_transactions', 'BRONZE',
                              pipeline_type='HISTORICAL', status='SKIPPED')

        # Silver — skips internally if partition exists (INV-22, INV-49b)
        run_silver_accounts(date_str, run_id)
        run_silver_transactions(date_str, run_id)

        # Gold — delete prior canonical so run_gold recomputes over all Silver
        # data accumulated so far (Gold reads all Silver partitions, not just
        # the current date). Without this, gold_runner skips after the first date.
        from pipeline.gold_runner import GOLD_MODELS
        for _, (_, canonical_path) in GOLD_MODELS.items():
            if os.path.exists(canonical_path):
                os.remove(canonical_path)
        run_gold(run_id)

        # Watermark advances only after Gold succeeds (INV-33)
        write_watermark(current, run_id)
        current += timedelta(days=1)


def run_incremental(run_id: str) -> None:
    from pipeline.bronze_loader import load_bronze_accounts, load_bronze_transactions
    from pipeline.control import read_watermark, write_watermark
    from pipeline.gold_runner import run_gold
    from pipeline.run_log import write_run_log_row
    from pipeline.silver_runner import run_silver_accounts, run_silver_transactions

    watermark = read_watermark()
    if watermark is None:
        print("Error: no watermark — run historical pipeline first", file=sys.stderr)
        sys.exit(1)

    target   = watermark + timedelta(days=1)
    date_str = target.strftime('%Y-%m-%d')
    txn_path  = f'source/transactions_{date_str}.csv'
    acct_path = f'source/accounts_{date_str}.csv'

    if not os.path.exists(txn_path) or not os.path.exists(acct_path):
        print(f"no source file for {date_str} — pipeline is current")
        return  # INV-36: no run log row, no watermark change

    # Silver TC always SKIPPED in incremental (INV-24b)
    write_run_log_row(run_id, 'silver_transaction_codes', 'SILVER',
                      pipeline_type='INCREMENTAL', status='SKIPPED')

    # Bronze accounts
    bronze_acct_path = f'data/bronze/accounts/date={date_str}/data.parquet'
    if not os.path.exists(bronze_acct_path):
        started_at = datetime.now(timezone.utc)
        rows = load_bronze_accounts(date_str, run_id)
        completed_at = datetime.now(timezone.utc)
        write_run_log_row(run_id, 'bronze_accounts', 'BRONZE',
                          started_at=started_at, completed_at=completed_at,
                          pipeline_type='INCREMENTAL', status='SUCCESS',
                          records_written=rows)
    else:
        write_run_log_row(run_id, 'bronze_accounts', 'BRONZE',
                          pipeline_type='INCREMENTAL', status='SKIPPED')

    # Bronze transactions
    bronze_txn_path = f'data/bronze/transactions/date={date_str}/data.parquet'
    if not os.path.exists(bronze_txn_path):
        started_at = datetime.now(timezone.utc)
        rows = load_bronze_transactions(date_str, run_id)
        completed_at = datetime.now(timezone.utc)
        write_run_log_row(run_id, 'bronze_transactions', 'BRONZE',
                          started_at=started_at, completed_at=completed_at,
                          pipeline_type='INCREMENTAL', status='SUCCESS',
                          records_written=rows)
    else:
        write_run_log_row(run_id, 'bronze_transactions', 'BRONZE',
                          pipeline_type='INCREMENTAL', status='SKIPPED')

    # Silver + Gold + watermark (INV-33, INV-49)
    run_silver_accounts(date_str, run_id)
    run_silver_transactions(date_str, run_id)
    run_gold(run_id)
    write_watermark(target, run_id)


def main():
    run_id = str(uuid.uuid4())

    # PID file: stale/live check
    if os.path.exists(PID_FILE):
        pid = int(open(PID_FILE).read().strip())
        try:
            os.kill(pid, 0)
            print(f"Error: pipeline already running (PID {pid})", file=sys.stderr)
            sys.exit(1)
        except ProcessLookupError:
            os.remove(PID_FILE)  # stale: process dead
        except PermissionError:
            print(f"Error: pipeline already running (PID {pid})", file=sys.stderr)
            sys.exit(1)

    # Write PID
    os.makedirs(os.path.dirname(PID_FILE), exist_ok=True)
    open(PID_FILE, 'w').write(str(os.getpid()))

    # SIGTERM handler
    def _sigterm(signum, frame):
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
        sys.exit(0)
    signal.signal(signal.SIGTERM, _sigterm)

    try:
        parser = argparse.ArgumentParser(
            prog="pipeline.py",
            description="Credit card lake pipeline",
        )
        group = parser.add_mutually_exclusive_group()

        group.add_argument("--historical", action="store_true", help="Run historical pipeline")
        parser.add_argument("--start-date", metavar="YYYY-MM-DD")
        parser.add_argument("--end-date", metavar="YYYY-MM-DD")

        group.add_argument("--incremental", action="store_true", help="Run incremental pipeline")

        group.add_argument("--reset-watermark", metavar="YYYY-MM-DD", help="Reset watermark to date")
        parser.add_argument("--confirm", action="store_true", help="Confirm reset-watermark")
        parser.add_argument("--smoke-test-sleep", metavar="N", type=int, help=argparse.SUPPRESS)

        args = parser.parse_args()

        if args.smoke_test_sleep is not None:
            time.sleep(args.smoke_test_sleep)
            raise SystemExit(0)
        elif args.historical:
            if not args.start_date or not args.end_date:
                print("Error: --historical requires --start-date and --end-date", file=sys.stderr)
                sys.exit(1)
            run_historical(args.start_date, args.end_date, run_id)
        elif args.incremental:
            run_incremental(run_id)
        elif args.reset_watermark:
            from pipeline.control import read_watermark, write_watermark
            if not args.confirm:
                print("Error: --confirm required for --reset-watermark", file=sys.stderr)
                sys.exit(1)
            prior = read_watermark()
            print(f"Current watermark: {prior or 'none'}")
            try:
                target = datetime.strptime(args.reset_watermark, '%Y-%m-%d').date()
            except ValueError:
                print(f"Error: invalid date '{args.reset_watermark}'", file=sys.stderr)
                sys.exit(1)
            write_watermark(target, 'manual-reset')
            print(f"Watermark reset to {target}")
            raise SystemExit(0)
        else:
            parser.print_help()

        raise SystemExit(0)
    finally:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)


if __name__ == "__main__":
    main()
