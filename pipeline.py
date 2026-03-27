import argparse
import os
import signal
import sys
import time
import uuid
from datetime import datetime

PID_FILE = 'data/pipeline/pipeline.pid'


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
            print(f"[historical] run_id={run_id} (not yet implemented)")
        elif args.incremental:
            print(f"[incremental] run_id={run_id} (not yet implemented)")
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
