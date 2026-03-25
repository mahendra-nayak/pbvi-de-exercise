import argparse
import uuid


def main():
    run_id = str(uuid.uuid4())

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

    args = parser.parse_args()

    if args.historical:
        print(f"[historical] run_id={run_id} (not yet implemented)")
    elif args.incremental:
        print(f"[incremental] run_id={run_id} (not yet implemented)")
    elif args.reset_watermark:
        print(f"[reset-watermark] run_id={run_id} (not yet implemented)")
    else:
        parser.print_help()

    raise SystemExit(0)


if __name__ == "__main__":
    main()
