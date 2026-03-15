from __future__ import annotations

import argparse
import logging

from bronze_loader.loader import load_latest_to_bronze, supported_datasets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Load the latest raw Letterboxd CSV from MinIO into a bronze warehouse table.",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        choices=supported_datasets(),
        help="Dataset/table name to load into bronze.",
    )
    parser.add_argument(
        "--prefix",
        default="letterboxd/",
        help="MinIO object key prefix to search under.",
    )
    return parser


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    args = build_parser().parse_args()
    object_key = load_latest_to_bronze(target_table=args.dataset, prefix=args.prefix)
    logging.getLogger("bronze_loader.cli").info(
        "Bronze load complete for dataset=%s object_key=%s",
        args.dataset,
        object_key,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
