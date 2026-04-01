from __future__ import annotations

import argparse
from pathlib import Path

from mtlib.dataset.zones import import_zones_from_json


def main() -> None:
    parser = argparse.ArgumentParser(description="Import zones JSON into existing HDF5 dataset.")
    parser.add_argument("zones_json", type=Path, help="Path to positions.json")
    parser.add_argument("dataset_path", type=Path, help="Path to target HDF5 dataset")

    args = parser.parse_args()
    import_zones_from_json(args.zones_json, args.dataset_path)


if __name__ == "__main__":
    main()