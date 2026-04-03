from __future__ import annotations

import argparse
from pathlib import Path

from mtlib.dataset.filtering import DatasetFilterConfig, filter_dataset


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter HDF5 dataset by ship activity rules.")
    parser.add_argument("dataset_path", type=Path, help="Path to source HDF5 dataset")
    parser.add_argument("out_path", type=Path, help="Path to output filtered HDF5 dataset")

    parser.add_argument("--chunk-rows-positions", type=int, default=2_000_000)
    parser.add_argument("--chunk-rows-ships", type=int, default=5_000_000)

    parser.add_argument("--speed-moving-min", type=int, default=10)
    parser.add_argument("--speed-sanity-max", type=int, default=800)

    parser.add_argument("--min-total-points", type=int, default=50)
    parser.add_argument("--min-moving-points", type=int, default=5)
    parser.add_argument("--min-max-speed", type=int, default=20)

    parser.add_argument("--overwrite", action="store_true")

    args = parser.parse_args()

    config = DatasetFilterConfig(
        chunk_rows_positions=args.chunk_rows_positions,
        chunk_rows_ships=args.chunk_rows_ships,
        speed_moving_min=args.speed_moving_min,
        speed_sanity_max=args.speed_sanity_max,
        min_total_points=args.min_total_points,
        min_moving_points=args.min_moving_points,
        min_max_speed=args.min_max_speed,
    )

    out_path = filter_dataset(
        dataset_path=args.dataset_path,
        out_path=args.out_path,
        config=config,
        overwrite=args.overwrite,
        show_progress=True,
    )

    print(out_path)


if __name__ == "__main__":
    main()