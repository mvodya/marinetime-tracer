from __future__ import annotations

import argparse
from pathlib import Path

from mtlib.dataset.tsorted import TrackSortedConfig, repack_tracksorted_dataset


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build /positions/tracks/... layout sorted by track_id ranges"
    )
    parser.add_argument("dataset_path", type=Path, help="Path to source HDF5 dataset")
    parser.add_argument("out_path", type=Path, help="Path to output tsorted HDF5 dataset")

    parser.add_argument("--poi-json", type=Path, default=None, help="Path to POI json")
    parser.add_argument("--no-poi-filter", action="store_true", help="Disable POI filter")
    parser.add_argument("--overwrite", action="store_true")

    parser.add_argument("--tracks-per-group", type=int, default=100_000)
    parser.add_argument("--datasets-per-group", type=int, default=100)

    parser.add_argument("--read-chunk-rows", type=int, default=2_000_000)
    parser.add_argument("--flush-threshold-rows", type=int, default=2_000_000)

    parser.add_argument(
        "--no-copy-original-positions",
        action="store_true",
        help="Do not copy original /positions into output",
    )

    args = parser.parse_args()

    config = TrackSortedConfig(
        use_poi_filter=not args.no_poi_filter,
        tracks_per_group=args.tracks_per_group,
        datasets_per_group=args.datasets_per_group,
        read_chunk_rows=args.read_chunk_rows,
        flush_threshold_rows=args.flush_threshold_rows,
        copy_original_positions=not args.no_copy_original_positions,
    )

    out_path = repack_tracksorted_dataset(
        dataset_path=args.dataset_path,
        out_path=args.out_path,
        poi_json_path=args.poi_json,
        config=config,
        overwrite=args.overwrite,
        show_progress=True,
    )

    print(out_path)


if __name__ == "__main__":
    main()