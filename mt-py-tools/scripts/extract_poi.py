from __future__ import annotations

import argparse
from pathlib import Path

from mtlib.dataset.poi import POIExtractionConfig, extract_poi_to_json


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract POI data from track dataset")
    parser.add_argument("dataset_path", type=Path, help="Path to HDF5 dataset with /tracks")
    parser.add_argument("--output", type=Path, default=None, help="Output JSON path")

    parser.add_argument("--min-lon", type=float, default=105.0)
    parser.add_argument("--max-lon", type=float, default=171.0)
    parser.add_argument("--min-lat", type=float, default=17.0)
    parser.add_argument("--max-lat", type=float, default=60.0)

    parser.add_argument("--bins-x", type=int, default=1200)
    parser.add_argument("--bins-y", type=int, default=900)

    parser.add_argument("--threshold-mode", choices=["percentile", "absolute"], default="percentile")
    parser.add_argument("--threshold-value", type=float, default=96.0)
    parser.add_argument("--min-cluster-cells", type=int, default=3)

    parser.add_argument("--chunk-rows-tracks", type=int, default=2_000_000)
    parser.add_argument("--chunk-rows-positions", type=int, default=2_000_000)

    parser.add_argument("--min-track-points", type=int, default=None)
    parser.add_argument("--require-both-pois", action="store_true", default=True)
    parser.add_argument("--top-destinations-per-poi", type=int, default=10)
    parser.add_argument("--min-destination-len", type=int, default=3)

    args = parser.parse_args()

    config = POIExtractionConfig(
        extent=(args.min_lon, args.max_lon, args.min_lat, args.max_lat),
        bins=(args.bins_x, args.bins_y),
        threshold_mode=args.threshold_mode,
        threshold_value=args.threshold_value,
        min_cluster_cells=args.min_cluster_cells,
        chunk_rows_tracks=args.chunk_rows_tracks,
        chunk_rows_positions=args.chunk_rows_positions,
        min_track_points=args.min_track_points,
        require_both_pois=args.require_both_pois,
        top_destinations_per_poi=args.top_destinations_per_poi,
        min_destination_len=args.min_destination_len,
    )

    out_path = extract_poi_to_json(
        dataset_path=args.dataset_path,
        output_path=args.output,
        config=config,
    )

    print(out_path)


if __name__ == "__main__":
    main()