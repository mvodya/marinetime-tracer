#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import json

from mtlib.nn import ArtifactBuildConfig, build_training_artifacts, guess_poi_json_path


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Build reusable training artifacts for track restoration.",
    )
    p.add_argument("dataset_path", type=Path, help="Path to *_tsorted.h5 dataset")
    p.add_argument("--out-dir", type=Path, required=True, help="Output directory for artifacts")
    p.add_argument("--poi-json", type=Path, default=None, help="Optional POI JSON path")
    p.add_argument("--overwrite", action="store_true", help="Allow non-empty output directory")

    p.add_argument("--global-extent", type=float, nargs=4, metavar=("MIN_LON", "MAX_LON", "MIN_LAT", "MAX_LAT"),
                   default=[105.0, 171.0, 17.0, 60.0])

    p.add_argument("--gap-time-sec", type=int, default=2 * 60 * 60)
    p.add_argument("--gap-dist-m", type=float, default=30_000.0)

    p.add_argument("--frag-gap-time-sec", type=int, default=1 * 60 * 60)
    p.add_argument("--frag-gap-dist-m", type=float, default=10_000.0)
    p.add_argument("--frag-min-disp-m", type=float, default=10_000.0)
    p.add_argument("--frag-min-points", type=int, default=32)

    p.add_argument("--good-tracks-target", type=int, default=500_000)
    p.add_argument("--h5-chunk-rows", type=int, default=1_500_000)
    p.add_argument("--density-max-points", type=int, default=30_000_000)
    p.add_argument("--density-chunk-rows", type=int, default=1_500_000)

    p.add_argument("--grid-size", type=int, default=128)
    p.add_argument("--cell-m", type=float, default=100.0)
    p.add_argument("--density-cell-m", type=float, default=1000.0)

    p.add_argument("--val-frac", type=float, default=0.02)
    p.add_argument("--split-seed", type=int, default=42)
    return p


def main() -> None:
    args = build_parser().parse_args()

    cfg = ArtifactBuildConfig()
    cfg.gap_time_sec = args.gap_time_sec
    cfg.gap_dist_m = args.gap_dist_m
    cfg.frag_gap_time_sec = args.frag_gap_time_sec
    cfg.frag_gap_dist_m = args.frag_gap_dist_m
    cfg.frag_min_disp_m = args.frag_min_disp_m
    cfg.frag_min_points = args.frag_min_points
    cfg.good_tracks_target = args.good_tracks_target
    cfg.h5_chunk_rows = args.h5_chunk_rows
    cfg.density_max_points = args.density_max_points
    cfg.density_chunk_rows = args.density_chunk_rows
    cfg.val_frac = args.val_frac
    cfg.split_seed = args.split_seed
    cfg.grid.grid_size = args.grid_size
    cfg.grid.cell_m = args.cell_m
    cfg.grid.density_cell_m = args.density_cell_m

    poi_json = args.poi_json or guess_poi_json_path(args.dataset_path)

    result = build_training_artifacts(
        dataset_path=args.dataset_path,
        out_dir=args.out_dir,
        config=cfg,
        poi_json_path=poi_json,
        global_extent=args.global_extent,
        overwrite=args.overwrite,
        show_progress=True,
    )

    print("\nBuilt artifacts:")
    for key, path in result.items():
        print(f"  {key:12s} -> {path}")

    meta_path = result["meta"]
    print("\nMeta summary:")
    print(json.dumps(json.loads(Path(meta_path).read_text(encoding="utf-8")), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
