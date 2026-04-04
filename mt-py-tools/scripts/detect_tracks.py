from __future__ import annotations

import argparse
from pathlib import Path

from mtlib.dataset.tracks import TrackDetectionConfig, detect_tracks


def main() -> None:
    parser = argparse.ArgumentParser(description="Detect tracks and write track_id into positions.")
    parser.add_argument("src_path", type=Path)
    parser.add_argument("dst_path", type=Path)
    parser.add_argument("--overwrite", action="store_true")

    parser.add_argument("--speed-moving-min", type=int, default=10)
    parser.add_argument("--stop-radius-m", type=float, default=250.0)
    parser.add_argument("--stop-dwell-sec", type=int, default=30 * 60)
    parser.add_argument("--gap-hard-sec", type=int, default=5 * 3600)
    parser.add_argument("--gap-very-hard-sec", type=int, default=10 * 3600)
    parser.add_argument("--dist-after-gap-m", type=float, default=150_000.0)
    parser.add_argument("--jump-hard-m", type=float, default=250_000.0)
    parser.add_argument("--dest-gap-sec", type=int, default=4 * 3600)
    parser.add_argument("--dest-dist-m", type=float, default=50_000.0)
    parser.add_argument("--idle-track-id", type=int, default=-1)
    parser.add_argument("--chunk-rows", type=int, default=2_000_000)

    args = parser.parse_args()

    config = TrackDetectionConfig(
        speed_moving_min=args.speed_moving_min,
        stop_radius_m=args.stop_radius_m,
        stop_dwell_sec=args.stop_dwell_sec,
        gap_hard_sec=args.gap_hard_sec,
        gap_very_hard_sec=args.gap_very_hard_sec,
        dist_after_gap_m=args.dist_after_gap_m,
        jump_hard_m=args.jump_hard_m,
        dest_gap_sec=args.dest_gap_sec,
        dest_dist_m=args.dest_dist_m,
        idle_track_id=args.idle_track_id,
        chunk_rows=args.chunk_rows,
    )

    out = detect_tracks(
        src_path=args.src_path,
        dst_path=args.dst_path,
        config=config,
        overwrite=args.overwrite,
        show_progress=True,
    )

    print(out)


if __name__ == "__main__":
    main()