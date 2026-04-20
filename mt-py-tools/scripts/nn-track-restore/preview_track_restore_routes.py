#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import h5py
import pandas as pd
import torch
from tqdm.auto import tqdm

from mtlib.nn import (
    GridConfig,
    ResUNetAttention,
    RouteExtractionConfig,
    TrackInpaintDataset,
    load_checkpoint,
    load_frags,
    predict_and_extract_route,
    read_track_fragment,
    save_route_comparison_png,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Preview extracted restored routes and save comparison figures"
    )
    p.add_argument("dataset_path", type=Path)
    p.add_argument("artifact_dir", type=Path)
    p.add_argument("checkpoint", type=Path)
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--split", choices=["train", "val"], default="val")
    p.add_argument("--count", type=int, default=8)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--device", type=str, default="cpu")
    p.add_argument("--grid-size", type=int, default=128)
    p.add_argument("--cell-m", type=float, default=100.0)
    p.add_argument("--density-cell-m", type=float, default=1000.0)
    p.add_argument("--line-radius", type=int, default=1)
    p.add_argument("--line-radius-known", type=int, default=None)
    p.add_argument("--line-radius-target", type=int, default=None)
    p.add_argument("--base-ch", type=int, default=32)
    p.add_argument("--groups", type=int, default=8)
    p.add_argument("--attn-heads", type=int, default=4)
    p.add_argument("--anchor-radius", type=int, default=2)
    p.add_argument("--low-thr", type=float, default=0.20)
    p.add_argument("--high-thr", type=float, default=0.55)
    p.add_argument("--skeleton-max-iters", type=int, default=64)
    p.add_argument("--off-corridor-penalty", type=float, default=1.5)
    return p


def main() -> None:
    args = build_parser().parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    grid_cfg = GridConfig(
        grid_size=args.grid_size,
        cell_m=args.cell_m,
        density_cell_m=args.density_cell_m,
        line_radius=args.line_radius,
        line_radius_known=args.line_radius_known,
        line_radius_target=args.line_radius_target,
    )
    extract_cfg = RouteExtractionConfig(
        anchor_radius=args.anchor_radius,
        low_thr=args.low_thr,
        high_thr=args.high_thr,
        skeleton_max_iters=args.skeleton_max_iters,
        off_corridor_penalty=args.off_corridor_penalty,
    )

    frags_path = args.artifact_dir / (
        "frags_train.parquet" if args.split == "train" else "frags_val.parquet"
    )
    if not frags_path.exists():
        frags_path = frags_path.with_suffix(".csv")

    frags = load_frags(frags_path)
    if len(frags) == 0:
        raise RuntimeError("Fragments table is empty")

    ds = TrackInpaintDataset(
        dataset_path=args.dataset_path,
        frags_path=frags_path,
        track_index_path=args.artifact_dir / "track_index.pkl",
        density_path=args.artifact_dir / "density.npz",
        grid_cfg=grid_cfg,
        return_meta=True,
    )

    device = torch.device(args.device)
    model = ResUNetAttention(
        in_ch=4,
        out_ch=1,
        base_ch=args.base_ch,
        groups=args.groups,
        attn_heads=args.attn_heads,
    ).to(device)
    load_checkpoint(args.checkpoint, model=model, map_location=device)
    model.eval()

    start = max(0, int(args.offset))
    end = min(len(ds), start + max(0, int(args.count)))
    if start >= end:
        raise RuntimeError(f"Empty preview range: start={start} end={end}")

    h5 = h5py.File(args.dataset_path, "r")
    rows: list[dict[str, object]] = []

    try:
        for idx in tqdm(range(start, end), desc="Preview restored routes"):
            example = ds[idx]
            meta = example["meta"]
            fragment = read_track_fragment(
                h5,
                ds.track_index,
                meta["track_id"],
                meta["s"],
                meta["e"],
            )
            prob_map, result = predict_and_extract_route(
                model,
                example,
                fragment,
                grid_cfg=grid_cfg,
                device=device,
                extract_config=extract_cfg,
            )

            gap = tuple(meta["gaps"][0])
            out_path = args.out_dir / (
                f"{idx:04d}_track_{int(meta['track_id'])}_s{int(meta['s'])}_e{int(meta['e'])}.png"
            )
            title = (
                f"idx={idx} | split={args.split} | track_id={int(meta['track_id'])} | "
                f"source={result.path_source} | meanP={result.mean_prob_on_path:.3f}"
            )
            save_route_comparison_png(
                out_path,
                fragment,
                gap,
                meta["extent"],
                prob_map,
                result,
                title=title,
            )
            rows.append(
                {
                    "idx": int(idx),
                    "track_id": int(meta["track_id"]),
                    "s": int(meta["s"]),
                    "e": int(meta["e"]),
                    "points": int(meta["points"]),
                    "gap": gap,
                    "path_source": result.path_source,
                    "path_len": int(len(result.path_cells)),
                    "mean_prob_on_path": float(result.mean_prob_on_path),
                    "graph_nodes": int(result.artifacts.graph.number_of_nodes()),
                    "graph_edges": int(result.artifacts.graph.number_of_edges()),
                    "image_path": str(out_path),
                }
            )
    finally:
        h5.close()
        ds.close()

    summary = pd.DataFrame(rows)
    summary_path = args.out_dir / "summary.csv"
    summary.to_csv(summary_path, index=False)
    print(f"Saved {len(summary)} preview images to: {args.out_dir}")
    print(f"Saved summary CSV: {summary_path}")
    if len(summary) > 0:
        print(
            summary[
                ["idx", "track_id", "path_source", "path_len", "mean_prob_on_path"]
            ].to_string(index=False)
        )


if __name__ == "__main__":
    main()
