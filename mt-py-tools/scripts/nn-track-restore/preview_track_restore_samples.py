#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from mtlib.nn import GridConfig, TrackInpaintDataset


def prepare_density_for_display(
    density: np.ndarray, q: float = 0.98
) -> tuple[np.ndarray, float]:
    vis = np.log1p(density.astype(np.float32, copy=False))
    nz = vis[vis > 0]
    vmax = float(np.quantile(nz, q)) if nz.size else 1.0
    vmax = max(vmax, 1e-6)
    return np.clip(vis, 0.0, vmax), vmax


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Preview generated train samples for track restoration"
    )
    p.add_argument("dataset_path", type=Path)
    p.add_argument("artifact_dir", type=Path)
    p.add_argument("--split", choices=["train", "val"], default="train")
    p.add_argument("--index", type=int, default=0)
    p.add_argument("--grid-size", type=int, default=128)
    p.add_argument("--cell-m", type=float, default=100.0)
    p.add_argument("--density-cell-m", type=float, default=1000.0)
    p.add_argument("--line_radius", type=int, default=1)
    return p


def main() -> None:
    args = build_parser().parse_args()

    cfg = GridConfig(
        grid_size=args.grid_size,
        cell_m=args.cell_m,
        density_cell_m=args.density_cell_m,
        line_radius=args.line_radius,
    )

    frags_name = "frags_train.parquet" if args.split == "train" else "frags_val.parquet"
    frags_path = args.artifact_dir / frags_name
    if not frags_path.exists():
        frags_path = frags_path.with_suffix(".csv")

    ds = TrackInpaintDataset(
        dataset_path=args.dataset_path,
        frags_path=frags_path,
        track_index_path=args.artifact_dir / "track_index.pkl",
        density_path=args.artifact_dir / "density.npz",
        grid_cfg=cfg,
        return_meta=True,
    )

    ex = ds[args.index]
    x = ex["x"].numpy()
    y = ex["y"].numpy()[0]
    meta = ex["meta"]

    fig = plt.figure(figsize=(12, 9))

    ax1 = fig.add_subplot(2, 2, 1)
    ax1.imshow(x[0], origin="lower")
    ax1.set_title("known")

    ax2 = fig.add_subplot(2, 2, 2)
    density_vis, density_vmax = prepare_density_for_display(x[3])
    ax2.imshow(density_vis, origin="lower", vmin=0.0, vmax=density_vmax)
    ax2.set_title("density")

    ax3 = fig.add_subplot(2, 2, 3)
    ax3.imshow(y, origin="lower")
    ax3.set_title("target")

    ax4 = fig.add_subplot(2, 2, 4)
    ax4.imshow(y, origin="lower")
    ax4.imshow(np.ma.masked_where(x[0] <= 0, x[0]), origin="lower", alpha=0.7)
    ax4.set_title("target + known")

    fig.suptitle(
        f"track_id={meta['track_id']} points={meta['points']} gaps={meta['gaps']}",
        fontsize=12,
    )
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
