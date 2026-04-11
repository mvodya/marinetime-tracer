from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader

from mtlib.dataset.ds import open_dataset

from .artifacts import load_density_npz, load_frags, load_track_index
from .config import GridConfig
from .geo import crop_resample_map, make_pos_channels, sample_gaps, window_for_fragment


@dataclass(slots=True)
class FragmentArrays:
    track_id: np.ndarray
    s: np.ndarray
    e: np.ndarray
    points: np.ndarray
    t0: np.ndarray
    t1: np.ndarray

    @classmethod
    def from_frame(cls, frags: pd.DataFrame) -> "FragmentArrays":
        need = ["track_id", "s", "e", "points", "t0", "t1"]
        miss = [c for c in need if c not in frags.columns]
        if miss:
            raise KeyError(f"Missing columns in frags: {miss}")
        return cls(
            track_id=frags["track_id"].to_numpy(dtype=np.int64, copy=True),
            s=frags["s"].to_numpy(dtype=np.int32, copy=True),
            e=frags["e"].to_numpy(dtype=np.int32, copy=True),
            points=frags["points"].to_numpy(dtype=np.int32, copy=True),
            t0=frags["t0"].to_numpy(dtype=np.int64, copy=True),
            t1=frags["t1"].to_numpy(dtype=np.int64, copy=True),
        )

    def __len__(self) -> int:
        return int(len(self.track_id))

    def row(self, idx: int) -> dict[str, int]:
        return {
            "track_id": int(self.track_id[idx]),
            "s": int(self.s[idx]),
            "e": int(self.e[idx]),
            "points": int(self.points[idx]),
            "t0": int(self.t0[idx]),
            "t1": int(self.t1[idx]),
        }


def load_fragment_arrays(path: str | Path) -> FragmentArrays:
    return FragmentArrays.from_frame(load_frags(path))


def read_track_fragment(
    h5: h5py.File,
    track_index: dict[int, tuple[str, int, int]],
    track_id: int,
    s: int,
    e: int,
) -> np.ndarray:
    meta = track_index.get(int(track_id))
    if meta is None:
        raise KeyError(f"track_id not found in track_index: {track_id}")

    dset_path, track_s, track_e = meta
    dset = h5[dset_path]

    start = track_s + int(s)
    end = track_s + int(e)
    if start < track_s or end > track_e or end <= start:
        raise IndexError(
            f"Fragment bounds out of range for track {track_id}: "
            f"track=({track_s}, {track_e}) req=({start}, {end})"
        )

    arr = dset[start:end]
    if len(arr) == 0:
        raise ValueError(f"Empty fragment for track {track_id} and bounds ({s}, {e})")

    tids = arr["track_id"].astype(np.int64, copy=False)
    if np.any(tids != int(track_id)):
        arr = arr[tids == int(track_id)]
        if len(arr) == 0:
            raise ValueError(f"Filtered fragment became empty for track {track_id}")

    return arr


def rasterize_polyline_to_grid(
    lat: np.ndarray,
    lon: np.ndarray,
    extent: list[float],
    cfg: GridConfig,
    *,
    mark_points: bool = True,
    max_step_cells: int | None = None,
    line_radius: int | None = None,
) -> np.ndarray:
    min_lon, max_lon, min_lat, max_lat = map(float, extent)
    h = w = cfg.grid_size
    out = np.zeros((h, w), dtype=np.float32)

    if len(lat) == 0:
        return out

    if line_radius is None:
        line_radius = cfg.line_radius

    x = ((lon - min_lon) / max(1e-12, (max_lon - min_lon)) * w).astype(np.float64)
    y = ((lat - min_lat) / max(1e-12, (max_lat - min_lat)) * h).astype(np.float64)

    x = np.clip(np.floor(x), 0, w - 1).astype(np.int32)
    y = np.clip(np.floor(y), 0, h - 1).astype(np.int32)

    def stamp(xx: int, yy: int) -> None:
        if line_radius <= 0:
            out[yy, xx] = 1.0
            return

        x0 = max(0, xx - line_radius)
        x1 = min(w, xx + line_radius + 1)
        y0 = max(0, yy - line_radius)
        y1 = min(h, yy + line_radius + 1)

        for sy in range(y0, y1):
            for sx in range(x0, x1):
                if max(abs(sx - xx), abs(sy - yy)) <= line_radius:
                    out[sy, sx] = 1.0

    def draw_line(x0: int, y0: int, x1: int, y1: int) -> None:
        dx = abs(x1 - x0)
        dy = -abs(y1 - y0)
        sx = 1 if x0 < x1 else -1
        sy = 1 if y0 < y1 else -1
        err = dx + dy

        xx, yy = x0, y0
        while True:
            stamp(xx, yy)
            if xx == x1 and yy == y1:
                break
            e2 = 2 * err
            if e2 >= dy:
                err += dy
                xx += sx
            if e2 <= dx:
                err += dx
                yy += sy

    if mark_points:
        for xx, yy in zip(x, y, strict=False):
            stamp(int(xx), int(yy))

    if len(x) == 1:
        return out

    for i in range(len(x) - 1):
        x0, y0 = int(x[i]), int(y[i])
        x1, y1 = int(x[i + 1]), int(y[i + 1])

        if max_step_cells is not None:
            if max(abs(x1 - x0), abs(y1 - y0)) > max_step_cells:
                continue

        draw_line(x0, y0, x1, y1)

    return out


def build_known_and_target_masks(
    fragment: np.ndarray,
    cfg: GridConfig,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray, list[tuple[int, int]]]:
    lat = fragment["lat"].astype(np.float64)
    lon = fragment["lon"].astype(np.float64)

    extent = window_for_fragment(fragment, cfg)
    gaps = sample_gaps(len(fragment), cfg, rng)

    known_mask = np.ones(len(fragment), dtype=bool)
    for s, e in gaps:
        known_mask[s:e] = False

    known_radius = cfg.line_radius if cfg.line_radius_known is None else cfg.line_radius_known
    target_radius = cfg.line_radius if cfg.line_radius_target is None else cfg.line_radius_target

    known = rasterize_polyline_to_grid(
        lat[known_mask],
        lon[known_mask],
        extent,
        cfg,
        mark_points=True,
        max_step_cells=cfg.grid_size // 2,
        line_radius=known_radius,
    )
    target = rasterize_polyline_to_grid(
        lat,
        lon,
        extent,
        cfg,
        mark_points=True,
        max_step_cells=cfg.grid_size // 2,
        line_radius=target_radius,
    )
    return known, target, gaps


class TrackInpaintDataset(Dataset):
    def __init__(
        self,
        dataset_path: str | Path,
        frags_path: str | Path,
        track_index_path: str | Path,
        density_path: str | Path,
        *,
        grid_cfg: GridConfig,
        seed: int = 42,
        return_meta: bool = True,
        density_mode: str = "bilinear",
    ) -> None:
        self.dataset_path = Path(dataset_path)
        self.frags = load_fragment_arrays(frags_path)
        self.track_index = load_track_index(track_index_path)
        self.density_map, self.density_geo, self.global_extent = load_density_npz(density_path)

        self.grid_cfg = grid_cfg
        self.seed = int(seed)
        self.return_meta = bool(return_meta)
        self.density_mode = density_mode

        self._h5: h5py.File | None = None
        self._pos_x, self._pos_y = make_pos_channels(grid_cfg)

    def __len__(self) -> int:
        return len(self.frags)

    def _ensure_open(self) -> h5py.File:
        if self._h5 is None:
            self._h5 = open_dataset(self.dataset_path, "r")
        return self._h5

    def close(self) -> None:
        if self._h5 is not None:
            try:
                self._h5.close()
            finally:
                self._h5 = None

    def __del__(self):
        self.close()

    def _make_rng(self, idx: int) -> np.random.Generator:
        return np.random.default_rng(self.seed + int(idx))

    def make_example(self, idx: int) -> dict[str, Any]:
        row = self.frags.row(idx)
        h5 = self._ensure_open()

        fragment = read_track_fragment(
            h5,
            self.track_index,
            row["track_id"],
            row["s"],
            row["e"],
        )
        if len(fragment) < (self.grid_cfg.n_anchor * 2 + self.grid_cfg.gaps_min_points + 1):
            raise ValueError(
                f"Fragment too short for gaps: track={row['track_id']} len={len(fragment)}"
            )

        rng = self._make_rng(idx)
        known, target, gaps = build_known_and_target_masks(fragment, self.grid_cfg, rng)
        extent = window_for_fragment(fragment, self.grid_cfg)

        density = crop_resample_map(
            self.density_map,
            self.density_geo,
            extent,
            (self.grid_cfg.grid_size, self.grid_cfg.grid_size),
            mode=self.density_mode,
        ).astype(np.float32)

        x = np.stack(
            [
                known.astype(np.float32, copy=False),
                self._pos_x,
                self._pos_y,
                density,
            ],
            axis=0,
        )
        y = target[None, :, :].astype(np.float32, copy=False)

        result: dict[str, Any] = {
            "x": torch.from_numpy(x),
            "y": torch.from_numpy(y),
        }

        if self.return_meta:
            result["meta"] = {
                "idx": int(idx),
                "track_id": row["track_id"],
                "s": row["s"],
                "e": row["e"],
                "points": row["points"],
                "t0": row["t0"],
                "t1": row["t1"],
                "extent": [float(v) for v in extent],
                "gaps": [(int(a), int(b)) for a, b in gaps],
            }
        return result

    def __getitem__(self, idx: int) -> dict[str, Any]:
        return self.make_example(int(idx))


def collate_keep_meta(batch: list[dict[str, Any]]) -> dict[str, Any]:
    xs = torch.stack([item["x"] for item in batch], dim=0)
    ys = torch.stack([item["y"] for item in batch], dim=0)
    out: dict[str, Any] = {"x": xs, "y": ys}
    if "meta" in batch[0]:
        out["meta"] = [item["meta"] for item in batch]
    return out


def seed_worker(worker_id: int) -> None:
    seed = torch.initial_seed() % (2**32)
    np.random.seed(seed)


def make_loader(
    dataset: Dataset,
    *,
    batch_size: int,
    shuffle: bool,
    num_workers: int = 0,
    drop_last: bool = False,
    pin_memory: bool = False,
) -> DataLoader:
    return DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=num_workers,
        drop_last=drop_last,
        pin_memory=pin_memory,
        collate_fn=collate_keep_meta,
        worker_init_fn=seed_worker if num_workers > 0 else None,
        persistent_workers=(num_workers > 0),
    )


def build_datasets_from_artifact_dir(
    artifact_dir: str | Path,
    dataset_path: str | Path,
    *,
    grid_cfg: GridConfig,
    seed: int = 42,
    return_meta: bool = True,
) -> tuple[TrackInpaintDataset, TrackInpaintDataset]:
    artifact_dir = Path(artifact_dir)

    track_index_path = artifact_dir / "track_index.pkl"
    density_path = artifact_dir / "density.npz"

    train_frags_path = artifact_dir / "frags_train.parquet"
    if not train_frags_path.exists():
        train_frags_path = artifact_dir / "frags_train.csv"

    val_frags_path = artifact_dir / "frags_val.parquet"
    if not val_frags_path.exists():
        val_frags_path = artifact_dir / "frags_val.csv"

    train_ds = TrackInpaintDataset(
        dataset_path=dataset_path,
        frags_path=train_frags_path,
        track_index_path=track_index_path,
        density_path=density_path,
        grid_cfg=grid_cfg,
        seed=seed,
        return_meta=return_meta,
    )
    val_ds = TrackInpaintDataset(
        dataset_path=dataset_path,
        frags_path=val_frags_path,
        track_index_path=track_index_path,
        density_path=density_path,
        grid_cfg=grid_cfg,
        seed=seed + 10_000_000,
        return_meta=return_meta,
    )
    return train_ds, val_ds
