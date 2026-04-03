from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator

import h5py
import numpy as np
from tqdm.auto import tqdm

from .ds import (
    append_rows,
    collect_day_datasets,
    ensure_group,
    open_dataset,
)

@dataclass(slots=True)
class DatasetFilterConfig:
    # --- Chunk sizes ---
    chunk_rows_positions: int = 2_000_000
    chunk_rows_ships: int = 5_000_000

    # --- Speed rules ---
    speed_moving_min: int = 10   # speed >= этого порога считаем "движением"
    speed_sanity_max: int = 800  # все, что выше, считаем мусором и удаляем

    # --- Keep criteria ---
    min_total_points: int = 50
    min_moving_points: int = 5
    min_max_speed: int = 20


def scan_max_ship_id(
    ships_ds: h5py.Dataset,
    *,
    chunk_rows_ships: int,
    show_progress: bool = True,
) -> int:
    n_ships = int(ships_ds.shape[0])
    max_ship_id = 0

    iterator = range(0, n_ships, chunk_rows_ships)
    if show_progress:
        iterator = tqdm(iterator, total=(n_ships + chunk_rows_ships - 1) // chunk_rows_ships,
                        desc="Pass0: scan ships for max_ship_id", unit="chunk")

    for start in iterator:
        end = min(n_ships, start + chunk_rows_ships)
        chunk = ships_ds[start:end]
        if chunk.size:
            m = int(chunk["ship_id"].max(initial=0))
            if m > max_ship_id:
                max_ship_id = m

    return max_ship_id


def compute_ship_stats(
    days: list[tuple[tuple[str, str, str], h5py.Dataset]],
    *,
    max_ship_id: int,
    chunk_rows_positions: int,
    speed_moving_min: int,
    speed_sanity_max: int,
    show_progress: bool = True,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Returns:
        total_points  : uint32 array indexed by ship_id
        moving_points : uint32 array indexed by ship_id
        max_speed     : uint16 array indexed by ship_id
    """
    total_points = np.zeros(max_ship_id + 1, dtype=np.uint32)
    moving_points = np.zeros(max_ship_id + 1, dtype=np.uint32)
    max_speed = np.zeros(max_ship_id + 1, dtype=np.uint16)

    total_positions = sum(int(day_ds.shape[0]) for _, day_ds in days)

    progress = None
    if show_progress:
        progress = tqdm(total=total_positions, desc="Pass1: scan positions (stats)", unit="rows")

    for _, day_ds in days:
        n = int(day_ds.shape[0])

        for start in range(0, n, chunk_rows_positions):
            end = min(n, start + chunk_rows_positions)
            chunk = day_ds[start:end]

            ship_ids = chunk["ship_id"].astype(np.int64, copy=False)

            sp = chunk["speed"].astype(np.int32, copy=False)
            sp_sane = np.clip(sp, 0, speed_sanity_max).astype(np.int32, copy=False)
            moving_mask = sp_sane >= speed_moving_min

            u, c = np.unique(ship_ids, return_counts=True)
            total_points[u] += c.astype(np.uint32, copy=False)

            if moving_mask.any():
                mv_ids = ship_ids[moving_mask]
                u2, c2 = np.unique(mv_ids, return_counts=True)
                moving_points[u2] += c2.astype(np.uint32, copy=False)

            np.maximum.at(max_speed, ship_ids, sp_sane.astype(np.uint16, copy=False))

            if progress is not None:
                progress.update(end - start)

    if progress is not None:
        progress.close()

    return total_points, moving_points, max_speed


def build_keep_mask(
    total_points: np.ndarray,
    moving_points: np.ndarray,
    max_speed: np.ndarray,
    *,
    min_total_points: int,
    min_moving_points: int,
    min_max_speed: int,
) -> np.ndarray:
    return (
        (total_points >= min_total_points)
        & (moving_points >= min_moving_points)
        & (max_speed >= min_max_speed)
    )


def create_filtered_dataset(
    src: h5py.File,
    out_path: str | Path,
    *,
    filter_rules_text: str,
    overwrite: bool = False,
) -> h5py.File:
    out_path = Path(out_path)

    if out_path.exists():
        if overwrite:
            out_path.unlink()
        else:
            raise FileExistsError(f"Output file already exists: {out_path}")

    dst = h5py.File(out_path, "w")

    for k, v in src.attrs.items():
        dst.attrs[k] = v

    dst.attrs["filtered_at"] = datetime.utcnow().isoformat()
    dst.attrs["filter_rules"] = filter_rules_text

    if "files" in src:
        src.copy("files", dst)

    if "zones" in src:
        src.copy("zones", dst)

    tracks_dtype = src["tracks"].dtype if "tracks" in src else np.dtype([("track_id", "i8")])
    dst.create_dataset(
        "tracks",
        shape=(0,),
        maxshape=(None,),
        dtype=tracks_dtype,
        chunks=True,
        compression="gzip",
        compression_opts=4,
    )

    ships_dtype = src["ships"].dtype
    dst.create_dataset(
        "ships",
        shape=(0,),
        maxshape=(None,),
        dtype=ships_dtype,
        chunks=True,
        compression="gzip",
        compression_opts=4,
    )

    ensure_group(dst, "positions")
    return dst


def write_filtered_ships(
    src_ships: h5py.Dataset,
    dst_ships: h5py.Dataset,
    keep_mask: np.ndarray,
    *,
    chunk_rows_ships: int,
    show_progress: bool = True,
) -> None:
    n_ships = int(src_ships.shape[0])

    progress = None
    if show_progress:
        progress = tqdm(total=n_ships, desc="Pass2: write ships", unit="rows")

    for start in range(0, n_ships, chunk_rows_ships):
        end = min(n_ships, start + chunk_rows_ships)
        chunk = src_ships[start:end]

        ids = chunk["ship_id"].astype(np.int64, copy=False)
        kept = chunk[keep_mask[ids]]
        append_rows(dst_ships, kept)

        if progress is not None:
            progress.update(end - start)

    if progress is not None:
        progress.close()


def write_filtered_positions(
    days: list[tuple[tuple[str, str, str], h5py.Dataset]],
    dst: h5py.File,
    keep_mask: np.ndarray,
    *,
    chunk_rows_positions: int,
    show_progress: bool = True,
) -> None:
    total_positions = sum(int(day_ds.shape[0]) for _, day_ds in days)

    progress = None
    if show_progress:
        progress = tqdm(total=total_positions, desc="Pass3: write positions", unit="rows")

    for (yyyy, mm, dd), day_src in days:
        g = ensure_group(dst, f"positions/{yyyy}/{mm}")

        day_dst = g.create_dataset(
            dd,
            shape=(0,),
            maxshape=(None,),
            dtype=day_src.dtype,
            chunks=True,
            compression="gzip",
            compression_opts=4,
        )

        n = int(day_src.shape[0])

        for start in range(0, n, chunk_rows_positions):
            end = min(n, start + chunk_rows_positions)
            chunk = day_src[start:end]

            ids = chunk["ship_id"].astype(np.int64, copy=False)
            kept = chunk[keep_mask[ids]]
            append_rows(day_dst, kept)

            if progress is not None:
                progress.update(end - start)

    if progress is not None:
        progress.close()


def filter_dataset(
    dataset_path: str | Path,
    out_path: str | Path,
    *,
    config: DatasetFilterConfig | None = None,
    overwrite: bool = False,
    show_progress: bool = True,
) -> Path:
    config = config or DatasetFilterConfig()

    with open_dataset(dataset_path, "r") as src:
        ships_src = src["ships"]

        days, _ = collect_day_datasets(src)

        max_ship_id = scan_max_ship_id(
            ships_src,
            chunk_rows_ships=config.chunk_rows_ships,
            show_progress=show_progress,
        )

        total_points, moving_points, max_speed = compute_ship_stats(
            days,
            max_ship_id=max_ship_id,
            chunk_rows_positions=config.chunk_rows_positions,
            speed_moving_min=config.speed_moving_min,
            speed_sanity_max=config.speed_sanity_max,
            show_progress=show_progress,
        )

        keep_mask = build_keep_mask(
            total_points,
            moving_points,
            max_speed,
            min_total_points=config.min_total_points,
            min_moving_points=config.min_moving_points,
            min_max_speed=config.min_max_speed,
        )

        filter_rules_text = (
            f"MIN_TOTAL_POINTS={config.min_total_points}, "
            f"MIN_MOVING_POINTS={config.min_moving_points}, "
            f"MIN_MAX_SPEED={config.min_max_speed}, "
            f"SPEED_MOVING_MIN={config.speed_moving_min}, "
            f"SPEED_SANITY_MAX={config.speed_sanity_max}"
        )

        dst = create_filtered_dataset(
            src,
            out_path,
            filter_rules_text=filter_rules_text,
            overwrite=overwrite,
        )

        try:
            write_filtered_ships(
                ships_src,
                dst["ships"],
                keep_mask,
                chunk_rows_ships=config.chunk_rows_ships,
                show_progress=show_progress,
            )

            write_filtered_positions(
                days,
                dst,
                keep_mask,
                chunk_rows_positions=config.chunk_rows_positions,
                show_progress=show_progress,
            )
        finally:
            dst.close()

    return Path(out_path)