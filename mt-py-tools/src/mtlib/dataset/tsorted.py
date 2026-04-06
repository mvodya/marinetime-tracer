from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import h5py
import numpy as np
from tqdm.auto import tqdm

from .ds import open_dataset


TQDM_KW = dict(
    ascii=True,
    dynamic_ncols=True,
    mininterval=0.5,
)


@dataclass(slots=True)
class TrackSortedConfig:
    use_poi_filter: bool = True

    tracks_per_group: int = 100_000
    datasets_per_group: int = 100

    read_chunk_rows: int = 2_000_000
    flush_threshold_rows: int = 2_000_000

    copy_original_positions: bool = True

    tsort_fields: tuple[str, ...] = (
        "track_id",
        "timestamp",
        "lat",
        "lon",
        "speed",
        "course",
        "heading",
        "rot",
        "elapsed",
        "ship_id",
        "file_id",
        "destination",
        "tile_z",
    )


def ensure_parent_dir(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def range_label(a: int, b: int) -> str:
    return f"{a:08d}-{b:08d}"


def compute_group_bounds(track_id: int, tracks_per_group: int) -> tuple[int, int]:
    g0 = (track_id // tracks_per_group) * tracks_per_group
    g1 = g0 + tracks_per_group - 1
    return g0, g1


def compute_subrange_bounds(track_id: int, g0: int, subrange: int, tracks_per_group: int) -> tuple[int, int, int]:
    s_idx = (track_id - g0) // subrange
    s0 = g0 + s_idx * subrange
    s1 = min(s0 + subrange - 1, g0 + tracks_per_group - 1)
    return int(s_idx), int(s0), int(s1)


def stable_sort_by_track_then_time(arr: np.ndarray) -> np.ndarray:
    if arr.shape[0] <= 1:
        return arr

    idx_time = np.argsort(arr["timestamp"], kind="mergesort")
    arr2 = arr[idx_time]

    idx_track = np.argsort(arr2["track_id"], kind="mergesort")
    return arr2[idx_track]


def find_any_positions_dataset(ds: h5py.File) -> h5py.Dataset:
    pos_root = ds["positions"]
    for y in pos_root:
        gy = pos_root[y]
        if not isinstance(gy, h5py.Group):
            continue
        for m in gy:
            gm = gy[m]
            if not isinstance(gm, h5py.Group):
                continue
            for d in gm:
                obj = gm[d]
                if isinstance(obj, h5py.Dataset):
                    return obj
    raise RuntimeError("No positions datasets found in ds['positions'].")


def make_tsorted_dtype(src_dtype: np.dtype, tsort_fields: tuple[str, ...]) -> np.dtype:
    return np.dtype([(name, src_dtype.fields[name][0]) for name in tsort_fields])


def load_poi_track_ids(
    poi_json_path: str | Path,
    *,
    show_progress: bool = True,
) -> set[int]:
    poi_json_path = Path(poi_json_path)

    with open(poi_json_path, "r", encoding="utf-8") as f:
        poi = json.load(f)

    poi_track_ids: set[int] = set()
    pairs = poi.get("pairs", {})

    iterator = pairs.items()
    if show_progress:
        iterator = tqdm(iterator, desc="Collect POI track_ids", **TQDM_KW)

    for _, v in iterator:
        tids = v.get("track_ids", [])
        poi_track_ids.update(int(x) for x in tids)

    return poi_track_ids


def create_tsorted_output(
    src: h5py.File,
    out_path: str | Path,
    *,
    ts_dtype: np.dtype,
    copy_original_positions: bool,
    overwrite: bool = False,
) -> h5py.File:
    out_path = Path(out_path)

    if out_path.exists():
        if overwrite:
            out_path.unlink()
        else:
            raise FileExistsError(f"Output file already exists: {out_path}")

    ensure_parent_dir(out_path)
    out = h5py.File(out_path, "w")

    for k, v in src.attrs.items():
        out.attrs[k] = v

    out.attrs["tsorted_built_at"] = datetime.now(timezone.utc).isoformat()

    for name in ("files", "ships", "tracks", "zones"):
        if name in src:
            src.copy(name, out, name=name)

    if copy_original_positions:
        src.copy("positions", out, name="positions")
    else:
        out.create_group("positions")

    out["positions"].require_group("tracks")
    return out


def get_or_create_tsorted_dataset(
    out_tracks_root: h5py.Group,
    ds_cache: dict[str, h5py.Dataset],
    *,
    g0: int,
    g1: int,
    s0: int,
    s1: int,
    ts_dtype: np.dtype,
) -> h5py.Dataset:
    g_label = range_label(g0, g1)
    s_label = range_label(s0, s1)

    path_key = f"{g_label}/{s_label}"
    cached = ds_cache.get(path_key)
    if cached is not None:
        return cached

    g = out_tracks_root.require_group(g_label)

    if s_label in g:
        dset = g[s_label]
    else:
        dset = g.create_dataset(
            s_label,
            shape=(0,),
            maxshape=(None,),
            dtype=ts_dtype,
            chunks=True,
            compression="gzip",
            compression_opts=4,
        )

    ds_cache[path_key] = dset
    return dset


def append_rows(dset: h5py.Dataset, rows: np.ndarray) -> None:
    if rows.shape[0] == 0:
        return

    n0 = dset.shape[0]
    n_add = rows.shape[0]
    dset.resize((n0 + n_add,))
    dset[n0:n0 + n_add] = rows


def flush_one_buffer(
    out_tracks_root: h5py.Group,
    ds_cache: dict[str, h5py.Dataset],
    buffers: dict[tuple[int, int, int, int], list[np.ndarray]],
    buffer_sizes: dict[tuple[int, int, int, int], int],
    *,
    key: tuple[int, int, int, int],
    ts_dtype: np.dtype,
) -> int:
    parts = buffers.get(key)
    if not parts:
        return 0

    merged = np.concatenate(parts, axis=0)
    merged = stable_sort_by_track_then_time(merged)

    g0, g1, s0, s1 = key
    dset = get_or_create_tsorted_dataset(
        out_tracks_root=out_tracks_root,
        ds_cache=ds_cache,
        g0=g0,
        g1=g1,
        s0=s0,
        s1=s1,
        ts_dtype=ts_dtype,
    )
    append_rows(dset, merged)

    written = int(merged.shape[0])
    buffers[key].clear()
    buffer_sizes[key] = 0
    return written


def flush_all_buffers(
    out_tracks_root: h5py.Group,
    ds_cache: dict[str, h5py.Dataset],
    buffers: dict[tuple[int, int, int, int], list[np.ndarray]],
    buffer_sizes: dict[tuple[int, int, int, int], int],
    *,
    ts_dtype: np.dtype,
    show_progress: bool = True,
) -> int:
    keys = [k for k, v in buffer_sizes.items() if v > 0]
    total_written = 0

    iterator = keys
    if show_progress and keys:
        iterator = tqdm(keys, desc="Flush remaining buffers", unit="buffer", **TQDM_KW)

    for key in iterator:
        total_written += flush_one_buffer(
            out_tracks_root=out_tracks_root,
            ds_cache=ds_cache,
            buffers=buffers,
            buffer_sizes=buffer_sizes,
            key=key,
            ts_dtype=ts_dtype,
        )

    return total_written


def subset_fields(chunk: np.ndarray, ts_dtype: np.dtype, tsort_fields: tuple[str, ...]) -> np.ndarray:
    out = np.empty((chunk.shape[0],), dtype=ts_dtype)
    for name in tsort_fields:
        out[name] = chunk[name]
    return out


def filter_chunk_by_poi(chunk: np.ndarray, poi_track_ids: set[int] | None) -> np.ndarray:
    if poi_track_ids is None:
        return chunk

    tids = chunk["track_id"].astype(np.int64, copy=False)

    mask = np.fromiter(
        (int(t) in poi_track_ids for t in tids),
        dtype=np.bool_,
        count=tids.shape[0],
    )

    if not mask.any():
        return chunk[:0]

    return chunk[mask]


def split_chunk_into_subranges(
    chunk: np.ndarray,
    *,
    tracks_per_group: int,
    datasets_per_group: int,
):
    if chunk.shape[0] == 0:
        return []

    subrange = math.ceil(tracks_per_group / datasets_per_group)

    tids = chunk["track_id"].astype(np.int64, copy=False)
    g0s = (tids // tracks_per_group) * tracks_per_group
    s_idxs = ((tids - g0s) // subrange).astype(np.int64, copy=False)

    order = np.lexsort((s_idxs, g0s))
    chunk = chunk[order]
    g0s = g0s[order]
    s_idxs = s_idxs[order]

    change = np.empty(chunk.shape[0], dtype=np.bool_)
    change[0] = True
    change[1:] = (g0s[1:] != g0s[:-1]) | (s_idxs[1:] != s_idxs[:-1])

    cuts = np.flatnonzero(change)
    cuts = np.append(cuts, chunk.shape[0])

    result: list[tuple[tuple[int, int, int, int], np.ndarray]] = []

    for i in range(len(cuts) - 1):
        a = int(cuts[i])
        b = int(cuts[i + 1])

        g0 = int(g0s[a])
        g1 = g0 + tracks_per_group - 1

        s_idx = int(s_idxs[a])
        s0 = g0 + s_idx * subrange
        s1 = min(s0 + subrange - 1, g1)

        key = (g0, g1, s0, s1)
        result.append((key, chunk[a:b]))

    return result


def repack_tracksorted_dataset(
    dataset_path: str | Path,
    out_path: str | Path,
    *,
    poi_json_path: str | Path | None = None,
    config: TrackSortedConfig | None = None,
    overwrite: bool = False,
    show_progress: bool = True,
) -> Path:
    config = config or TrackSortedConfig()

    poi_track_ids: set[int] | None = None
    if config.use_poi_filter:
        if poi_json_path is None:
            raise ValueError("poi_json_path is required when use_poi_filter=True")
        poi_track_ids = load_poi_track_ids(
            poi_json_path,
            show_progress=show_progress,
        )

    with open_dataset(dataset_path, "r") as ds:
        src_any_pos = find_any_positions_dataset(ds)
        src_dtype = src_any_pos.dtype
        ts_dtype = make_tsorted_dtype(src_dtype, config.tsort_fields)

        out = create_tsorted_output(
            src=ds,
            out_path=out_path,
            ts_dtype=ts_dtype,
            copy_original_positions=config.copy_original_positions,
            overwrite=overwrite,
        )

        ds_cache: dict[str, h5py.Dataset] = {}
        buffers: dict[tuple[int, int, int, int], list[np.ndarray]] = defaultdict(list)
        buffer_sizes: dict[tuple[int, int, int, int], int] = defaultdict(int)

        try:
            pos_root = ds["positions"]
            years = sorted(pos_root.keys())

            total_days = 0
            for y in years:
                gy = pos_root[y]
                if not isinstance(gy, h5py.Group):
                    continue
                for m in gy.keys():
                    gm = gy[m]
                    if not isinstance(gm, h5py.Group):
                        continue
                    total_days += len(gm.keys())

            out_tracks_root = out["positions"]["tracks"]

            p_days = None
            if show_progress:
                p_days = tqdm(total=total_days, desc="Repack days", unit="day", **TQDM_KW)

            rows_read_total = 0
            rows_after_filter_total = 0
            rows_written_total = 0

            for y in years:
                gy = pos_root[y]
                if not isinstance(gy, h5py.Group):
                    continue

                months = sorted(gy.keys())
                for m in months:
                    gm = gy[m]
                    if not isinstance(gm, h5py.Group):
                        continue

                    days = sorted(gm.keys())
                    for d in days:
                        day_ds = gm[d]
                        if not isinstance(day_ds, h5py.Dataset):
                            continue

                        n = int(day_ds.shape[0])

                        if p_days is not None:
                            p_days.set_postfix_str(f"{y}/{m}/{d} rows={n}")

                        if n == 0:
                            if p_days is not None:
                                p_days.update(1)
                            continue

                        for start in range(0, n, config.read_chunk_rows):
                            end = min(start + config.read_chunk_rows, n)
                            chunk = day_ds[start:end]
                            rows_read_total += int(chunk.shape[0])

                            chunk2 = subset_fields(
                                chunk=chunk,
                                ts_dtype=ts_dtype,
                                tsort_fields=config.tsort_fields,
                            )

                            chunk2 = filter_chunk_by_poi(chunk2, poi_track_ids)
                            if chunk2.shape[0] == 0:
                                continue

                            rows_after_filter_total += int(chunk2.shape[0])

                            groups = split_chunk_into_subranges(
                                chunk2,
                                tracks_per_group=config.tracks_per_group,
                                datasets_per_group=config.datasets_per_group,
                            )

                            for key, part in groups:
                                buffers[key].append(part)
                                buffer_sizes[key] += int(part.shape[0])

                                if buffer_sizes[key] >= config.flush_threshold_rows:
                                    rows_written_total += flush_one_buffer(
                                        out_tracks_root=out_tracks_root,
                                        ds_cache=ds_cache,
                                        buffers=buffers,
                                        buffer_sizes=buffer_sizes,
                                        key=key,
                                        ts_dtype=ts_dtype,
                                    )

                        if p_days is not None:
                            p_days.update(1)

            if p_days is not None:
                p_days.close()

            rows_written_total += flush_all_buffers(
                out_tracks_root=out_tracks_root,
                ds_cache=ds_cache,
                buffers=buffers,
                buffer_sizes=buffer_sizes,
                ts_dtype=ts_dtype,
                show_progress=show_progress,
            )

            out.attrs["tsorted_tracks_per_group"] = int(config.tracks_per_group)
            out.attrs["tsorted_datasets_per_group"] = int(config.datasets_per_group)
            out.attrs["tsorted_read_chunk_rows"] = int(config.read_chunk_rows)
            out.attrs["tsorted_flush_threshold_rows"] = int(config.flush_threshold_rows)
            out.attrs["tsorted_copy_original_positions"] = int(config.copy_original_positions)
            out.attrs["tsorted_use_poi_filter"] = int(config.use_poi_filter)
            out.attrs["tsorted_fields"] = json.dumps(list(config.tsort_fields), ensure_ascii=False)

            if poi_track_ids is not None:
                out.attrs["tsorted_poi_tracks_count"] = int(len(poi_track_ids))

            out.attrs["tsorted_rows_read"] = int(rows_read_total)
            out.attrs["tsorted_rows_after_filter"] = int(rows_after_filter_total)
            out.attrs["tsorted_rows_written"] = int(rows_written_total)

            out.flush()

        finally:
            for dset in ds_cache.values():
                try:
                    dset.flush()
                except Exception:
                    pass
            out.close()

    return Path(out_path)