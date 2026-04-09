from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable
import hashlib
import json
import pickle

import h5py
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from mtlib.dataset.ds import open_dataset
from mtlib.dataset.tsorted import load_poi_track_ids

from .config import ArtifactBuildConfig
from .geo import haversine_m, split_track_into_segments, EARTH_M_PER_DEG_LAT


FRAGS_COLUMNS = [
    "track_id",
    "s",
    "e",
    "points",
    "t0",
    "t1",
    "min_lat",
    "min_lon",
    "max_lat",
    "max_lon",
]


def iter_track_datasets(root_group: h5py.Group):
    for g1_name in root_group.keys():
        g1 = root_group[g1_name]
        for dset_name in g1.keys():
            dset = g1[dset_name]
            full_path = f"{root_group.name}/{g1_name}/{dset_name}"
            yield full_path, dset


def build_track_index_for_ids(
    root_group: h5py.Group,
    target_ids: Iterable[int],
    *,
    chunk_rows: int = 1_500_000,
    show_progress: bool = True,
) -> dict[int, tuple[str, int, int]]:
    target_ids_set = set(int(x) for x in target_ids)
    index: dict[int, tuple[str, int, int]] = {}

    datasets = list(iter_track_datasets(root_group))
    for full_path, dset in tqdm(
        datasets,
        desc="Datasets in /positions/tracks",
        disable=not show_progress,
    ):
        n = int(dset.shape[0])
        if n == 0:
            continue

        for start in tqdm(
            range(0, n, chunk_rows),
            desc=f"scan {Path(full_path).name}",
            leave=False,
            disable=not show_progress,
        ):
            end = min(start + chunk_rows, n)
            chunk = dset[start:end]
            tids = chunk["track_id"].astype(np.int64)

            if len(tids) == 0:
                continue

            change = np.nonzero(tids[1:] != tids[:-1])[0] + 1
            bounds = np.concatenate(([0], change, [len(tids)]))

            for bi in range(len(bounds) - 1):
                s = int(bounds[bi])
                e = int(bounds[bi + 1])
                tid = int(tids[s])
                if tid not in target_ids_set:
                    continue

                abs_s = start + s
                abs_e = start + e
                prev = index.get(tid)

                if prev is None:
                    index[tid] = (full_path, abs_s, abs_e)
                else:
                    prev_path, prev_s, prev_e = prev
                    if prev_path == full_path:
                        index[tid] = (full_path, min(prev_s, abs_s), max(prev_e, abs_e))
                    else:
                        raise RuntimeError(
                            f"Track {tid} appeared in multiple datasets: {prev_path!r} and {full_path!r}"
                        )

    return index


def get_track_points_by_id(
    ds: h5py.File,
    track_index: dict[int, tuple[str, int, int]],
    track_id: int,
) -> np.ndarray | None:
    meta = track_index.get(int(track_id))
    if meta is None:
        return None

    full_path, s, e = meta
    dset = ds[full_path]
    arr = dset[s:e]

    if len(arr) > 0 and (
        int(arr["track_id"][0]) != int(track_id) or int(arr["track_id"][-1]) != int(track_id)
    ):
        arr = arr[arr["track_id"] == track_id]

    return arr


def build_density_from_positions_tracks(
    ds: h5py.File,
    extent: Iterable[float],
    *,
    cell_m: float = 1000.0,
    max_points: int | None = None,
    chunk_rows: int = 1_500_000,
    show_progress: bool = True,
) -> tuple[np.ndarray, tuple[float, float, float, float]]:
    min_lon, max_lon, min_lat, max_lat = map(float, extent)

    mean_lat = 0.5 * (min_lat + max_lat)
    m_per_deg_lat = EARTH_M_PER_DEG_LAT
    m_per_deg_lon = max(1e-6, EARTH_M_PER_DEG_LAT * np.cos(np.deg2rad(mean_lat)))

    dlat = cell_m / m_per_deg_lat
    dlon = cell_m / m_per_deg_lon

    ny = int(np.ceil((max_lat - min_lat) / dlat))
    nx = int(np.ceil((max_lon - min_lon) / dlon))
    dens = np.zeros((ny, nx), dtype=np.uint32)

    root = ds["positions/tracks"]
    total_seen = 0

    group_names = list(root.keys())
    for g1_name in tqdm(group_names, desc="density groups", disable=not show_progress):
        g1 = root[g1_name]
        for dset_name in g1.keys():
            dset = g1[dset_name]
            n = int(dset.shape[0])
            if n == 0:
                continue

            for start in range(0, n, chunk_rows):
                end = min(start + chunk_rows, n)
                chunk = dset[start:end]

                lat = chunk["lat"].astype(np.float64)
                lon = chunk["lon"].astype(np.float64)

                ok = (lat >= min_lat) & (lat <= max_lat) & (lon >= min_lon) & (lon <= max_lon)
                if not np.any(ok):
                    continue

                lat = lat[ok]
                lon = lon[ok]

                ix = np.clip(((lon - min_lon) / dlon).astype(np.int32), 0, nx - 1)
                iy = np.clip(((lat - min_lat) / dlat).astype(np.int32), 0, ny - 1)

                np.add.at(dens, (iy, ix), 1)
                total_seen += len(lat)

                if max_points is not None and total_seen >= max_points:
                    return dens, (min_lon, min_lat, dlon, dlat)

    return dens, (min_lon, min_lat, dlon, dlat)


def select_good_track_ids_from_poi_json(
    poi_json_path: str | Path,
    *,
    gap_time_sec: int,
    gap_dist_m: float,
    top_k: int | None = None,
) -> tuple[pd.DataFrame, np.ndarray]:
    obj = json.loads(Path(poi_json_path).read_text(encoding="utf-8"))
    track_quality = obj.get("track_quality", {})

    rows = []
    rows_append = rows.append
    for k, v in track_quality.items():
        rows_append(
            (
                int(k),
                int(v.get("points_seen", 0)),
                int(v.get("max_gap_sec", 0)),
                float(v.get("max_gap_dist_m", 0.0)),
            )
        )

    qdf = pd.DataFrame(
        rows,
        columns=["track_id", "points_seen", "max_gap_sec", "max_gap_dist_m"],
    )
    qdf.sort_values("track_id", inplace=True)
    qdf.reset_index(drop=True, inplace=True)

    mask = (qdf["max_gap_sec"] <= gap_time_sec) & (qdf["max_gap_dist_m"] <= gap_dist_m)
    good = qdf.loc[mask].copy()
    good.sort_values(["points_seen", "track_id"], ascending=[False, True], inplace=True)
    if top_k is not None:
        good = good.head(top_k).copy()

    return qdf, good["track_id"].to_numpy(dtype=np.int64)


def build_fragments_table(
    ds: h5py.File,
    track_index: dict[int, tuple[str, int, int]],
    work_ids: Iterable[int],
    *,
    frag_gap_time_sec: int,
    frag_gap_dist_m: float,
    frag_min_points: int,
    frag_min_disp_m: float,
    show_progress: bool = True,
) -> pd.DataFrame:
    rows: list[tuple[int, int, int, int, int, int, float, float, float, float]] = []
    rows_append = rows.append

    for tid in tqdm(list(work_ids), desc="Fragmenting tracks", disable=not show_progress):
        pts = get_track_points_by_id(ds, track_index, int(tid))
        if pts is None or len(pts) == 0:
            continue

        segs = split_track_into_segments(pts, frag_gap_time_sec, frag_gap_dist_m)
        if not segs:
            continue

        for s, e in segs:
            ln = e - s
            if ln < frag_min_points:
                continue

            seg = pts[s:e]
            lat0 = float(seg["lat"][0])
            lon0 = float(seg["lon"][0])
            disp = haversine_m(lat0, lon0, seg["lat"], seg["lon"])
            max_disp = float(np.max(disp)) if len(seg) else 0.0
            if max_disp < frag_min_disp_m:
                continue

            rows_append(
                (
                    int(tid),
                    int(s),
                    int(e),
                    int(ln),
                    int(seg["timestamp"][0]),
                    int(seg["timestamp"][-1]),
                    float(np.min(seg["lat"])),
                    float(np.min(seg["lon"])),
                    float(np.max(seg["lat"])),
                    float(np.max(seg["lon"])),
                )
            )

    frags = pd.DataFrame(rows, columns=FRAGS_COLUMNS)
    if len(frags) == 0:
        return frags

    frags.sort_values(["track_id", "s", "e"], inplace=True)
    frags.reset_index(drop=True, inplace=True)
    return frags


def split_train_val(
    frags: pd.DataFrame,
    *,
    val_frac: float = 0.02,
    seed: int = 42,
) -> tuple[pd.DataFrame, pd.DataFrame, set[int]]:
    rng = np.random.default_rng(seed)
    track_ids = frags["track_id"].unique()
    rng.shuffle(track_ids)
    n_val = int(len(track_ids) * val_frac)
    val_set = set(int(x) for x in track_ids[:n_val])

    is_val = frags["track_id"].isin(val_set).to_numpy()
    tr = frags.loc[~is_val].reset_index(drop=True)
    va = frags.loc[is_val].reset_index(drop=True)
    return tr, va, val_set


def frags_quick_stats(frags: pd.DataFrame) -> dict[str, Any]:
    if len(frags) == 0:
        return {"fragments": 0}

    df = frags.copy()
    df["dur_s"] = df["t1"] - df["t0"]
    df["dlat"] = df["max_lat"] - df["min_lat"]
    df["dlon"] = df["max_lon"] - df["min_lon"]

    return {
        "fragments": int(len(df)),
        "points_percentiles": {
            str(k): float(v)
            for k, v in zip([10, 25, 50, 75, 90, 95, 99], np.percentile(df["points"], [10, 25, 50, 75, 90, 95, 99]))
        },
        "duration_percentiles_sec": {
            str(k): float(v)
            for k, v in zip([10, 25, 50, 75, 90, 95, 99], np.percentile(df["dur_s"], [10, 25, 50, 75, 90, 95, 99]))
        },
        "bbox_span_deg_percentiles": {
            "dlat_p50": float(np.percentile(df["dlat"], 50)),
            "dlat_p90": float(np.percentile(df["dlat"], 90)),
            "dlon_p50": float(np.percentile(df["dlon"], 50)),
            "dlon_p90": float(np.percentile(df["dlon"], 90)),
        },
    }


def dataset_fingerprint(ds: h5py.File, dataset_path: str | Path) -> tuple[str, dict[str, str]]:
    attrs = {k: str(ds.attrs[k]) for k in ds.attrs.keys()}
    payload = json.dumps(
        {"dataset_path": str(dataset_path), "attrs": attrs},
        sort_keys=True,
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest(), attrs


def save_density_npz(
    path: str | Path,
    density_map: np.ndarray,
    density_geo: tuple[float, float, float, float],
    global_extent: Iterable[float],
) -> Path:
    path = Path(path)
    np.savez_compressed(
        str(path),
        density_map=density_map,
        density_geo=np.array(density_geo, dtype=np.float64),
        global_extent=np.array(list(global_extent), dtype=np.float64),
    )
    return path


def load_density_npz(path: str | Path):
    z = np.load(str(path), allow_pickle=False)
    density_map = z["density_map"]
    density_geo = tuple(float(x) for x in z["density_geo"])
    global_extent = [float(x) for x in z["global_extent"]]
    return density_map, density_geo, global_extent


def save_frags(frags: pd.DataFrame, path: str | Path) -> Path:
    path = Path(path)
    try:
        frags.to_parquet(path, index=False)
        return path
    except Exception:
        csv_path = path.with_suffix(".csv")
        frags.to_csv(csv_path, index=False)
        return csv_path


def load_frags(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if path.exists():
        return pd.read_parquet(path)
    csv_path = path.with_suffix(".csv")
    if csv_path.exists():
        return pd.read_csv(csv_path)
    raise FileNotFoundError(f"No frags parquet/csv found near: {path}")


def save_track_index(path: str | Path, track_index: dict[int, tuple[str, int, int]]) -> Path:
    path = Path(path)
    with open(path, "wb") as f:
        pickle.dump(track_index, f, protocol=pickle.HIGHEST_PROTOCOL)
    return path


def load_track_index(path: str | Path) -> dict[int, tuple[str, int, int]]:
    with open(path, "rb") as f:
        return pickle.load(f)


def save_split(val_track_ids: Iterable[int], path: str | Path, *, seed: int, val_frac: float) -> Path:
    path = Path(path)
    obj = {
        "seed": int(seed),
        "val_frac": float(val_frac),
        "val_track_ids": sorted(int(x) for x in val_track_ids),
    }
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_split(path: str | Path) -> tuple[set[int], dict[str, Any]]:
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    return set(int(x) for x in obj["val_track_ids"]), obj


def save_meta(path: str | Path, meta: dict[str, Any]) -> Path:
    path = Path(path)
    path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def guess_poi_json_path(dataset_path: str | Path) -> Path:
    dataset_path = Path(dataset_path)
    return dataset_path.with_name(dataset_path.stem.replace("_tsorted", "") + "_poi.json")


def build_training_artifacts(
    dataset_path: str | Path,
    out_dir: str | Path,
    *,
    config: ArtifactBuildConfig | None = None,
    poi_json_path: str | Path | None = None,
    global_extent: Iterable[float] = (105.0, 171.0, 17.0, 60.0),
    overwrite: bool = False,
    show_progress: bool = True,
) -> dict[str, Path]:
    config = config or ArtifactBuildConfig()
    dataset_path = Path(dataset_path)
    out_dir = Path(out_dir)

    if out_dir.exists() and any(out_dir.iterdir()) and not overwrite:
        raise FileExistsError(f"Output directory already exists and is not empty: {out_dir}")

    out_dir.mkdir(parents=True, exist_ok=True)
    poi_json_path = Path(poi_json_path) if poi_json_path is not None else guess_poi_json_path(dataset_path)

    density_path = out_dir / "density.npz"
    frags_path = out_dir / "frags.parquet"
    train_frags_path = out_dir / "frags_train.parquet"
    val_frags_path = out_dir / "frags_val.parquet"
    track_index_path = out_dir / "track_index.pkl"
    split_path = out_dir / "split.json"
    config_path = out_dir / "config.json"
    meta_path = out_dir / "meta.json"

    with open_dataset(dataset_path, "r") as ds:
        qdf, good_track_ids = select_good_track_ids_from_poi_json(
            poi_json_path,
            gap_time_sec=config.gap_time_sec,
            gap_dist_m=config.gap_dist_m,
            top_k=config.good_tracks_target,
        )

        track_index = build_track_index_for_ids(
            ds["positions/tracks"],
            good_track_ids,
            chunk_rows=config.h5_chunk_rows,
            show_progress=show_progress,
        )

        work_ids = np.array([tid for tid in good_track_ids if int(tid) in track_index], dtype=np.int64)

        density_map, density_geo = build_density_from_positions_tracks(
            ds,
            global_extent,
            cell_m=config.grid.density_cell_m,
            max_points=config.density_max_points,
            chunk_rows=config.density_chunk_rows,
            show_progress=show_progress,
        )

        frags = build_fragments_table(
            ds,
            track_index,
            work_ids,
            frag_gap_time_sec=config.frag_gap_time_sec,
            frag_gap_dist_m=config.frag_gap_dist_m,
            frag_min_points=config.frag_min_points,
            frag_min_disp_m=config.frag_min_disp_m,
            show_progress=show_progress,
        )

        train_frags, val_frags, val_track_ids = split_train_val(
            frags,
            val_frac=config.val_frac,
            seed=config.split_seed,
        )

        fp, attrs = dataset_fingerprint(ds, dataset_path)

    save_density_npz(density_path, density_map, density_geo, global_extent)
    frags_real_path = save_frags(frags, frags_path)
    train_frags_real_path = save_frags(train_frags, train_frags_path)
    val_frags_real_path = save_frags(val_frags, val_frags_path)
    save_track_index(track_index_path, track_index)
    save_split(val_track_ids, split_path, seed=config.split_seed, val_frac=config.val_frac)
    config.save_json(config_path)

    meta = {
        "dataset_path": str(dataset_path),
        "poi_json_path": str(poi_json_path),
        "dataset_fingerprint": fp,
        "dataset_attrs": attrs,
        "global_extent": list(map(float, global_extent)),
        "density_shape": [int(x) for x in density_map.shape],
        "density_geo": [float(x) for x in density_geo],
        "qdf_size": int(len(qdf)),
        "good_track_ids_size": int(len(good_track_ids)),
        "track_index_size": int(len(track_index)),
        "work_ids_size": int(len(work_ids)),
        "frags_size": int(len(frags)),
        "train_frags_size": int(len(train_frags)),
        "val_frags_size": int(len(val_frags)),
        "frags_stats": frags_quick_stats(frags),
        "files": {
            "density": str(density_path.name),
            "frags": str(frags_real_path.name),
            "frags_train": str(train_frags_real_path.name),
            "frags_val": str(val_frags_real_path.name),
            "track_index": str(track_index_path.name),
            "split": str(split_path.name),
            "config": str(config_path.name),
        },
    }
    save_meta(meta_path, meta)

    return {
        "density": density_path,
        "frags": frags_real_path,
        "frags_train": train_frags_real_path,
        "frags_val": val_frags_real_path,
        "track_index": track_index_path,
        "split": split_path,
        "config": config_path,
        "meta": meta_path,
    }
