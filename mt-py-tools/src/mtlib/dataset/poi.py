from __future__ import annotations

import json
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from tqdm.auto import tqdm

from .ds import iter_position_day_paths, open_dataset
from .tracks import haversine_m


@dataclass(slots=True)
class POIExtractionConfig:
    extent: tuple[float, float, float, float] = (105.0, 171.0, 17.0, 60.0)
    bins: tuple[int, int] = (1200, 900)

    threshold_mode: str = "percentile"
    threshold_value: float = 96.0
    min_cluster_cells: int = 3

    chunk_rows_tracks: int = 2_000_000
    chunk_rows_positions: int = 2_000_000

    min_track_points: int | None = None
    require_both_pois: bool = True
    top_destinations_per_poi: int = 10
    min_destination_len: int = 3


def build_start_end_heatmaps_tracks(
    ds_tracks,
    extent: tuple[float, float, float, float],
    bins: tuple[int, int] = (1200, 900),
    chunk_rows: int = 2_000_000,
    min_points: int | None = None,
):
    min_lon, max_lon, min_lat, max_lat = map(float, extent)
    nx, ny = map(int, bins)

    x_edges = np.linspace(min_lon, max_lon, nx + 1, dtype=np.float64)
    y_edges = np.linspace(min_lat, max_lat, ny + 1, dtype=np.float64)

    heat_start = np.zeros((ny, nx), dtype=np.uint32)
    heat_end = np.zeros((ny, nx), dtype=np.uint32)

    n = int(ds_tracks.shape[0])
    for s in tqdm(
        range(0, n, chunk_rows), desc="Pass1: tracks -> heatmaps", unit="chunk"
    ):
        e = min(n, s + chunk_rows)
        block = ds_tracks[s:e]

        mask = np.ones(block.shape[0], dtype=bool)
        if min_points is not None:
            mask &= block["points_count"] >= int(min_points)

        slat = block["start_lat"].astype(np.float64, copy=False)
        slon = block["start_lon"].astype(np.float64, copy=False)
        elat = block["end_lat"].astype(np.float64, copy=False)
        elon = block["end_lon"].astype(np.float64, copy=False)

        mask &= (
            np.isfinite(slat)
            & np.isfinite(slon)
            & np.isfinite(elat)
            & np.isfinite(elon)
        )
        if not np.any(mask):
            continue

        mask_start = (
            mask
            & (slon >= min_lon)
            & (slon <= max_lon)
            & (slat >= min_lat)
            & (slat <= max_lat)
        )
        if np.any(mask_start):
            hs, _, _ = np.histogram2d(
                slat[mask_start], slon[mask_start], bins=(y_edges, x_edges)
            )
            heat_start += hs.astype(np.uint32, copy=False)

        mask_end = (
            mask
            & (elon >= min_lon)
            & (elon <= max_lon)
            & (elat >= min_lat)
            & (elat <= max_lat)
        )
        if np.any(mask_end):
            he, _, _ = np.histogram2d(
                elat[mask_end], elon[mask_end], bins=(y_edges, x_edges)
            )
            heat_end += he.astype(np.uint32, copy=False)

    heat_total = heat_start + heat_end
    return heat_start, heat_end, heat_total, x_edges, y_edges


def make_dense_mask(
    heat_total: np.ndarray, mode: str = "percentile", value: float = 99.5
):
    nz = heat_total[heat_total > 0]
    if nz.size == 0:
        return np.zeros_like(heat_total, dtype=bool), 0.0

    if mode == "percentile":
        thr = float(np.percentile(nz, float(value)))
    elif mode == "absolute":
        thr = float(value)
    else:
        raise ValueError("mode must be 'percentile' or 'absolute'")

    mask = heat_total >= thr
    return mask, thr


def connected_components_8(mask: np.ndarray, min_cells: int = 3):
    """
    8-neighborhood only.
    Returns list[np.ndarray], each item shape=(k, 2) with [y, x].
    """
    h, w = mask.shape
    visited = np.zeros_like(mask, dtype=np.uint8)

    nbrs = [
        (-1, -1),
        (-1, 0),
        (-1, 1),
        (0, -1),
        (0, 1),
        (1, -1),
        (1, 0),
        (1, 1),
    ]

    clusters: list[np.ndarray] = []

    for y in tqdm(range(h), desc="Pass2: connected components rows", unit="row"):
        row = mask[y]
        if not row.any():
            continue

        for x in np.flatnonzero(row):
            if visited[y, x]:
                continue

            q = deque()
            q.append((y, x))
            visited[y, x] = 1
            cells: list[tuple[int, int]] = []

            while q:
                cy, cx = q.popleft()
                cells.append((cy, cx))

                for dy, dx in nbrs:
                    ny = cy + dy
                    nx = cx + dx
                    if (
                        0 <= ny < h
                        and 0 <= nx < w
                        and mask[ny, nx]
                        and not visited[ny, nx]
                    ):
                        visited[ny, nx] = 1
                        q.append((ny, nx))

            if len(cells) >= int(min_cells):
                clusters.append(np.asarray(cells, dtype=np.int32))

    return clusters


def cell_centers_from_edges(x_edges: np.ndarray, y_edges: np.ndarray):
    x_centers = (x_edges[:-1] + x_edges[1:]) * 0.5
    y_centers = (y_edges[:-1] + y_edges[1:]) * 0.5
    return x_centers, y_centers


def build_pois_from_clusters_no_sort(
    clusters,
    heat_start: np.ndarray,
    heat_end: np.ndarray,
    heat_total: np.ndarray,
    x_edges: np.ndarray,
    y_edges: np.ndarray,
):
    x_centers, y_centers = cell_centers_from_edges(x_edges, y_edges)

    pois = []
    for poi_id, cells in enumerate(clusters):
        ys = cells[:, 0]
        xs = cells[:, 1]

        w = heat_total[ys, xs].astype(np.float64, copy=False)
        wsum = float(w.sum()) if w.size else 0.0

        if wsum > 0.0:
            lon_c = float(np.sum(x_centers[xs] * w) / wsum)
            lat_c = float(np.sum(y_centers[ys] * w) / wsum)
        else:
            lon_c = float(np.mean(x_centers[xs]))
            lat_c = float(np.mean(y_centers[ys]))

        x0 = int(xs.min())
        x1 = int(xs.max()) + 1
        y0 = int(ys.min())
        y1 = int(ys.max()) + 1

        pois.append(
            {
                "poi_id": int(poi_id),
                "center_lat": lat_c,
                "center_lon": lon_c,
                "bbox": [
                    float(x_edges[x0]),
                    float(y_edges[y0]),
                    float(x_edges[x1]),
                    float(y_edges[y1]),
                ],
                "cells_count": int(cells.shape[0]),
                "count_start": int(heat_start[ys, xs].sum()),
                "count_end": int(heat_end[ys, xs].sum()),
                "count_total": int(heat_total[ys, xs].sum()),
                "top_destinations": [],
            }
        )

    return pois


def build_cell_to_poi(clusters, shape: tuple[int, int]) -> np.ndarray:
    ny, nx = shape
    cell_to_poi = np.full((ny, nx), -1, dtype=np.int32)

    for poi_id, cells in enumerate(clusters):
        ys = cells[:, 0]
        xs = cells[:, 1]
        cell_to_poi[ys, xs] = int(poi_id)

    return cell_to_poi


def point_to_cell(lat, lon, min_lon, min_lat, inv_dx, inv_dy, nx, ny):
    x = np.floor((lon - min_lon) * inv_dx).astype(np.int32)
    y = np.floor((lat - min_lat) * inv_dy).astype(np.int32)

    x = np.clip(x, 0, nx - 1)
    y = np.clip(y, 0, ny - 1)
    return y, x


def pass2_assign_pois_and_collect(
    ds_tracks,
    extent,
    cell_to_poi,
    chunk_rows: int = 2_000_000,
    min_points: int | None = None,
    require_both: bool = True,
):
    min_lon, max_lon, min_lat, max_lat = map(float, extent)
    ny, nx = cell_to_poi.shape

    dx = (max_lon - min_lon) / nx
    dy = (max_lat - min_lat) / ny
    inv_dx = 1.0 / dx
    inv_dy = 1.0 / dy

    track_to_pois: dict[int, tuple[int, int]] = {}

    poi_to_tracks_start = defaultdict(list)
    poi_to_tracks_end = defaultdict(list)
    poi_pair_to_tracks = defaultdict(list)

    min_ts = None
    max_ts = None

    n = int(ds_tracks.shape[0])
    for s in tqdm(
        range(0, n, chunk_rows), desc="Pass3: tracks -> POI assign", unit="chunk"
    ):
        e = min(n, s + chunk_rows)
        block = ds_tracks[s:e]

        mask = np.ones(block.shape[0], dtype=bool)
        if min_points is not None:
            mask &= block["points_count"] >= int(min_points)

        slat = block["start_lat"].astype(np.float64, copy=False)
        slon = block["start_lon"].astype(np.float64, copy=False)
        elat = block["end_lat"].astype(np.float64, copy=False)
        elon = block["end_lon"].astype(np.float64, copy=False)

        tids = block["track_id"].astype(np.int64, copy=False)
        sts = block["start_timestamp"].astype(np.int64, copy=False)
        ets = block["end_timestamp"].astype(np.int64, copy=False)

        mask &= (
            np.isfinite(slat)
            & np.isfinite(slon)
            & np.isfinite(elat)
            & np.isfinite(elon)
        )
        mask &= (
            (slon >= min_lon)
            & (slon <= max_lon)
            & (slat >= min_lat)
            & (slat <= max_lat)
        )
        mask &= (
            (elon >= min_lon)
            & (elon <= max_lon)
            & (elat >= min_lat)
            & (elat <= max_lat)
        )
        if not np.any(mask):
            continue

        part_tid = tids[mask]
        part_sts = sts[mask]
        part_ets = ets[mask]

        sy, sx = point_to_cell(
            slat[mask], slon[mask], min_lon, min_lat, inv_dx, inv_dy, nx, ny
        )
        ey, ex = point_to_cell(
            elat[mask], elon[mask], min_lon, min_lat, inv_dx, inv_dy, nx, ny
        )

        sp = cell_to_poi[sy, sx].astype(np.int32, copy=False)
        ep = cell_to_poi[ey, ex].astype(np.int32, copy=False)

        valid = np.ones(part_tid.shape[0], dtype=bool)
        if require_both:
            valid &= (sp >= 0) & (ep >= 0) & (sp != ep)

        if not np.any(valid):
            continue

        tids_v = part_tid[valid]
        sts_v = part_sts[valid]
        ets_v = part_ets[valid]
        sp_v = sp[valid]
        ep_v = ep[valid]

        if sts_v.size:
            cur_min = int(sts_v.min())
            cur_max = int(ets_v.max())
            min_ts = cur_min if min_ts is None else min(min_ts, cur_min)
            max_ts = cur_max if max_ts is None else max(max_ts, cur_max)

        for tid, a, b in zip(tids_v.tolist(), sp_v.tolist(), ep_v.tolist()):
            tid_i = int(tid)
            a_i = int(a)
            b_i = int(b)

            track_to_pois[tid_i] = (a_i, b_i)
            poi_to_tracks_start[a_i].append(tid_i)
            poi_to_tracks_end[b_i].append(tid_i)
            poi_pair_to_tracks[(a_i, b_i)].append(tid_i)

    return (
        track_to_pois,
        poi_to_tracks_start,
        poi_to_tracks_end,
        poi_pair_to_tracks,
        min_ts,
        max_ts,
    )


def collect_ts_lat_lon_tid_for_track_ids(
    ds,
    track_ids: np.ndarray,
    start_ts: int,
    end_ts: int,
    chunk_rows: int = 2_000_000,
):
    track_ids = np.unique(track_ids.astype(np.int64, copy=False))
    if track_ids.size == 0:
        return None

    day_paths = list(iter_position_day_paths(ds, start_ts, end_ts))
    if not day_paths:
        return None

    tid_parts = []
    ts_parts = []
    lat_parts = []
    lon_parts = []

    for path in tqdm(
        day_paths, desc="Pass4: positions scan (collect gaps)", unit="day"
    ):
        dset = ds[path]
        n = int(dset.shape[0])

        for s in tqdm(
            range(0, n, chunk_rows),
            desc=f"Chunks {path[-10:]}",
            leave=False,
            unit="chunk",
        ):
            e = min(s + chunk_rows, n)
            block = dset[s:e]

            mask = np.isin(block["track_id"], track_ids, assume_unique=False)
            if not np.any(mask):
                continue

            part = block[mask]
            tid_parts.append(part["track_id"].astype(np.int64, copy=False))
            ts_parts.append(part["timestamp"].astype(np.int64, copy=False))
            lat_parts.append(part["lat"].astype(np.float64, copy=False))
            lon_parts.append(part["lon"].astype(np.float64, copy=False))

    if not tid_parts:
        return None

    return {
        "tid": np.concatenate(tid_parts),
        "ts": np.concatenate(ts_parts),
        "lat": np.concatenate(lat_parts),
        "lon": np.concatenate(lon_parts),
    }


def compute_gap_metrics(
    track_ids_sorted: np.ndarray,
    tid_raw: np.ndarray,
    ts_raw: np.ndarray,
    lat_raw: np.ndarray,
    lon_raw: np.ndarray,
):
    order = np.lexsort((ts_raw, tid_raw))
    tid = tid_raw[order]
    ts = ts_raw[order]
    lat = lat_raw[order]
    lon = lon_raw[order]

    idx = np.searchsorted(track_ids_sorted, tid)
    n_tracks = track_ids_sorted.size

    points_seen = np.bincount(idx, minlength=n_tracks).astype(np.int32)

    max_gap_sec = np.zeros(n_tracks, dtype=np.int32)
    max_gap_dist_m = np.zeros(n_tracks, dtype=np.float32)

    if ts.size < 2:
        return points_seen, max_gap_sec, max_gap_dist_m

    same = idx[1:] == idx[:-1]
    if not np.any(same):
        return points_seen, max_gap_sec, max_gap_dist_m

    idx_same = idx[1:][same].astype(np.int32, copy=False)

    dt = ts[1:].astype(np.int64) - ts[:-1].astype(np.int64)
    dt_same = dt[same]
    dt_same = np.maximum(dt_same, 0)
    dt_same_i32 = np.minimum(dt_same, np.iinfo(np.int32).max).astype(
        np.int32, copy=False
    )

    np.maximum.at(max_gap_sec, idx_same, dt_same_i32)

    lat1 = lat[:-1][same].astype(np.float64, copy=False)
    lon1 = lon[:-1][same].astype(np.float64, copy=False)
    lat2 = lat[1:][same].astype(np.float64, copy=False)
    lon2 = lon[1:][same].astype(np.float64, copy=False)

    dist_m = haversine_m(lat1, lon1, lat2, lon2)
    np.maximum.at(max_gap_dist_m, idx_same, dist_m.astype(np.float32, copy=False))

    return points_seen, max_gap_sec, max_gap_dist_m


def decode_destination(arr) -> np.ndarray:
    out = np.empty(arr.shape[0], dtype=object)

    for i, b in enumerate(arr):
        if isinstance(b, (bytes, np.bytes_)):
            s = b.decode("utf-8", errors="replace").strip().strip("\x00")
        else:
            s = str(b).strip()
        out[i] = s

    return out


def collect_last_destination_per_track(
    ds,
    track_ids: np.ndarray,
    start_ts: int,
    end_ts: int,
    chunk_rows: int = 2_000_000,
):
    track_ids = np.unique(track_ids.astype(np.int64, copy=False))
    if track_ids.size == 0:
        return {}, {}

    last_ts: dict[int, int] = {}
    last_dest: dict[int, str] = {}

    day_paths = list(iter_position_day_paths(ds, start_ts, end_ts))
    if not day_paths:
        return last_ts, last_dest

    for path in tqdm(day_paths, desc="Pass5: positions scan (last dest)", unit="day"):
        dset = ds[path]
        n = int(dset.shape[0])

        for s in tqdm(
            range(0, n, chunk_rows),
            desc=f"Chunks {path[-10:]}",
            leave=False,
            unit="chunk",
        ):
            e = min(s + chunk_rows, n)
            block = dset[s:e]

            mask = np.isin(block["track_id"], track_ids, assume_unique=False)
            if not np.any(mask):
                continue

            part = block[mask]
            tid = part["track_id"].astype(np.int64, copy=False)
            ts = part["timestamp"].astype(np.int64, copy=False)
            dest = decode_destination(part["destination"])

            for t, tt, dd in zip(tid.tolist(), ts.tolist(), dest.tolist()):
                t = int(t)
                tt = int(tt)
                prev = last_ts.get(t)

                if prev is None or tt > prev:
                    last_ts[t] = tt
                    last_dest[t] = dd

    return last_ts, last_dest


def normalize_dest(s: str) -> str:
    if s is None:
        return ""

    s = str(s).strip().strip("\x00")
    if not s:
        return ""

    s = s.replace("_", " ").replace("-", " ")
    s = " ".join(s.split())
    return s.upper()


_BAD_DEST_EXACT = {
    "CLASS A",
    "CLASS B",
    "CLASS C",
    "UNKNOWN",
    "UNDEFINED",
    "NONE",
    "N/A",
    "NA",
}
_BAD_DEST_PREFIX = ("CLASS ",)


def is_bad_destination(norm_s: str) -> bool:
    if not norm_s:
        return True
    if norm_s in _BAD_DEST_EXACT:
        return True
    for prefix in _BAD_DEST_PREFIX:
        if norm_s.startswith(prefix):
            return True
    return False


def aggregate_destinations_by_poi(
    track_to_pois: dict, last_dest_map: dict, min_len: int = 3
):
    poi_counters = defaultdict(Counter)

    for tid, (_, end_poi) in track_to_pois.items():
        dest = normalize_dest(last_dest_map.get(int(tid), ""))
        if len(dest) < min_len:
            continue
        poi_counters[int(end_poi)][dest] += 1

    return poi_counters


def fill_top_destinations(
    pois: list[dict],
    poi_dest_counters,
    top_n: int = 10,
):
    for p in pois:
        pid = int(p["poi_id"])
        cnt = poi_dest_counters.get(pid)

        if not cnt:
            p["top_destinations"] = []
            continue

        good = [(name, c) for name, c in cnt.items() if not is_bad_destination(name)]
        bad = [(name, c) for name, c in cnt.items() if is_bad_destination(name)]

        good.sort(key=lambda x: x[1], reverse=True)
        bad.sort(key=lambda x: x[1], reverse=True)

        merged = good[:top_n]
        if len(merged) < top_n:
            merged += bad[: (top_n - len(merged))]

        p["top_destinations"] = [{"name": name, "count": int(c)} for name, c in merged]


def build_track_quality_dict(
    track_ids_sorted: np.ndarray,
    points_seen: np.ndarray,
    max_gap_sec: np.ndarray,
    max_gap_dist_m: np.ndarray,
):
    return {
        int(tid): {
            "points_seen": int(points_seen[i]),
            "max_gap_sec": int(max_gap_sec[i]),
            "max_gap_dist_m": float(max_gap_dist_m[i]),
        }
        for i, tid in enumerate(track_ids_sorted)
        if points_seen[i] > 0
    }


def extract_poi_data(
    dataset_path: str | Path,
    *,
    config: POIExtractionConfig | None = None,
):
    config = config or POIExtractionConfig()

    with open_dataset(dataset_path, "r") as ds:
        if "tracks" not in ds:
            raise KeyError("Dataset has no /tracks table")

        ds_tracks = ds["tracks"]

        heat_start, heat_end, heat_total, x_edges, y_edges = (
            build_start_end_heatmaps_tracks(
                ds_tracks=ds_tracks,
                extent=config.extent,
                bins=config.bins,
                chunk_rows=config.chunk_rows_tracks,
                min_points=config.min_track_points,
            )
        )

        dense_mask, dense_thr = make_dense_mask(
            heat_total=heat_total,
            mode=config.threshold_mode,
            value=config.threshold_value,
        )

        clusters = connected_components_8(
            mask=dense_mask,
            min_cells=config.min_cluster_cells,
        )

        pois = build_pois_from_clusters_no_sort(
            clusters=clusters,
            heat_start=heat_start,
            heat_end=heat_end,
            heat_total=heat_total,
            x_edges=x_edges,
            y_edges=y_edges,
        )

        cell_to_poi = build_cell_to_poi(clusters, heat_total.shape)

        (
            track_to_pois,
            poi_to_tracks_start,
            poi_to_tracks_end,
            poi_pair_to_tracks,
            poi_min_ts,
            poi_max_ts,
        ) = pass2_assign_pois_and_collect(
            ds_tracks=ds_tracks,
            extent=config.extent,
            cell_to_poi=cell_to_poi,
            chunk_rows=config.chunk_rows_tracks,
            min_points=config.min_track_points,
            require_both=config.require_both_pois,
        )

        track_ids_all = np.fromiter(
            track_to_pois.keys(), dtype=np.int64, count=len(track_to_pois)
        )

        track_quality = {}
        if track_ids_all.size > 0 and poi_min_ts is not None and poi_max_ts is not None:
            collected = collect_ts_lat_lon_tid_for_track_ids(
                ds=ds,
                track_ids=track_ids_all,
                start_ts=int(poi_min_ts),
                end_ts=int(poi_max_ts),
                chunk_rows=config.chunk_rows_positions,
            )

            if collected is not None:
                track_ids_sorted = np.sort(track_ids_all.astype(np.int64, copy=False))

                points_seen, max_gap_sec_arr, max_gap_dist_m_arr = compute_gap_metrics(
                    track_ids_sorted=track_ids_sorted,
                    tid_raw=collected["tid"],
                    ts_raw=collected["ts"],
                    lat_raw=collected["lat"],
                    lon_raw=collected["lon"],
                )

                track_quality = build_track_quality_dict(
                    track_ids_sorted=track_ids_sorted,
                    points_seen=points_seen,
                    max_gap_sec=max_gap_sec_arr,
                    max_gap_dist_m=max_gap_dist_m_arr,
                )

            _, last_dest_map = collect_last_destination_per_track(
                ds=ds,
                track_ids=track_ids_all,
                start_ts=int(poi_min_ts),
                end_ts=int(poi_max_ts),
                chunk_rows=config.chunk_rows_positions,
            )

            poi_dest_counters = aggregate_destinations_by_poi(
                track_to_pois=track_to_pois,
                last_dest_map=last_dest_map,
                min_len=config.min_destination_len,
            )

            fill_top_destinations(
                pois=pois,
                poi_dest_counters=poi_dest_counters,
                top_n=config.top_destinations_per_poi,
            )

        export = {
            "source_dataset": str(Path(dataset_path)),
            "extent": list(config.extent),
            "bins": list(config.bins),
            "threshold": {
                "mode": config.threshold_mode,
                "value": float(config.threshold_value),
                "dense_thr": float(dense_thr),
                "min_cluster_cells": int(config.min_cluster_cells),
            },
            "stats": {
                "pois_count": len(pois),
                "pairs_count": len(poi_pair_to_tracks),
                "tracks_count": len(track_to_pois),
            },
            "pois": pois,
            "pairs": {
                f"{a}->{b}": {
                    "start_poi": int(a),
                    "end_poi": int(b),
                    "tracks_count": len(tids),
                    "track_ids": [int(t) for t in tids],
                }
                for (a, b), tids in poi_pair_to_tracks.items()
            },
            "track_quality": track_quality,
        }

        debug = {
            "heat_start": heat_start,
            "heat_end": heat_end,
            "heat_total": heat_total,
            "dense_mask": dense_mask,
            "clusters": clusters,
            "cell_to_poi": cell_to_poi,
            "poi_to_tracks_start": poi_to_tracks_start,
            "poi_to_tracks_end": poi_to_tracks_end,
            "poi_pair_to_tracks": poi_pair_to_tracks,
            "track_to_pois": track_to_pois,
            "poi_min_ts": poi_min_ts,
            "poi_max_ts": poi_max_ts,
        }

        return export, debug


def save_poi_json(
    export: dict,
    output_path: str | Path,
) -> Path:
    output_path = Path(output_path)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2)

    return output_path


def extract_poi_to_json(
    dataset_path: str | Path,
    output_path: str | Path | None = None,
    *,
    config: POIExtractionConfig | None = None,
) -> Path:
    dataset_path = Path(dataset_path)

    if output_path is None:
        output_path = dataset_path.with_name(dataset_path.stem + "_poi.json")
    else:
        output_path = Path(output_path)

    export, _ = extract_poi_data(
        dataset_path=dataset_path,
        config=config,
    )

    return save_poi_json(export, output_path)
