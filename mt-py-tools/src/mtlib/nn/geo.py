from __future__ import annotations

from typing import Iterable

import numpy as np

from .config import GridConfig
from ..dataset.tracks import haversine_m

EARTH_M_PER_DEG_LAT = 111_320.0


def compute_gaps_for_track(points: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    ts = points["timestamp"].astype(np.int64)
    lat = points["lat"].astype(np.float64)
    lon = points["lon"].astype(np.float64)
    dt = np.diff(ts)
    dist = haversine_m(lat[:-1], lon[:-1], lat[1:], lon[1:])
    return dt, dist


def split_track_into_segments(
    points: np.ndarray,
    gap_time_sec: int,
    gap_dist_m: float,
) -> list[tuple[int, int]]:
    n = len(points)
    if n == 0:
        return []
    if n == 1:
        return [(0, 1)]

    dt, dist = compute_gaps_for_track(points)
    cut = np.nonzero((dt > gap_time_sec) | (dist > gap_dist_m))[0] + 1
    bounds = np.concatenate(([0], cut, [n]))

    segs: list[tuple[int, int]] = []
    for i in range(len(bounds) - 1):
        s = int(bounds[i])
        e = int(bounds[i + 1])
        if e > s:
            segs.append((s, e))
    return segs


def crop_resample_map(
    src: np.ndarray,
    src_geo: tuple[float, float, float, float],
    dst_extent: Iterable[float],
    dst_size: tuple[int, int],
    mode: str = "nearest",
) -> np.ndarray:
    src_min_lon, src_min_lat, src_dlon, src_dlat = src_geo
    dst_min_lon, dst_max_lon, dst_min_lat, dst_max_lat = map(float, dst_extent)
    h, w = dst_size

    ys = dst_min_lat + (np.arange(h) + 0.5) * ((dst_max_lat - dst_min_lat) / h)
    xs = dst_min_lon + (np.arange(w) + 0.5) * ((dst_max_lon - dst_min_lon) / w)

    ix_f = np.clip((xs - src_min_lon) / src_dlon, 0, src.shape[1] - 1)
    iy_f = np.clip((ys - src_min_lat) / src_dlat, 0, src.shape[0] - 1)

    if mode == "nearest":
        ix = np.rint(ix_f).astype(np.int32)
        iy = np.rint(iy_f).astype(np.int32)
        return src[iy[:, None], ix[None, :]]

    if mode == "bilinear":
        ix0 = np.floor(ix_f).astype(np.int32)
        iy0 = np.floor(iy_f).astype(np.int32)
        ix1 = np.clip(ix0 + 1, 0, src.shape[1] - 1)
        iy1 = np.clip(iy0 + 1, 0, src.shape[0] - 1)

        wx = (ix_f - ix0).astype(np.float32)
        wy = (iy_f - iy0).astype(np.float32)

        a = src[iy0[:, None], ix0[None, :]].astype(np.float32)
        b = src[iy0[:, None], ix1[None, :]].astype(np.float32)
        c = src[iy1[:, None], ix0[None, :]].astype(np.float32)
        d = src[iy1[:, None], ix1[None, :]].astype(np.float32)

        return (
            a * (1 - wx)[None, :] * (1 - wy)[:, None]
            + b * wx[None, :] * (1 - wy)[:, None]
            + c * (1 - wx)[None, :] * wy[:, None]
            + d * wx[None, :] * wy[:, None]
        )

    raise ValueError("mode must be 'nearest' or 'bilinear'")


def window_for_fragment(points: np.ndarray, cfg: GridConfig, pad_cells: int = 32) -> list[float]:
    lats = points["lat"].astype(np.float64)
    lons = points["lon"].astype(np.float64)

    c_lat = 0.5 * (float(lats.min()) + float(lats.max()))
    c_lon = 0.5 * (float(lons.min()) + float(lons.max()))

    m_per_deg_lat = EARTH_M_PER_DEG_LAT
    m_per_deg_lon = max(1e-6, EARTH_M_PER_DEG_LAT * np.cos(np.deg2rad(c_lat)))

    grid_m = cfg.grid_size * cfg.cell_m
    half_m = 0.5 * grid_m
    pad_m = pad_cells * cfg.cell_m

    lat_span_m = (float(lats.max()) - float(lats.min())) * m_per_deg_lat
    lon_span_m = (float(lons.max()) - float(lons.min())) * m_per_deg_lon
    half_bbox_m = 0.5 * max(lat_span_m, lon_span_m)
    half_m = max(half_m, half_bbox_m)

    half_dlat = (half_m + pad_m) / m_per_deg_lat
    half_dlon = (half_m + pad_m) / m_per_deg_lon
    return [c_lon - half_dlon, c_lon + half_dlon, c_lat - half_dlat, c_lat + half_dlat]


def make_pos_channels(cfg: GridConfig) -> tuple[np.ndarray, np.ndarray]:
    h = w = cfg.grid_size
    xs = (np.arange(w) + 0.5) / w
    ys = (np.arange(h) + 0.5) / h
    pos_x = np.broadcast_to(xs[None, :], (h, w)).astype(np.float32)
    pos_y = np.broadcast_to(ys[:, None], (h, w)).astype(np.float32)
    return pos_x, pos_y


def sample_gaps(n_points: int, cfg: GridConfig, rng: np.random.Generator) -> list[tuple[int, int]]:
    k = int(rng.integers(cfg.gaps_count_min, cfg.gaps_count_max + 1))
    gaps: list[tuple[int, int]] = []

    left = cfg.n_anchor
    right = n_points - cfg.n_anchor
    if right - left <= cfg.gaps_min_points + 5:
        return gaps

    for _ in range(k):
        gap_len = int(rng.integers(cfg.gaps_min_points, cfg.gaps_max_points + 1))
        s = int(rng.integers(left, max(left + 1, right - gap_len)))
        e = min(s + gap_len, right)
        gaps.append((s, e))

    gaps.sort()
    merged: list[list[int]] = []
    for s, e in gaps:
        if not merged or s > merged[-1][1]:
            merged.append([s, e])
        else:
            merged[-1][1] = max(merged[-1][1], e)

    return [(int(a), int(b)) for a, b in merged]
