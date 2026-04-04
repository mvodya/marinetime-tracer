from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import h5py
import numpy as np
from tqdm.auto import tqdm

from .ds import open_dataset, iter_day_datasets, ensure_group


@dataclass(slots=True)
class TrackDetectionConfig:
    speed_moving_min: int = 10
    stop_radius_m: float = 250.0
    stop_dwell_sec: int = 30 * 60

    gap_hard_sec: int = 5 * 3600
    gap_very_hard_sec: int = 10 * 3600
    dist_after_gap_m: float = 150_000.0

    jump_hard_m: float = 250_000.0

    dest_gap_sec: int = 4 * 3600
    dest_dist_m: float = 50_000.0

    idle_track_id: int = -1
    chunk_rows: int = 2_000_000


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p = math.pi / 180.0
    dlat = (lat2 - lat1) * p
    dlon = (lon2 - lon1) * p
    a = (
        math.sin(dlat * 0.5) ** 2
        + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin(dlon * 0.5) ** 2
    )
    c = 2.0 * math.asin(min(1.0, math.sqrt(a)))
    return r * c


def decode_bytes(value):
    if isinstance(value, (bytes, np.bytes_)):
        return bytes(value)
    return value


def append_track_row(dst_tracks: h5py.Dataset, row: np.void) -> None:
    n = dst_tracks.shape[0]
    dst_tracks.resize((n + 1,))
    dst_tracks[n] = row


def set_idle(st: dict, ts: int, lat: float, lon: float, dest) -> None:
    st["idle"] = True
    st["idle_since_ts"] = int(ts)
    st["idle_lat"] = float(lat)
    st["idle_lon"] = float(lon)
    st["idle_dest"] = dest

    st["stop_active"] = True
    st["stop_anchor_lat"] = float(lat)
    st["stop_anchor_lon"] = float(lon)
    st["stop_accum_sec"] = 0


def idle_should_start(st: dict, ts: int, lat: float, lon: float, speed: int, dest, config: TrackDetectionConfig) -> bool:
    sp = int(speed)
    moved_far = haversine_m(st["idle_lat"], st["idle_lon"], float(lat), float(lon)) > config.stop_radius_m
    dest_changed = dest != st.get("idle_dest", None)
    return (
        (sp >= config.speed_moving_min and moved_far)
        or (sp >= config.speed_moving_min and dest_changed)
        or (moved_far and dest_changed)
    )


def start_new_track(
    st: dict,
    ship_id: int,
    ts: int,
    lat: float,
    lon: float,
    speed: int,
    course: int,
    dest,
    next_track_id: int,
    config: TrackDetectionConfig,
) -> int:
    st["idle"] = False
    st["track_id"] = next_track_id

    st["ship_id"] = ship_id
    st["start_ts"] = ts
    st["start_lat"] = float(lat)
    st["start_lon"] = float(lon)

    st["last_ts"] = ts
    st["last_lat"] = float(lat)
    st["last_lon"] = float(lon)
    st["last_speed"] = int(speed)
    st["last_course"] = int(course)
    st["last_dest"] = dest

    st["points_count"] = 1

    st["stop_accum_sec"] = 0
    st["stop_anchor_lat"] = float(lat)
    st["stop_anchor_lon"] = float(lon)
    st["stop_active"] = int(speed) < config.speed_moving_min
    return next_track_id + 1


def close_track_if_any(dst_tracks: h5py.Dataset, st: dict) -> None:
    if st.get("idle", False):
        return
    if st.get("points_count", 0) < 2:
        return

    row = np.zeros((1,), dtype=dst_tracks.dtype)[0]
    row["track_id"] = st["track_id"]
    row["ship_id"] = st["ship_id"]
    row["start_timestamp"] = st["start_ts"]
    row["end_timestamp"] = st["last_ts"]
    row["start_lat"] = st["start_lat"]
    row["start_lon"] = st["start_lon"]
    row["end_lat"] = st["last_lat"]
    row["end_lon"] = st["last_lon"]
    row["points_count"] = st["points_count"]

    append_track_row(dst_tracks, row)


def need_new_track(st: dict, ts: int, lat: float, lon: float, speed: int, course: int, dest, config: TrackDetectionConfig) -> tuple[bool, int, float, bool]:
    last_ts = st["last_ts"]
    dt = int(ts) - int(last_ts)
    if dt < 0:
        dt = 0

    dist = haversine_m(st["last_lat"], st["last_lon"], float(lat), float(lon))
    last_dest = st["last_dest"]
    dest_changed = dest != last_dest

    if dt > config.gap_very_hard_sec:
        return True, dt, dist, dest_changed

    if dt > config.gap_hard_sec and dist > config.dist_after_gap_m:
        return True, dt, dist, dest_changed

    if dist > config.jump_hard_m:
        return True, dt, dist, dest_changed

    if dest_changed:
        if dt > config.dest_gap_sec or dist > config.dest_dist_m or st["stop_active"]:
            return True, dt, dist, dest_changed

    return False, dt, dist, dest_changed


def update_stop_logic(st: dict, dt: int, speed: int, lat: float, lon: float, config: TrackDetectionConfig) -> None:
    sp = int(speed)

    if sp < config.speed_moving_min:
        if not st["stop_active"]:
            st["stop_active"] = True
            st["stop_anchor_lat"] = float(lat)
            st["stop_anchor_lon"] = float(lon)
            st["stop_accum_sec"] = 0

        d_anchor = haversine_m(st["stop_anchor_lat"], st["stop_anchor_lon"], float(lat), float(lon))
        if d_anchor <= config.stop_radius_m:
            st["stop_accum_sec"] += int(dt)
        else:
            st["stop_anchor_lat"] = float(lat)
            st["stop_anchor_lon"] = float(lon)
            st["stop_accum_sec"] = 0
    else:
        st["stop_active"] = False
        st["stop_accum_sec"] = 0


def create_tracks_dataset_copy(src: h5py.File, dst_path: str | Path, config: TrackDetectionConfig, overwrite: bool = False) -> h5py.File:
    dst_path = Path(dst_path)

    if dst_path.exists():
        if overwrite:
            dst_path.unlink()
        else:
            raise FileExistsError(f"Output file already exists: {dst_path}")

    dst = h5py.File(dst_path, "w")

    for k, v in src.attrs.items():
        dst.attrs[k] = v

    dst.attrs["tracks_built_at"] = datetime.utcnow().isoformat()
    dst.attrs["tracks_rules"] = (
        f"SPEED_MOVING_MIN={config.speed_moving_min}, "
        f"STOP_RADIUS_M={config.stop_radius_m}, "
        f"STOP_DWELL_SEC={config.stop_dwell_sec}, "
        f"GAP_HARD_SEC={config.gap_hard_sec}, "
        f"GAP_VERY_HARD_SEC={config.gap_very_hard_sec}, "
        f"DIST_AFTER_GAP_M={config.dist_after_gap_m}, "
        f"JUMP_HARD_M={config.jump_hard_m}, "
        f"DEST_GAP_SEC={config.dest_gap_sec}, "
        f"DEST_DIST_M={config.dest_dist_m}"
    )

    dst.create_dataset("ships", data=src["ships"][:], dtype=src["ships"].dtype,
                       chunks=True, compression="gzip", compression_opts=4)

    dst.create_dataset("files", data=src["files"][:], dtype=src["files"].dtype,
                       chunks=True, compression="gzip", compression_opts=4)

    if "zones" in src:
        dst.create_dataset("zones", data=src["zones"][:], dtype=src["zones"].dtype,
                           chunks=True, compression="gzip", compression_opts=4)

    dst.create_dataset("tracks", shape=(0,), maxshape=(None,), dtype=src["tracks"].dtype,
                       chunks=True, compression="gzip", compression_opts=4)

    dst.create_group("positions")
    return dst


def process_day_for_tracks(
    src_day: h5py.Dataset,
    dst_day: h5py.Dataset,
    dst_tracks: h5py.Dataset,
    ship_state: dict[int, dict],
    next_track_id: int,
    config: TrackDetectionConfig,
    pbar=None,
) -> int:
    nrows = src_day.shape[0]

    for start in range(0, nrows, config.chunk_rows):
        end = min(nrows, start + config.chunk_rows)
        chunk = src_day[start:end]

        ship_ids = chunk["ship_id"]
        ts_arr = chunk["timestamp"]
        lat_arr = chunk["lat"]
        lon_arr = chunk["lon"]
        spd_arr = chunk["speed"]
        crs_arr = chunk["course"]
        dest_arr = chunk["destination"]

        out = chunk.copy()

        for i in range(out.shape[0]):
            ship_id = int(ship_ids[i])
            ts = int(ts_arr[i])
            lat = float(lat_arr[i])
            lon = float(lon_arr[i])
            speed = int(spd_arr[i])
            course = int(crs_arr[i])
            dest = decode_bytes(dest_arr[i])

            st = ship_state.get(ship_id)
            if st is None:
                st = {}
                ship_state[ship_id] = st
                next_track_id = start_new_track(st, ship_id, ts, lat, lon, speed, course, dest, next_track_id, config)
                out["track_id"][i] = st["track_id"]
                continue

            new_track, dt, dist, dest_changed = need_new_track(st, ts, lat, lon, speed, course, dest, config)
            update_stop_logic(st, dt, speed, lat, lon, config)
            stop_long_enough = st["stop_active"] and st["stop_accum_sec"] >= config.stop_dwell_sec

            if st.get("idle", False):
                if idle_should_start(st, ts, lat, lon, speed, dest, config):
                    next_track_id = start_new_track(st, ship_id, ts, lat, lon, speed, course, dest, next_track_id, config)
                    out["track_id"][i] = st["track_id"]
                else:
                    out["track_id"][i] = config.idle_track_id
                    update_stop_logic(st, dt, speed, lat, lon, config)
                    st["last_ts"] = ts
                    st["last_lat"] = lat
                    st["last_lon"] = lon
                    st["last_speed"] = speed
                    st["last_course"] = course
                    st["last_dest"] = dest
                continue

            if new_track:
                close_track_if_any(dst_tracks, st)
                next_track_id = start_new_track(st, ship_id, ts, lat, lon, speed, course, dest, next_track_id, config)
                out["track_id"][i] = st["track_id"]
                continue

            if stop_long_enough:
                close_track_if_any(dst_tracks, st)
                set_idle(st, ts, lat, lon, dest)
                out["track_id"][i] = config.idle_track_id

                st["last_ts"] = ts
                st["last_lat"] = lat
                st["last_lon"] = lon
                st["last_speed"] = speed
                st["last_course"] = course
                st["last_dest"] = dest
                st["points_count"] = 0
                continue

            out["track_id"][i] = st["track_id"]
            st["last_ts"] = ts
            st["last_lat"] = lat
            st["last_lon"] = lon
            st["last_speed"] = speed
            st["last_course"] = course
            st["last_dest"] = dest
            st["points_count"] += 1

        dst_day[start:end] = out

        if pbar is not None:
            pbar.update(end - start)

    return next_track_id


def detect_tracks(
    src_path: str | Path,
    dst_path: str | Path,
    *,
    config: TrackDetectionConfig | None = None,
    overwrite: bool = False,
    show_progress: bool = True,
) -> Path:
    config = config or TrackDetectionConfig()

    with open_dataset(src_path, "r") as src:
        dst = create_tracks_dataset_copy(src, dst_path, config, overwrite=overwrite)

        ship_state: dict[int, dict] = {}
        next_track_id = 1

        try:
            day_list = list(iter_day_datasets(src))
            total_positions = sum(ds.shape[0] for _, ds in day_list)

            pbar = tqdm(total=total_positions, unit="pos", desc="Build tracks") if show_progress else None

            for (yyyy, mm, dd), src_day in day_list:
                g = ensure_group(dst, f"positions/{yyyy}/{mm}")

                nrows = src_day.shape[0]
                dst_day = g.create_dataset(
                    dd,
                    shape=(nrows,),
                    maxshape=(nrows,),
                    dtype=src_day.dtype,
                    chunks=True,
                    compression="gzip",
                    compression_opts=4,
                )

                next_track_id = process_day_for_tracks(
                    src_day=src_day,
                    dst_day=dst_day,
                    dst_tracks=dst["tracks"],
                    ship_state=ship_state,
                    next_track_id=next_track_id,
                    config=config,
                    pbar=pbar,
                )

            if pbar is not None:
                pbar.close()

            for st in ship_state.values():
                close_track_if_any(dst["tracks"], st)

            dst.attrs["tracks_count"] = int(dst["tracks"].shape[0])

        finally:
            dst.flush()
            dst.close()

    return Path(dst_path)