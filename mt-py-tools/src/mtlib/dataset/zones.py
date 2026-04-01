from __future__ import annotations

import json
from pathlib import Path

import h5py
import numpy as np


zone_dtype = np.dtype([
    ("zone_id", "i8"),     # Уникальный идентификатор зоны
    ("name", "S256"),      # Название (описание) зоны
    ("lat", "f4"),         # Широта (WGS-84), в градусах
    ("lon", "f4"),         # Долгота (WGS-84), в градусах
    ("zoom", "i4")         # Уровень зума на карте Marine Traffic
])


def _to_bytes(value: str | bytes | None, default: str = "null") -> bytes:
    if value is None:
        value = default
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8", errors="ignore")


def load_zones_json(path: str | Path) -> list[dict]:
    path = Path(path)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def append_zones_to_hdf5(dataset_path: str | Path, zones: list[dict]) -> None:
    dataset_path = Path(dataset_path)

    rows = []
    for zone_id, zone in enumerate(zones):
        rows.append((
            zone_id,
            _to_bytes(zone["zone"]),
            float(zone["lat"]),
            float(zone["lon"]),
            int(zone["zoom"]),
        ))

    with h5py.File(dataset_path, "a") as f:
        if "zones" not in f:
            ds = f.create_dataset(
                "zones",
                shape=(0,),
                maxshape=(None,),
                dtype=zone_dtype,
                compression="gzip",
                chunks=True,
                compression_opts=4,
            )
        else:
            ds = f["zones"]

        old = ds.shape[0]
        ds.resize((old + len(rows),))
        ds[old:] = np.array(rows, dtype=zone_dtype)


def import_zones_from_json(json_path: str | Path, dataset_path: str | Path) -> None:
    zones = load_zones_json(json_path)
    append_zones_to_hdf5(dataset_path, zones)