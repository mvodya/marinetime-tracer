from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

import h5py
import numpy as np
from tqdm import tqdm

F_VER = "1.0"

positions_dtype = np.dtype(
    [
        ("ship_id", "i8"),  # Внутренний идентификатор судна
        ("timestamp", "i4"),  # Время отчета
        ("lat", "f4"),  # Широта (WGS-84), в градусах
        ("lon", "f4"),  # Долгота (WGS-84), в градусах
        ("speed", "i4"),  # Скорость над грунтом (Speed over ground) в узлах
        ("course", "i4"),  # Курс (Course over ground) (в градусах)
        ("heading", "i4"),  # Направление носа судна (Heading), целое (в градусах)
        ("rot", "i4"),  # Rate of Turn (изменение курса), в AIS шкале (-127..127)
        ("elapsed", "i4"),  # Время с последнего отчета (секунды)
        (
            "destination",
            "S64",
        ),  # Указанный порт/пункт назначения (текст, неформализован)
        ("tile_z", "i4"),  # Уровень зума тайла Marine Traffic
        ("file_id", "i4"),  # Индекс файла в таблице `/files`, откуда поступила запись
        ("track_id", "i8"),  # Идентификатор трека (маршрута)
    ]
)

ships_dtype = np.dtype(
    [
        ("ship_id", "i8"),  # Внутренний уникальный идентификатор
        ("mt_id", "S128"),  # Marinetraffic ID
        ("name", "S128"),  # Название судна
        ("flag", "S4"),  # ISO-код страны флага (например, "RU", "CN")
        ("ship_type", "i4"),  # AIS raw ship type
        ("gt_ship_type", "i4"),  # Нормализованный/кластеризованный тип судна
        ("length", "i4"),  # Длина судна (в метрах)
        ("width", "i4"),  # Ширина судна (в метрах)
        ("dwt", "i4"),  # Deadweight tonnage - дедвейт, тоннаж
    ]
)

tracks_dtype = np.dtype(
    [
        ("track_id", "i8"),  # Уникальный ID трека
        ("ship_id", "i8"),  # Идентификатор судна
        ("start_timestamp", "i4"),  # Время начала трека
        ("end_timestamp", "i4"),  # Время окончания трека
        ("start_lat", "f4"),  # Координаты начальной точки трека
        ("start_lon", "f4"),
        ("end_lat", "f4"),  # Координаты финальной точки трека
        ("end_lon", "f4"),
        ("points_count", "i4"),  # Количество точек в треке
    ]
)

files_dtype = np.dtype(
    [
        ("file_id", "i4"),  # Уникальный идентификатор файла
        ("name", "S256"),  # Имя файла
        ("positions_count", "i4"),  # Кол-во записей в файле
        ("timestamp", "i4"),  # Время парсинга
    ]
)


def _to_bytes(value: str | bytes | None, default: str = "null") -> bytes:
    if value is None:
        value = default
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8", errors="ignore")


def safe_int(value, default: int = -1) -> int:
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def parse_required_timestamp(value) -> int | None:
    try:
        if value is None:
            return None
        ts = int(value)
    except (ValueError, TypeError):
        return None

    return ts if ts > 0 else None


def get_folder_stats(path: str | Path) -> tuple[float, int]:
    path = Path(path)
    total_size = 0
    file_count = 0

    for dirpath, _, filenames in os.walk(path):
        for filename in filenames:
            try:
                fp = Path(dirpath) / filename
                if fp.is_file():
                    total_size += fp.stat().st_size
                    file_count += 1
            except OSError:
                continue

    size_gb = total_size / (1024**3)
    return size_gb, file_count


def get_json_files_by_date_range(
    root_dir: str | Path,
    start_date: str,
    end_date: str,
    date_format: str = "%d.%m.%Y",
) -> list[Path]:
    root_dir = Path(root_dir)
    start = datetime.strptime(start_date, date_format)
    end = datetime.strptime(end_date, date_format)

    result: list[Path] = []
    current = start

    while current <= end:
        dir_path = (
            root_dir
            / f"{current.year:04d}"
            / f"{current.month:02d}"
            / f"{current.day:02d}"
        )
        if dir_path.exists():
            result.extend(sorted(dir_path.glob("*.json")))
        current += timedelta(days=1)

    return result


def create_empty_hdf5(
    output_path: str | Path,
    *,
    source_path: str | Path | None = None,
    author: str = "Mark Vodyanitskiy (mvodya@icloud.com)",
    version: str = F_VER,
    overwrite: bool = False,
) -> Path:
    output_path = Path(output_path)

    if output_path.exists() and not overwrite:
        raise FileExistsError(f"HDF5 file already exists: {output_path}")

    sources_count = -1
    sources_size_gb = -1.0
    if source_path is not None:
        sources_size_gb, sources_count = get_folder_stats(source_path)

    with h5py.File(output_path, "w") as h5:
        h5.attrs["created_at"] = datetime.utcnow().isoformat()
        h5.attrs["version"] = version
        h5.attrs["author"] = author
        h5.attrs["sources_count"] = sources_count
        h5.attrs["sources_size"] = (
            f"{sources_size_gb:.3f} GB" if sources_size_gb >= 0 else "unknown"
        )

        h5.create_dataset(
            "ships",
            shape=(0,),
            maxshape=(None,),
            dtype=ships_dtype,
            compression="gzip",
            compression_opts=4,
            chunks=True,
        )

        h5.create_dataset(
            "files",
            shape=(0,),
            maxshape=(None,),
            dtype=files_dtype,
            compression="gzip",
            compression_opts=4,
            chunks=True,
        )

        h5.create_dataset(
            "tracks",
            shape=(0,),
            maxshape=(None,),
            dtype=tracks_dtype,
            compression="gzip",
            compression_opts=4,
            chunks=True,
        )

    return output_path


def _flush_batches(
    h5file: h5py.File,
    ships_batch: list[tuple],
    positions_batch: list[tuple],
    files_batch: list[tuple],
) -> None:
    if ships_batch:
        ds = h5file["ships"]
        old = ds.shape[0]
        ds.resize((old + len(ships_batch),))
        ds[old:] = np.array(ships_batch, dtype=ships_dtype)
        ships_batch.clear()

    if positions_batch:
        grouped: dict[tuple[int, int, int], list[tuple]] = defaultdict(list)

        for row in positions_batch:
            ts = int(row[1])
            dt = datetime.utcfromtimestamp(ts)
            grouped[(dt.year, dt.month, dt.day)].append(row)

        for (year, month, day), rows in grouped.items():
            path = f"positions/{year:04d}/{month:02d}"
            name = f"{day:02d}"
            group = h5file.require_group(path)

            if name not in group:
                ds = group.create_dataset(
                    name,
                    shape=(0,),
                    maxshape=(None,),
                    dtype=positions_dtype,
                    chunks=True,
                    compression="gzip",
                    compression_opts=4,
                )
            else:
                ds = group[name]

            old = ds.shape[0]
            ds.resize((old + len(rows),))
            ds[old:] = np.array(rows, dtype=positions_dtype)

        positions_batch.clear()

    if files_batch:
        ds = h5file["files"]
        old = ds.shape[0]
        ds.resize((old + len(files_batch),))
        ds[old:] = np.array(files_batch, dtype=files_dtype)
        files_batch.clear()

    h5file.flush()


def build_hdf5_from_archive(
    source_path: str | Path,
    output_path: str | Path,
    *,
    start_date: str,
    end_date: str,
    flush_every: int = 50,
    overwrite: bool = False,
    show_progress: bool = True,
) -> Path:
    source_path = Path(source_path)
    output_path = Path(output_path)

    create_empty_hdf5(
        output_path,
        source_path=source_path,
        overwrite=overwrite,
    )

    files = get_json_files_by_date_range(source_path, start_date, end_date)

    mt_id_storage: dict[str, int] = {}
    next_ship_id = 1
    file_id = 0

    ships_batch: list[tuple] = []
    positions_batch: list[tuple] = []
    files_batch: list[tuple] = []

    iterator: Iterable[Path]
    iterator = tqdm(files, desc="Parsing JSON files") if show_progress else files

    with h5py.File(output_path, "a") as h5file:
        skipped_invalid_timestamp = 0
        for file_path in iterator:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            records_count = len(data)
            files_batch.append(
                (
                    file_id,
                    _to_bytes(file_path.name, default=""),
                    records_count,
                    int(time.time()),
                )
            )

            for mt_id, record in data.items():
                if mt_id not in mt_id_storage:
                    mt_id_storage[mt_id] = next_ship_id
                    next_ship_id += 1

                    ships_batch.append(
                        (
                            mt_id_storage[mt_id],
                            _to_bytes(mt_id, default=""),
                            _to_bytes(record.get("SHIPNAME", "null")),
                            _to_bytes(record.get("FLAG", "null")),
                            safe_int(record.get("SHIPTYPE", -1)),
                            safe_int(record.get("GT_SHIPTYPE", -1)),
                            safe_int(record.get("LENGTH", -1)),
                            safe_int(record.get("WIDTH", -1)),
                            safe_int(record.get("DWT", -1)),
                        )
                    )

                ship_id = mt_id_storage[mt_id]
                timestamp = parse_required_timestamp(record.get("TIMESTAMP"))
                if timestamp is None:
                    skipped_invalid_timestamp += 1
                    continue

                positions_batch.append(
                    (
                        ship_id,
                        timestamp,
                        float(record["LAT"]),
                        float(record["LON"]),
                        safe_int(record.get("SPEED", -1)),
                        safe_int(record.get("COURSE", -1)),
                        safe_int(record.get("HEADING", -1)),
                        safe_int(record.get("ROT", 0)),
                        safe_int(record.get("ELAPSED", 0)),
                        _to_bytes(record.get("DESTINATION", "null")),
                        safe_int(record.get("TILE_Z", -1)),
                        file_id,
                        -1,
                    )
                )

            file_id += 1

            if file_id % flush_every == 0:
                _flush_batches(h5file, ships_batch, positions_batch, files_batch)

        _flush_batches(h5file, ships_batch, positions_batch, files_batch)
        h5file.attrs["skipped_invalid_timestamp"] = int(skipped_invalid_timestamp)

    return output_path
