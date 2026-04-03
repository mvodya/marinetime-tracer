from __future__ import annotations

from pathlib import Path
from typing import Iterator

import h5py
import numpy as np

def open_dataset(path: str | Path, mode: str = "r") -> h5py.File:
    path = Path(path)
    return h5py.File(path, mode)


def print_dataset_structure(ds: h5py.File):
    print("HDF5 Dataset Structure:\n\n")

    for name, obj in ds.items():
        print(f"{name}:")
        if isinstance(obj, h5py.Dataset) and obj.dtype.fields:
            for field_name, (field_dtype, offset, *_) in obj.dtype.fields.items():
                print(f"  - {field_name}: {field_dtype}")
            if len(obj) >= 1:
                print(f"  Example: {obj[1]}")
        if isinstance(obj, h5py.Group):
            if name == "positions" and "tracks" in obj:
                print(f"  /YYYY\n    /MM\n      /DD:")
                for k1, item in obj.items():
                    if k1 == "tracks":
                        continue
                    for _, item in item.items():
                        for _, item in item.items():
                            for field_name, (field_dtype, offset, *_) in item.dtype.fields.items():
                                print(f"        - {field_name}: {field_dtype}")
                            print(f"        Example: {item[0]}")
                            break
                        break
                    break

                print(f"  /tracks\n    /AAAA-BBBB\n      /CCCC-DDDD:")
                for _, item in obj["tracks"].items():
                    for _, item in item.items():
                        for field_name, (field_dtype, offset, *_) in item.dtype.fields.items():
                            print(f"        - {field_name}: {field_dtype}")
                        print(f"        Example: {item[0]}")
                        break
                    break
            else:
                print(f"  /YYYY\n    /MM\n      /DD:")
                for _, item in obj.items():
                    for _, item in item.items():
                        for _, item in item.items():
                            for field_name, (field_dtype, offset, *_) in item.dtype.fields.items():
                                print(f"        - {field_name}: {field_dtype}")
                            print(f"        Example: {item[0]}")
                            break
                        break
                    break
        print("\n")


def print_dataset_counts(ds: h5py.File):
    days = 0
    count = 0

    for _, year in ds["positions"].items():
        for _, month in year.items():
            for _, day in month.items():
                days += 1
                count += len(day)

    print(f"Days: {days}")
    print(f"Ships: {len(ds["ships"])}")
    print(f"Positions count: {count}")
    print(f"Tracks count: {len(ds["tracks"])}")


def iter_day_datasets(ds: h5py.File) -> Iterator[tuple[tuple[str, str, str], h5py.Dataset]]:
    """
    Iterate over daily datasets in /positions/YYYY/MM/DD
    Yields: (('YYYY', 'MM', 'DD'), dataset)
    """
    if "positions" not in ds:
        return

    gpos = ds["positions"]
    for yyyy in sorted(gpos.keys()):
        gy = gpos[yyyy]
        if not isinstance(gy, h5py.Group):
            continue

        for mm in sorted(gy.keys()):
            gm = gy[mm]
            if not isinstance(gm, h5py.Group):
                continue

            for dd in sorted(gm.keys()):
                dsd = gm[dd]
                if isinstance(dsd, h5py.Dataset):
                    yield (yyyy, mm, dd), dsd


def collect_day_datasets(ds: h5py.File) -> tuple[list[tuple[tuple[str, str, str], h5py.Dataset]], int]:
    """
    Collect all daily position datasets and total positions count.
    """
    days: list[tuple[tuple[str, str, str], h5py.Dataset]] = []
    total_positions = 0

    for key, day_ds in iter_day_datasets(ds):
        days.append((key, day_ds))
        total_positions += int(day_ds.shape[0])

    return days, total_positions


def ensure_group(h5: h5py.File | h5py.Group, path: str) -> h5py.Group:
    """
    Create nested groups like mkdir -p.
    Example: positions/2024/10
    """
    g = h5
    for part in [p for p in path.split("/") if p]:
        if part not in g:
            g = g.create_group(part)
        else:
            g = g[part]
    return g


def append_rows(dst_ds: h5py.Dataset, rows: np.ndarray) -> None:
    """
    Append rows to resizable 1D structured dataset.
    """
    if rows.size == 0:
        return

    old = dst_ds.shape[0]
    new = old + rows.shape[0]
    dst_ds.resize((new,))
    dst_ds[old:new] = rows
    