from __future__ import annotations

import argparse
from pathlib import Path

from mtlib.dataset import build_hdf5_from_archive


def main() -> None:
    parser = argparse.ArgumentParser(description="Сборка HDF5 датасета на основе json-архива спарсенных данных с MarineTraffic")
    parser.add_argument("source_path", type=Path, help="Путь до отсортированного архива")
    parser.add_argument("output_path", type=Path, help="Путь до нового HDF5 файла")
    parser.add_argument("--start-date", required=True, help="Начальная дата в формате DD.MM.YYYY")
    parser.add_argument("--end-date", required=True, help="Конечная дата в формате DD.MM.YYYY")
    parser.add_argument("--flush-every", type=int, default=50, help="Сохранять HDF каждые N файлов")
    parser.add_argument("--overwrite", action="store_true", help="Перезаписать HDF5 файл")

    args = parser.parse_args()

    build_hdf5_from_archive(
        source_path=args.source_path,
        output_path=args.output_path,
        start_date=args.start_date,
        end_date=args.end_date,
        flush_every=args.flush_every,
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    main()