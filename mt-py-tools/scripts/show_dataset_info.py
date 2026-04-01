from __future__ import annotations

import argparse
from pathlib import Path

from mtlib.dataset.ds import open_dataset, print_dataset_structure


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Вывод информации о датасете"
    )
    parser.add_argument(
        "--dataset",
        required=True,
        type=Path,
        help="Путь до файла HDF5 (.h5)",
    )
    return parser


def format_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{num_bytes} B"


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    dataset_path: Path = args.dataset

    if not dataset_path.exists():
        raise FileNotFoundError(f"Датасет не найден: {dataset_path}")

    print(f"Датасет: {dataset_path}")
    print(f"Размер:    {format_size(dataset_path.stat().st_size)}")
    print()

    with open_dataset(dataset_path) as ds:
        # Архитектура датасета
        print("Группы верхнего уровня:")
        for key in ds.keys():
            print(f"  - {key}")
        print()
        
        # Атрибуты датасета
        print("Атрибуты:")
        if ds.attrs:
            for key, value in ds.attrs.items():
                print(f"  - {key}: {value}")
        else:
            print("  (no root attributes)")

        print("\n\n\n")

        # Первый и последний файл из парсинга
        first_name = ds["files"]["name"][:-1][0].decode('utf-8')
        last_name = ds["files"]["name"][-1:][0].decode('utf-8')

        print(f"Первый файл: {first_name}\nПоследний файл: {last_name}")

        print("\n")

        # Подсчет количества позиций
        positions_sum = 0
        for file in ds["files"]:
            positions_sum += file["positions_count"]

        print(f"Всего записанных позиций (из оригинального парсинга): {positions_sum}")

        print("\n\n\n")

        # Вывод структуры датасета
        print_dataset_structure(ds)


if __name__ == "__main__":
    main()