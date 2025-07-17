import numpy as np
import h5py

# Функция для одного батча
def process_batch(batch, dataset_path, min_lat, max_lat, min_lon, max_lon, grid_step, lat_bins, lon_bins):
    local_counts = np.zeros((lat_bins, lon_bins), dtype=np.uint32)

    with h5py.File(dataset_path, "r") as d:
        for entry in batch:
            ds = d[entry["path"]]
            subset = ds[entry["start"]:entry["end"]]

            for row in subset:
                lat = row["lat"]
                lon = row["lon"]

                # Проверка диапазона
                if not (min_lat <= lat < max_lat and min_lon <= lon < max_lon):
                    continue

                lat_idx = int((lat - min_lat) / grid_step)
                lon_idx = int((lon - min_lon) / grid_step)

                local_counts[lat_idx, lon_idx] += 1

    return local_counts