# Scripts

Если `mtlib` еще не установлен:

```bash
python3 -m pip install -e ./mt-py-tools
```

## Переменные

```bash
RAW_ARCHIVE=data/archive
RAW_H5=data/dataset_raw.h5
FILTERED_H5=data/dataset_filtered.h5
TRACKS_H5=data/dataset_tracks.h5
TSORTED_H5=data/dataset_tsorted.h5
ZONES_JSON=./mt-grabber/positions.json
POI_JSON=data/poi.json

ARTIFACT_DIR=data/track_restore_artifacts
TRAIN_OUT=runs/track_restore_v1
EVAL_OUT=runs/track_restore_v1_eval
ROUTE_PREVIEW_OUT=runs/track_restore_routes_preview
```

## Подготовка датасета

1. `build_dataset_hdf5.py`

```bash
python3 mt-py-tools/scripts/build_dataset_hdf5.py "$RAW_ARCHIVE" "$RAW_H5" \
  --start-date 01.01.2024 \
  --end-date 31.01.2024 \
  --overwrite
```

2. `import_zones.py`

```bash
python3 mt-py-tools/scripts/import_zones.py "$ZONES_JSON" "$RAW_H5"
```

3. `filter_dataset.py`

```bash
python3 mt-py-tools/scripts/filter_dataset.py "$RAW_H5" "$FILTERED_H5" --overwrite
```

4. `detect_tracks.py`

```bash
python3 mt-py-tools/scripts/detect_tracks.py "$FILTERED_H5" "$TRACKS_H5" --overwrite
```

5. `extract_poi.py`

```bash
python3 mt-py-tools/scripts/extract_poi.py "$TRACKS_H5" --output "$POI_JSON"
```

6. `repack_tracksorted_dataset.py`

```bash
python3 mt-py-tools/scripts/repack_tracksorted_dataset.py \
  "$TRACKS_H5" \
  "$TSORTED_H5" \
  --poi-json "$POI_JSON" \
  --overwrite
```

## NN: Track Restore

1. `build_track_restore_artifacts.py`

```bash
python3 mt-py-tools/scripts/nn-track-restore/build_track_restore_artifacts.py \
  "$TSORTED_H5" \
  --poi-json "$POI_JSON" \
  --out-dir "$ARTIFACT_DIR" \
  --overwrite
```

2. `preview_track_restore_samples.py` для проверки

```bash
python3 mt-py-tools/scripts/nn-track-restore/preview_track_restore_samples.py \
  "$TSORTED_H5" \
  "$ARTIFACT_DIR" \
  --split train \
  --index 0
```

3. `train_track_restore_artifacts.py`

```bash
python3 mt-py-tools/scripts/nn-track-restore/train_track_restore_artifacts.py \
  "$TSORTED_H5" \
  "$ARTIFACT_DIR" \
  --out-dir "$TRAIN_OUT" \
  --epochs 20 \
  --batch-size 16 \
  --num-workers 4 \
  --device cuda \
  --tb
```

4. `eval_track_restore.py` для проверки

```bash
python3 mt-py-tools/scripts/nn-track-restore/eval_track_restore.py \
  "$TSORTED_H5" \
  "$ARTIFACT_DIR" \
  "$TRAIN_OUT/checkpoints/best.pt" \
  --out-dir "$EVAL_OUT" \
  --split val \
  --batch-size 16 \
  --num-workers 4 \
  --device cuda
```

5. `preview_track_restore_routes.py` для проверки полного пайплайна восстановления маршрута

```bash
python3 mt-py-tools/scripts/nn-track-restore/preview_track_restore_routes.py \
  "$TSORTED_H5" \
  "$ARTIFACT_DIR" \
  "$TRAIN_OUT/checkpoints/best.pt" \
  --out-dir "$ROUTE_PREVIEW_OUT" \
  --split val \
  --count 10 \
  --device cuda
```
