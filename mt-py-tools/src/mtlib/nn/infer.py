from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import torch
from tqdm.auto import tqdm

from .config import GridConfig
from .data import read_track_fragment
from .postprocess import (
    RouteExtractionConfig,
    RouteExtractionResult,
    extract_route_from_prob_map,
)


@torch.no_grad()
def predict_prob_map(
    model, x: torch.Tensor, *, device: torch.device | str
) -> np.ndarray:
    device = torch.device(device)
    logits = model(x.unsqueeze(0).to(device))
    probs = torch.sigmoid(logits)[0, 0].detach().cpu().numpy()
    return probs.astype(np.float32, copy=False)


@torch.no_grad()
def predict_example_prob_map(
    model, example: dict[str, Any], *, device: torch.device | str
) -> np.ndarray:
    return predict_prob_map(model, example["x"], device=device)


@torch.no_grad()
def predict_and_extract_route(
    model,
    example: dict[str, Any],
    fragment: np.ndarray,
    *,
    grid_cfg: GridConfig,
    device: torch.device | str,
    extract_config: RouteExtractionConfig | None = None,
) -> tuple[np.ndarray, RouteExtractionResult]:
    meta = example.get("meta")
    if meta is None:
        raise KeyError("example must contain 'meta' for route extraction")
    gaps = meta.get("gaps") or []
    if not gaps:
        raise ValueError("example meta does not contain gaps")

    prob_map = predict_example_prob_map(model, example, device=device)
    result = extract_route_from_prob_map(
        prob_map,
        fragment,
        tuple(gaps[0]),
        meta["extent"],
        grid_size=grid_cfg.grid_size,
        config=extract_config,
    )
    result.meta.update(
        {
            "track_id": meta.get("track_id"),
            "s": meta.get("s"),
            "e": meta.get("e"),
            "points": meta.get("points"),
            "t0": meta.get("t0"),
            "t1": meta.get("t1"),
        }
    )
    return prob_map, result


@torch.no_grad()
def predict_dataset_routes(
    model,
    dataset,
    *,
    grid_cfg: GridConfig,
    device: torch.device | str,
    indices: list[int] | np.ndarray | None = None,
    extract_config: RouteExtractionConfig | None = None,
    progress: bool = True,
) -> pd.DataFrame:
    if not hasattr(dataset, "track_index") or not hasattr(dataset, "_ensure_open"):
        raise TypeError("dataset must be TrackInpaintDataset-compatible")

    if indices is None:
        work_indices = list(range(len(dataset)))
    else:
        work_indices = [int(i) for i in indices]

    rows: list[dict[str, Any]] = []
    iterator = work_indices
    if progress:
        iterator = tqdm(work_indices, desc="Predict dataset routes")

    h5 = dataset._ensure_open()
    for idx in iterator:
        example = dataset[idx]
        meta = example.get("meta")
        if meta is None:
            raise KeyError("dataset examples must contain meta")

        fragment = read_track_fragment(
            h5,
            dataset.track_index,
            meta["track_id"],
            meta["s"],
            meta["e"],
        )
        prob_map, result = predict_and_extract_route(
            model,
            example,
            fragment,
            grid_cfg=grid_cfg,
            device=device,
            extract_config=extract_config,
        )
        rows.append(
            {
                "idx": int(idx),
                "track_id": int(meta["track_id"]),
                "s": int(meta["s"]),
                "e": int(meta["e"]),
                "points": int(meta["points"]),
                "path_source": result.path_source,
                "path_len": int(len(result.path_cells)),
                "mean_prob_on_path": float(result.mean_prob_on_path),
                "start_cell": result.artifacts.start_cell,
                "end_cell": result.artifacts.end_cell,
                "corridor_pixels": int(result.artifacts.corridor_mask.sum()),
                "skeleton_pixels": int(result.artifacts.skeleton_mask.sum()),
                "graph_nodes": int(result.artifacts.graph.number_of_nodes()),
                "graph_edges": int(result.artifacts.graph.number_of_edges()),
                "gap": tuple(meta["gaps"][0]) if meta.get("gaps") else None,
                "prob_shape": tuple(prob_map.shape),
                "result": result,
                "prob_map": prob_map,
                "meta": meta,
            }
        )

    return pd.DataFrame(rows)
