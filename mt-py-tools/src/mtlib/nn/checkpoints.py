from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import torch


def save_checkpoint(
    path: str | Path,
    *,
    epoch: int,
    model,
    optimizer,
    history: list[dict[str, Any]],
    best_metric: float | None = None,
    extra: dict[str, Any] | None = None,
) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    obj = {
        "epoch": int(epoch),
        "model_state": model.state_dict(),
        "optimizer_state": optimizer.state_dict(),
        "history": history,
        "best_metric": best_metric,
        "extra": extra or {},
    }
    torch.save(obj, path)
    return path


def load_checkpoint(path: str | Path, *, model=None, optimizer=None, map_location="cpu") -> dict[str, Any]:
    obj = torch.load(Path(path), map_location=map_location)
    if model is not None:
        model.load_state_dict(obj["model_state"])
    if optimizer is not None:
        optimizer.load_state_dict(obj["optimizer_state"])
    return obj


def save_history_csv(path: str | Path, history: list[dict[str, Any]]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(history).to_csv(path, index=False)
    return path
