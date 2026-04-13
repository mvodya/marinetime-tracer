from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import torch


@torch.no_grad()
def make_preview_figure(
    x: torch.Tensor,
    y: torch.Tensor,
    logits: torch.Tensor,
    metas: list[dict[str, Any]] | None = None,
    *,
    max_items: int = 4,
    pred_thr: float = 0.5,
):
    x_np = x.detach().cpu().numpy()
    y_np = y.detach().cpu().numpy()
    p_np = (torch.sigmoid(logits).detach().cpu().numpy() >= pred_thr).astype(np.float32)

    n = min(max_items, x_np.shape[0])
    fig = plt.figure(figsize=(16, 4 * n))

    for i in range(n):
        known = x_np[i, 0]
        density = x_np[i, 3]
        target = y_np[i, 0]
        pred = p_np[i, 0]

        title_suffix = ""
        if metas is not None and i < len(metas):
            m = metas[i]
            title_suffix = f" | track_id={m.get('track_id')} points={m.get('points')}"

        ax1 = fig.add_subplot(n, 4, i * 4 + 1)
        ax1.imshow(known, origin="lower")
        ax1.set_title(f"known{title_suffix}")
        ax1.axis("off")

        ax2 = fig.add_subplot(n, 4, i * 4 + 2)
        ax2.imshow(density, origin="lower")
        ax2.set_title("density")
        ax2.axis("off")

        ax3 = fig.add_subplot(n, 4, i * 4 + 3)
        ax3.imshow(target, origin="lower")
        ax3.set_title("target")
        ax3.axis("off")

        ax4 = fig.add_subplot(n, 4, i * 4 + 4)
        ax4.imshow(pred, origin="lower")
        ax4.set_title("pred")
        ax4.axis("off")

    plt.tight_layout()
    return fig


@torch.no_grad()
def save_preview_png(
    path: str | Path,
    x: torch.Tensor,
    y: torch.Tensor,
    logits: torch.Tensor,
    metas: list[dict[str, Any]] | None = None,
    *,
    max_items: int = 4,
    pred_thr: float = 0.5,
) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig = make_preview_figure(
        x, y, logits, metas,
        max_items=max_items,
        pred_thr=pred_thr,
    )
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path
