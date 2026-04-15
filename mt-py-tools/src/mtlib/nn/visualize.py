from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import torch


def _prepare_density_for_display(
    density: np.ndarray, q: float = 0.98
) -> tuple[np.ndarray, float]:
    vis = np.log1p(density.astype(np.float32, copy=False))
    nz = vis[vis > 0]
    vmax = float(np.quantile(nz, q)) if nz.size else 1.0
    vmax = max(vmax, 1e-6)
    return np.clip(vis, 0.0, vmax), vmax


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
        density_vis, density_vmax = _prepare_density_for_display(density)
        ax2.imshow(density_vis, origin="lower", vmin=0.0, vmax=density_vmax)
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
        x,
        y,
        logits,
        metas,
        max_items=max_items,
        pred_thr=pred_thr,
    )
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path
