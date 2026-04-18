from __future__ import annotations

from pathlib import Path
from typing import Any

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import numpy as np
import torch

from .postprocess import RouteExtractionResult


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


def set_map_style(ax) -> None:
    ax.coastlines(resolution="10m")
    ax.add_feature(cfeature.LAND, facecolor="#f0f0f0")
    ax.add_feature(cfeature.OCEAN, facecolor="#dceeff")
    ax.add_feature(cfeature.BORDERS, linewidth=0.5)
    gl = ax.gridlines(draw_labels=True, alpha=0.35, linewidth=0.6)
    gl.top_labels = False
    gl.right_labels = False


def make_route_extraction_grid_figure(
    prob_map: np.ndarray,
    known: np.ndarray,
    result: RouteExtractionResult,
    *,
    title: str | None = None,
):
    fig, axs = plt.subplots(2, 4, figsize=(16, 9), constrained_layout=True)
    art = result.artifacts

    axs[0, 0].imshow(known, origin="lower", vmin=0.0, vmax=1.0)
    axs[0, 0].imshow(
        np.ma.masked_where(~art.start_mask, art.start_mask),
        origin="lower",
        cmap="Blues",
        alpha=0.55,
    )
    axs[0, 0].imshow(
        np.ma.masked_where(~art.end_mask, art.end_mask),
        origin="lower",
        cmap="Greens",
        alpha=0.55,
    )
    axs[0, 0].set_title("known + anchors")
    axs[0, 0].axis("off")

    im1 = axs[0, 1].imshow(prob_map, origin="lower", vmin=0.0, vmax=1.0)
    axs[0, 1].set_title("probabilities")
    axs[0, 1].axis("off")
    fig.colorbar(im1, ax=axs[0, 1], fraction=0.046, pad=0.04)

    axs[0, 2].imshow(
        art.low_mask.astype(np.float32), origin="lower", vmin=0.0, vmax=1.0
    )
    axs[0, 2].set_title("low mask")
    axs[0, 2].axis("off")

    axs[0, 3].imshow(
        art.high_mask.astype(np.float32), origin="lower", vmin=0.0, vmax=1.0
    )
    axs[0, 3].set_title("high mask")
    axs[0, 3].axis("off")

    axs[1, 0].imshow(
        art.corridor_mask.astype(np.float32), origin="lower", vmin=0.0, vmax=1.0
    )
    axs[1, 0].set_title("corridor")
    axs[1, 0].axis("off")

    axs[1, 1].imshow(art.corridor_mask.astype(np.float32), origin="lower", alpha=0.35)
    axs[1, 1].imshow(
        np.ma.masked_where(~art.skeleton_mask, art.skeleton_mask),
        origin="lower",
        cmap="magma",
        alpha=0.95,
    )
    axs[1, 1].set_title("corridor + skeleton")
    axs[1, 1].axis("off")

    axs[1, 2].imshow(prob_map, origin="lower", vmin=0.0, vmax=1.0, alpha=0.8)
    axs[1, 2].imshow(
        np.ma.masked_where(art.final_path_mask == 0, art.final_path_mask),
        origin="lower",
        cmap="autumn",
        alpha=0.95,
    )
    axs[1, 2].scatter(
        [art.start_cell[1], art.end_cell[1]],
        [art.start_cell[0], art.end_cell[0]],
        c=["tab:blue", "tab:green"],
        s=35,
    )
    axs[1, 2].set_title(result.path_source)
    axs[1, 2].axis("off")

    axs[1, 3].imshow(prob_map, origin="lower", vmin=0.0, vmax=1.0)
    axs[1, 3].imshow(
        np.ma.masked_where(known <= 0, known), origin="lower", cmap="winter", alpha=0.65
    )
    axs[1, 3].set_title("probabilities + known")
    axs[1, 3].axis("off")

    if title:
        fig.suptitle(title)
    return fig


def make_route_comparison_figure(
    fragment: np.ndarray,
    gap: tuple[int, int],
    extent: list[float] | tuple[float, float, float, float],
    prob_map: np.ndarray,
    result: RouteExtractionResult,
    *,
    title: str | None = None,
):
    pred_prob_for_map = np.ma.masked_where(prob_map <= 1e-6, prob_map)
    final_path_geo = result.path_latlon
    true_missing_geo = np.column_stack(
        [fragment["lat"][gap[0] : gap[1]], fragment["lon"][gap[0] : gap[1]]]
    ).astype(np.float64)

    fig = plt.figure(figsize=(14, 6.5), constrained_layout=True)
    gs = fig.add_gridspec(1, 2, width_ratios=[1.0, 1.0])

    ax_map = fig.add_subplot(gs[0, 0], projection=ccrs.PlateCarree())
    set_map_style(ax_map)
    ax_map.set_extent(extent, crs=ccrs.PlateCarree())
    ax_map.plot(
        fragment["lon"],
        fragment["lat"],
        color="0.82",
        linewidth=1.2,
        transform=ccrs.PlateCarree(),
        zorder=1,
    )
    ax_map.plot(
        fragment["lon"][: gap[0]],
        fragment["lat"][: gap[0]],
        color="tab:blue",
        linewidth=2.3,
        transform=ccrs.PlateCarree(),
        label="known start",
        zorder=3,
    )
    ax_map.plot(
        fragment["lon"][gap[1] :],
        fragment["lat"][gap[1] :],
        color="tab:green",
        linewidth=2.3,
        transform=ccrs.PlateCarree(),
        label="known end",
        zorder=3,
    )
    ax_map.plot(
        true_missing_geo[:, 1],
        true_missing_geo[:, 0],
        color="tab:red",
        linewidth=2.0,
        linestyle="--",
        transform=ccrs.PlateCarree(),
        label="true missing",
        zorder=4,
    )
    if len(final_path_geo) > 0:
        ax_map.plot(
            final_path_geo[:, 1],
            final_path_geo[:, 0],
            color="tab:orange",
            linewidth=2.2,
            transform=ccrs.PlateCarree(),
            label="predicted path",
            zorder=5,
        )
    ax_map.scatter(
        [fragment["lon"][gap[0] - 1], fragment["lon"][gap[1]]],
        [fragment["lat"][gap[0] - 1], fragment["lat"][gap[1]]],
        c=["tab:blue", "tab:green"],
        s=55,
        transform=ccrs.PlateCarree(),
        zorder=6,
    )
    ax_map.set_title("Map")
    ax_map.legend(loc="lower left")

    ax_heat = fig.add_subplot(gs[0, 1], projection=ccrs.PlateCarree())
    set_map_style(ax_heat)
    ax_heat.set_extent(extent, crs=ccrs.PlateCarree())
    im = ax_heat.imshow(
        pred_prob_for_map,
        origin="lower",
        extent=extent,
        transform=ccrs.PlateCarree(),
        vmin=0.0,
        vmax=1.0,
        cmap="magma",
        alpha=0.88,
        zorder=1,
    )
    ax_heat.plot(
        fragment["lon"][: gap[0]],
        fragment["lat"][: gap[0]],
        color="deepskyblue",
        linewidth=2.0,
        transform=ccrs.PlateCarree(),
        label="known start",
        zorder=3,
    )
    ax_heat.plot(
        fragment["lon"][gap[1] :],
        fragment["lat"][gap[1] :],
        color="lime",
        linewidth=2.0,
        transform=ccrs.PlateCarree(),
        label="known end",
        zorder=3,
    )
    ax_heat.plot(
        true_missing_geo[:, 1],
        true_missing_geo[:, 0],
        color="white",
        linewidth=1.8,
        linestyle="--",
        transform=ccrs.PlateCarree(),
        label="true missing",
        zorder=4,
    )
    if len(final_path_geo) > 0:
        ax_heat.plot(
            final_path_geo[:, 1],
            final_path_geo[:, 0],
            color="cyan",
            linewidth=2.2,
            transform=ccrs.PlateCarree(),
            label="predicted path",
            zorder=5,
        )
    ax_heat.scatter(
        [fragment["lon"][gap[0] - 1], fragment["lon"][gap[1]]],
        [fragment["lat"][gap[0] - 1], fragment["lat"][gap[1]]],
        c=["deepskyblue", "lime"],
        s=55,
        transform=ccrs.PlateCarree(),
        zorder=6,
    )
    ax_heat.set_title(
        f"NN probabilities | {result.path_source} | meanP={result.mean_prob_on_path:.3f}"
    )
    ax_heat.legend(loc="lower left")
    cb = fig.colorbar(im, ax=ax_heat, fraction=0.046, pad=0.03)
    cb.set_label("predicted probability")

    if title:
        fig.suptitle(title)
    return fig


def save_route_comparison_png(
    path: str | Path,
    fragment: np.ndarray,
    gap: tuple[int, int],
    extent: list[float] | tuple[float, float, float, float],
    prob_map: np.ndarray,
    result: RouteExtractionResult,
    *,
    title: str | None = None,
    dpi: int = 180,
) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig = make_route_comparison_figure(
        fragment,
        gap,
        extent,
        prob_map,
        result,
        title=title,
    )
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    return path
