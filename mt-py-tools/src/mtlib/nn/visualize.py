from __future__ import annotations

from pathlib import Path
from typing import Any

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
from matplotlib.cm import ScalarMappable
from matplotlib.colors import Normalize
import numpy as np
import torch

from .postprocess import RouteExtractionResult


ROUTE_COLORS = {
    "full": "0.82",
    "known_start": "tab:blue",
    "known_end": "tab:green",
    "true_missing": "tab:red",
    "predicted": "tab:orange",
}


def _prepare_density_for_display(
    density: np.ndarray, q: float = 0.98
) -> tuple[np.ndarray, float]:
    vis = np.log1p(density.astype(np.float32, copy=False))
    nz = vis[vis > 0]
    vmax = float(np.quantile(nz, q)) if nz.size else 1.0
    vmax = max(vmax, 1e-6)
    return np.clip(vis, 0.0, vmax), vmax


def _probability_map_for_display(prob_map: np.ndarray) -> np.ndarray:
    vis = np.clip(prob_map, 0.0, 1.0)
    return np.where(vis <= 1e-6, 1e-3, vis)


def _route_source_label(path_source: str) -> str:
    return {
        "skeleton graph": "скелетный граф",
        "A* fallback": "резервный A*",
    }.get(path_source, path_source)


def _true_missing_geo(fragment: np.ndarray, gap: tuple[int, int]) -> np.ndarray:
    return np.column_stack(
        [fragment["lat"][gap[0] : gap[1]], fragment["lon"][gap[0] : gap[1]]]
    ).astype(np.float64)


def _plot_route_map(
    ax,
    fragment: np.ndarray,
    gap: tuple[int, int],
    extent: list[float] | tuple[float, float, float, float],
    result: RouteExtractionResult,
):
    final_path_geo = result.path_latlon
    true_missing_geo = _true_missing_geo(fragment, gap)

    set_map_style(ax)
    ax.set_extent(extent, crs=ccrs.PlateCarree())
    ax.plot(
        fragment["lon"],
        fragment["lat"],
        color=ROUTE_COLORS["full"],
        linewidth=1.2,
        transform=ccrs.PlateCarree(),
        zorder=1,
    )
    ax.plot(
        fragment["lon"][: gap[0]],
        fragment["lat"][: gap[0]],
        color=ROUTE_COLORS["known_start"],
        linewidth=2.3,
        transform=ccrs.PlateCarree(),
        label="известное начало",
        zorder=3,
    )
    ax.plot(
        fragment["lon"][gap[1] :],
        fragment["lat"][gap[1] :],
        color=ROUTE_COLORS["known_end"],
        linewidth=2.3,
        transform=ccrs.PlateCarree(),
        label="известный конец",
        zorder=3,
    )
    ax.plot(
        true_missing_geo[:, 1],
        true_missing_geo[:, 0],
        color=ROUTE_COLORS["true_missing"],
        linewidth=2.0,
        linestyle="--",
        transform=ccrs.PlateCarree(),
        label="истинный пропуск",
        zorder=4,
    )
    if len(final_path_geo) > 0:
        ax.plot(
            final_path_geo[:, 1],
            final_path_geo[:, 0],
            color=ROUTE_COLORS["predicted"],
            linewidth=2.2,
            transform=ccrs.PlateCarree(),
            label="предсказанный путь",
            zorder=5,
        )
    ax.scatter(
        [fragment["lon"][gap[0] - 1], fragment["lon"][gap[1]]],
        [fragment["lat"][gap[0] - 1], fragment["lat"][gap[1]]],
        c=[ROUTE_COLORS["known_start"], ROUTE_COLORS["known_end"]],
        s=55,
        transform=ccrs.PlateCarree(),
        zorder=6,
    )


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
    ax.add_feature(cfeature.OCEAN, facecolor="#dceeff", zorder=0)
    ax.add_feature(cfeature.LAND, facecolor="#f0f0f0", zorder=0)
    ax.coastlines(resolution="10m", zorder=2)
    ax.add_feature(cfeature.BORDERS, linewidth=0.5, zorder=2)
    gl = ax.gridlines(draw_labels=True, alpha=0.35, linewidth=0.6)
    gl.top_labels = False
    gl.right_labels = False


def make_route_extraction_grid_figure(
    prob_map: np.ndarray,
    known: np.ndarray,
    result: RouteExtractionResult,
    fragment: np.ndarray,
    gap: tuple[int, int],
    extent: list[float] | tuple[float, float, float, float],
    *,
    title: str | None = None,
):
    fig = plt.figure(figsize=(16, 9), constrained_layout=True)
    gs = fig.add_gridspec(2, 4)
    axs = np.empty((2, 4), dtype=object)
    for row in range(2):
        for col in range(4):
            if row == 1 and col == 3:
                axs[row, col] = fig.add_subplot(
                    gs[row, col], projection=ccrs.PlateCarree()
                )
            else:
                axs[row, col] = fig.add_subplot(gs[row, col])
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
    axs[0, 0].set_title("Известная часть пути")
    axs[0, 0].axis("off")

    im1 = axs[0, 1].imshow(prob_map, origin="lower", vmin=0.0, vmax=1.0)
    axs[0, 1].set_title("Предсказанные вероятности")
    axs[0, 1].axis("off")
    cb1 = fig.colorbar(im1, ax=axs[0, 1], fraction=0.046, pad=0.04)
    cb1.set_label("Вероятность")

    axs[0, 2].imshow(
        art.low_mask.astype(np.float32), origin="lower", vmin=0.0, vmax=1.0
    )
    axs[0, 2].set_title("Низкий порог (low_thr)")
    axs[0, 2].axis("off")

    axs[0, 3].imshow(
        art.high_mask.astype(np.float32), origin="lower", vmin=0.0, vmax=1.0
    )
    axs[0, 3].set_title("Высокий порог (high_thr)")
    axs[0, 3].axis("off")

    axs[1, 0].imshow(
        art.corridor_mask.astype(np.float32), origin="lower", vmin=0.0, vmax=1.0
    )
    axs[1, 0].set_title("Коридор")
    axs[1, 0].axis("off")

    axs[1, 1].imshow(art.corridor_mask.astype(np.float32), origin="lower", alpha=0.35)
    axs[1, 1].imshow(
        np.ma.masked_where(~art.skeleton_mask, art.skeleton_mask),
        origin="lower",
        cmap="magma",
        alpha=0.95,
    )
    axs[1, 1].set_title("Коридор + скелет")
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
    axs[1, 2].set_title(f"Предсказанный путь: {_route_source_label(result.path_source)}")
    axs[1, 2].axis("off")

    _plot_route_map(axs[1, 3], fragment, gap, extent, result)
    axs[1, 3].set_title("Карта маршрута")
    axs[1, 3].legend(loc="lower left", fontsize="small")

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
    final_path_geo = result.path_latlon
    true_missing_geo = _true_missing_geo(fragment, gap)

    fig = plt.figure(figsize=(14, 6.5), constrained_layout=True)
    gs = fig.add_gridspec(1, 2, width_ratios=[1.0, 1.0])

    ax_map = fig.add_subplot(gs[0, 0], projection=ccrs.PlateCarree())
    _plot_route_map(ax_map, fragment, gap, extent, result)
    ax_map.set_title("Карта")
    ax_map.legend(loc="lower left")

    ax_heat = fig.add_subplot(gs[0, 1], projection=ccrs.PlateCarree())
    set_map_style(ax_heat)
    ax_heat.set_extent(extent, crs=ccrs.PlateCarree())
    ax_heat.imshow(
        _probability_map_for_display(prob_map),
        origin="lower",
        extent=extent,
        transform=ccrs.PlateCarree(),
        vmin=0.0,
        vmax=1.0,
        cmap="magma",
        alpha=0.82,
        interpolation="nearest",
        zorder=1,
    )
    ax_heat.plot(
        fragment["lon"][: gap[0]],
        fragment["lat"][: gap[0]],
        color=ROUTE_COLORS["known_start"],
        linewidth=2.0,
        transform=ccrs.PlateCarree(),
        label="известное начало",
        zorder=3,
    )
    ax_heat.plot(
        fragment["lon"][gap[1] :],
        fragment["lat"][gap[1] :],
        color=ROUTE_COLORS["known_end"],
        linewidth=2.0,
        transform=ccrs.PlateCarree(),
        label="известный конец",
        zorder=3,
    )
    ax_heat.plot(
        true_missing_geo[:, 1],
        true_missing_geo[:, 0],
        color=ROUTE_COLORS["true_missing"],
        linewidth=1.8,
        linestyle="--",
        transform=ccrs.PlateCarree(),
        label="истинный пропуск",
        zorder=4,
    )
    if len(final_path_geo) > 0:
        ax_heat.plot(
            final_path_geo[:, 1],
            final_path_geo[:, 0],
            color=ROUTE_COLORS["predicted"],
            linewidth=2.2,
            transform=ccrs.PlateCarree(),
            label="предсказанный путь",
            zorder=5,
        )
    ax_heat.scatter(
        [fragment["lon"][gap[0] - 1], fragment["lon"][gap[1]]],
        [fragment["lat"][gap[0] - 1], fragment["lat"][gap[1]]],
        c=[ROUTE_COLORS["known_start"], ROUTE_COLORS["known_end"]],
        s=55,
        transform=ccrs.PlateCarree(),
        zorder=6,
    )
    ax_heat.set_title(
        f"Вероятностная карта | {_route_source_label(result.path_source)} | средняя P={result.mean_prob_on_path:.3f}"
    )
    ax_heat.legend(loc="lower left")
    sm = ScalarMappable(norm=Normalize(vmin=0.0, vmax=1.0), cmap="magma")
    sm.set_array([])
    cb = fig.colorbar(sm, ax=ax_heat, fraction=0.046, pad=0.03)
    cb.set_label("Предсказанная вероятность")

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
