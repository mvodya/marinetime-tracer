from __future__ import annotations

from dataclasses import dataclass, field
import heapq
import math
from typing import Any

import networkx as nx
import numpy as np
from tqdm.auto import tqdm


NEIGHBORS8: list[tuple[int, int]] = [
    (-1, -1),
    (-1, 0),
    (-1, 1),
    (0, -1),
    (0, 1),
    (1, -1),
    (1, 0),
    (1, 1),
]


@dataclass(slots=True)
class RouteExtractionConfig:
    anchor_radius: int = 2
    low_thr: float = 0.20
    high_thr: float = 0.55
    skeleton_max_iters: int = 64
    off_corridor_penalty: float = 1.5


@dataclass(slots=True)
class CorridorBuildResult:
    low: np.ndarray
    high: np.ndarray
    corridor: np.ndarray


@dataclass(slots=True)
class RouteExtractionArtifacts:
    start_cell: tuple[int, int]
    end_cell: tuple[int, int]
    start_mask: np.ndarray
    end_mask: np.ndarray
    low_mask: np.ndarray
    high_mask: np.ndarray
    corridor_mask: np.ndarray
    skeleton_mask: np.ndarray
    graph: nx.Graph
    graph_path: list[tuple[int, int]]
    final_path_mask: np.ndarray


@dataclass(slots=True)
class RouteExtractionResult:
    path_cells: list[tuple[int, int]]
    path_latlon: np.ndarray
    path_source: str
    mean_prob_on_path: float
    artifacts: RouteExtractionArtifacts
    meta: dict[str, Any] = field(default_factory=dict)


def point_to_grid_cell(
    lat: float,
    lon: float,
    extent: list[float] | tuple[float, float, float, float],
    grid_size: int,
) -> tuple[int, int]:
    min_lon, max_lon, min_lat, max_lat = map(float, extent)
    w = h = int(grid_size)
    x = int(
        np.clip(
            np.floor((float(lon) - min_lon) / max(1e-12, max_lon - min_lon) * w),
            0,
            w - 1,
        )
    )
    y = int(
        np.clip(
            np.floor((float(lat) - min_lat) / max(1e-12, max_lat - min_lat) * h),
            0,
            h - 1,
        )
    )
    return y, x


def cells_to_latlon(
    cells: list[tuple[int, int]],
    extent: list[float] | tuple[float, float, float, float],
    grid_size: int,
) -> np.ndarray:
    if not cells:
        return np.empty((0, 2), dtype=np.float64)

    min_lon, max_lon, min_lat, max_lat = map(float, extent)
    h = w = int(grid_size)
    rows: list[tuple[float, float]] = []
    for y, x in cells:
        lon = min_lon + ((x + 0.5) / w) * (max_lon - min_lon)
        lat = min_lat + ((y + 0.5) / h) * (max_lat - min_lat)
        rows.append((lat, lon))
    return np.asarray(rows, dtype=np.float64)


def make_anchor_mask(
    shape: tuple[int, int], cell: tuple[int, int], radius: int = 2
) -> np.ndarray:
    mask = np.zeros(shape, dtype=bool)
    cy, cx = map(int, cell)
    yy, xx = np.ogrid[: shape[0], : shape[1]]
    mask[(yy - cy) ** 2 + (xx - cx) ** 2 <= int(radius) ** 2] = True
    return mask


def build_hysteresis_corridor(
    prob_map: np.ndarray,
    start_mask: np.ndarray,
    end_mask: np.ndarray,
    low_thr: float = 0.20,
    high_thr: float = 0.55,
) -> CorridorBuildResult:
    low = prob_map >= float(low_thr)
    high = prob_map >= float(high_thr)

    allowed = low | start_mask | end_mask
    seeds = high | start_mask | end_mask

    corridor = np.zeros_like(allowed, dtype=bool)
    stack = [tuple(map(int, pt)) for pt in np.argwhere(seeds & allowed)]

    while stack:
        y, x = stack.pop()
        if corridor[y, x]:
            continue
        corridor[y, x] = True
        for dy, dx in NEIGHBORS8:
            ny = y + dy
            nx = x + dx
            if (
                0 <= ny < corridor.shape[0]
                and 0 <= nx < corridor.shape[1]
                and allowed[ny, nx]
                and not corridor[ny, nx]
            ):
                stack.append((ny, nx))

    return CorridorBuildResult(low=low, high=high, corridor=corridor)


def _neighbors_clockwise(img: np.ndarray, y: int, x: int) -> list[int]:
    return [
        int(img[y - 1, x]),
        int(img[y - 1, x + 1]),
        int(img[y, x + 1]),
        int(img[y + 1, x + 1]),
        int(img[y + 1, x]),
        int(img[y + 1, x - 1]),
        int(img[y, x - 1]),
        int(img[y - 1, x - 1]),
    ]


def zhang_suen_thinning(
    mask: np.ndarray, max_iters: int = 64, verbose: bool = False
) -> np.ndarray:
    img = np.pad(mask.astype(np.uint8), 1, mode="constant")
    iterator = range(int(max_iters))
    if verbose:
        iterator = tqdm(iterator, desc="zhang-suen", leave=False)

    for _ in iterator:
        changed = False

        to_remove: list[tuple[int, int]] = []
        ys, xs = np.nonzero(img)
        for y, x in zip(ys, xs, strict=False):
            if y == 0 or x == 0 or y == img.shape[0] - 1 or x == img.shape[1] - 1:
                continue
            p = _neighbors_clockwise(img, int(y), int(x))
            b = sum(p)
            a = sum((p[i] == 0 and p[(i + 1) % 8] == 1) for i in range(8))
            if (
                2 <= b <= 6
                and a == 1
                and p[0] * p[2] * p[4] == 0
                and p[2] * p[4] * p[6] == 0
            ):
                to_remove.append((int(y), int(x)))

        if to_remove:
            changed = True
            for y, x in to_remove:
                img[y, x] = 0

        to_remove = []
        ys, xs = np.nonzero(img)
        for y, x in zip(ys, xs, strict=False):
            if y == 0 or x == 0 or y == img.shape[0] - 1 or x == img.shape[1] - 1:
                continue
            p = _neighbors_clockwise(img, int(y), int(x))
            b = sum(p)
            a = sum((p[i] == 0 and p[(i + 1) % 8] == 1) for i in range(8))
            if (
                2 <= b <= 6
                and a == 1
                and p[0] * p[2] * p[6] == 0
                and p[0] * p[4] * p[6] == 0
            ):
                to_remove.append((int(y), int(x)))

        if to_remove:
            changed = True
            for y, x in to_remove:
                img[y, x] = 0

        if not changed:
            break

    return img[1:-1, 1:-1].astype(bool)


def build_skeleton_graph(skeleton: np.ndarray, prob_map: np.ndarray) -> nx.Graph:
    graph = nx.Graph()
    coords = [tuple(map(int, pt)) for pt in np.argwhere(skeleton)]
    coord_set = set(coords)

    for node in coords:
        y, x = node
        graph.add_node(node, prob=float(prob_map[y, x]))

    for y, x in coords:
        for dy, dx in NEIGHBORS8:
            nbr = (y + dy, x + dx)
            if nbr not in coord_set or graph.has_edge((y, x), nbr):
                continue
            dist = math.sqrt(2.0) if dy != 0 and dx != 0 else 1.0
            p = max(
                1e-6, 0.5 * (float(prob_map[y, x]) + float(prob_map[nbr[0], nbr[1]]))
            )
            graph.add_edge((y, x), nbr, weight=(-math.log(p)) * dist, dist=dist)

    return graph


def closest_graph_node(
    graph: nx.Graph, cell: tuple[int, int]
) -> tuple[int, int] | None:
    if graph.number_of_nodes() == 0:
        return None
    cy, cx = map(float, cell)
    return min(
        graph.nodes,
        key=lambda node: (node[0] - cy) ** 2 + (node[1] - cx) ** 2,
    )


def astar_grid_path(
    prob_map: np.ndarray,
    corridor_mask: np.ndarray,
    start_cell: tuple[int, int],
    end_cell: tuple[int, int],
    off_corridor_penalty: float = 1.5,
) -> list[tuple[int, int]]:
    h, w = prob_map.shape
    start = tuple(map(int, start_cell))
    goal = tuple(map(int, end_cell))

    def heuristic(cell: tuple[int, int]) -> float:
        return math.hypot(cell[0] - goal[0], cell[1] - goal[1])

    def step_cost(cell_from: tuple[int, int], cell_to: tuple[int, int]) -> float:
        y, x = cell_to
        base = -math.log(max(1e-6, float(prob_map[y, x])))
        if not corridor_mask[y, x]:
            base += float(off_corridor_penalty)
        dist = (
            math.sqrt(2.0)
            if abs(cell_from[0] - y) == 1 and abs(cell_from[1] - x) == 1
            else 1.0
        )
        return base * dist

    queue: list[tuple[float, float, tuple[int, int]]] = [(heuristic(start), 0.0, start)]
    came_from: dict[tuple[int, int], tuple[int, int]] = {}
    g_score: dict[tuple[int, int], float] = {start: 0.0}
    visited: set[tuple[int, int]] = set()

    while queue:
        _, cur_g, current = heapq.heappop(queue)
        if current in visited:
            continue
        visited.add(current)

        if current == goal:
            break

        cy, cx = current
        for dy, dx in NEIGHBORS8:
            ny = cy + dy
            nx = cx + dx
            if not (0 <= ny < h and 0 <= nx < w):
                continue

            nxt = (ny, nx)
            tentative = cur_g + step_cost(current, nxt)
            if tentative < g_score.get(nxt, float("inf")):
                g_score[nxt] = tentative
                came_from[nxt] = current
                heapq.heappush(queue, (tentative + heuristic(nxt), tentative, nxt))

    if goal not in came_from and goal != start:
        return []

    path = [goal]
    while path[-1] != start:
        path.append(came_from[path[-1]])
    path.reverse()
    return path


def path_to_mask(path: list[tuple[int, int]], shape: tuple[int, int]) -> np.ndarray:
    mask = np.zeros(shape, dtype=np.float32)
    for y, x in path:
        mask[int(y), int(x)] = 1.0
    return mask


def extract_route_from_prob_map(
    prob_map: np.ndarray,
    fragment: np.ndarray,
    gap: tuple[int, int],
    extent: list[float] | tuple[float, float, float, float],
    *,
    grid_size: int,
    config: RouteExtractionConfig | None = None,
) -> RouteExtractionResult:
    cfg = config or RouteExtractionConfig()

    start_cell = point_to_grid_cell(
        float(fragment["lat"][gap[0] - 1]),
        float(fragment["lon"][gap[0] - 1]),
        extent,
        grid_size,
    )
    end_cell = point_to_grid_cell(
        float(fragment["lat"][gap[1]]),
        float(fragment["lon"][gap[1]]),
        extent,
        grid_size,
    )

    start_mask = make_anchor_mask(prob_map.shape, start_cell, radius=cfg.anchor_radius)
    end_mask = make_anchor_mask(prob_map.shape, end_cell, radius=cfg.anchor_radius)

    hyst = build_hysteresis_corridor(
        prob_map,
        start_mask,
        end_mask,
        low_thr=cfg.low_thr,
        high_thr=cfg.high_thr,
    )
    skeleton = zhang_suen_thinning(
        hyst.corridor, max_iters=cfg.skeleton_max_iters, verbose=False
    )
    graph = build_skeleton_graph(skeleton, prob_map)
    start_node = closest_graph_node(graph, start_cell)
    end_node = closest_graph_node(graph, end_cell)

    graph_path: list[tuple[int, int]] = []
    if (
        start_node is not None
        and end_node is not None
        and nx.has_path(graph, start_node, end_node)
    ):
        graph_path = nx.shortest_path(
            graph, source=start_node, target=end_node, weight="weight"
        )

    final_path = graph_path
    path_source = "skeleton graph"
    if not final_path:
        final_path = astar_grid_path(
            prob_map,
            hyst.corridor,
            start_cell,
            end_cell,
            off_corridor_penalty=cfg.off_corridor_penalty,
        )
        path_source = "A* fallback"

    mean_prob = (
        float(np.mean([prob_map[y, x] for y, x in final_path]))
        if final_path
        else float("nan")
    )
    final_path_mask = path_to_mask(final_path, prob_map.shape)
    path_latlon = cells_to_latlon(final_path, extent, grid_size)

    artifacts = RouteExtractionArtifacts(
        start_cell=start_cell,
        end_cell=end_cell,
        start_mask=start_mask,
        end_mask=end_mask,
        low_mask=hyst.low,
        high_mask=hyst.high,
        corridor_mask=hyst.corridor,
        skeleton_mask=skeleton,
        graph=graph,
        graph_path=graph_path,
        final_path_mask=final_path_mask,
    )
    return RouteExtractionResult(
        path_cells=final_path,
        path_latlon=path_latlon,
        path_source=path_source,
        mean_prob_on_path=mean_prob,
        artifacts=artifacts,
        meta={
            "gap": (int(gap[0]), int(gap[1])),
            "extent": [float(v) for v in extent],
        },
    )
