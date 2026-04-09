from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
import json


@dataclass(slots=True)
class GridConfig:
    grid_size: int = 128
    cell_m: float = 100.0
    density_cell_m: float = 1000.0
    n_anchor: int = 5
    gaps_min_points: int = 5
    gaps_max_points: int = 100
    gaps_count_min: int = 1
    gaps_count_max: int = 3
    pred_thr: float = 0.5


@dataclass(slots=True)
class ArtifactBuildConfig:
    gap_time_sec: int = 2 * 60 * 60
    gap_dist_m: float = 30_000.0

    frag_gap_time_sec: int = 1 * 60 * 60
    frag_gap_dist_m: float = 10_000.0
    frag_min_disp_m: float = 10_000.0
    frag_min_points: int = 32

    good_tracks_target: int = 500_000
    h5_chunk_rows: int = 1_500_000
    density_max_points: int | None = 30_000_000
    density_chunk_rows: int = 1_500_000
    val_frac: float = 0.02
    split_seed: int = 42

    grid: GridConfig = field(default_factory=GridConfig)

    def to_dict(self) -> dict[str, Any]:
        return {
            "gap_time_sec": self.gap_time_sec,
            "gap_dist_m": self.gap_dist_m,
            "frag_gap_time_sec": self.frag_gap_time_sec,
            "frag_gap_dist_m": self.frag_gap_dist_m,
            "frag_min_disp_m": self.frag_min_disp_m,
            "frag_min_points": self.frag_min_points,
            "good_tracks_target": self.good_tracks_target,
            "h5_chunk_rows": self.h5_chunk_rows,
            "density_max_points": self.density_max_points,
            "density_chunk_rows": self.density_chunk_rows,
            "val_frac": self.val_frac,
            "split_seed": self.split_seed,
            "grid": asdict(self.grid),
        }

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "ArtifactBuildConfig":
        grid_obj = obj.get("grid", {})
        grid = GridConfig(**grid_obj)
        copy = dict(obj)
        copy["grid"] = grid
        return cls(**copy)

    def save_json(self, path: str | Path) -> Path:
        path = Path(path)
        path.write_text(json.dumps(self.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    @classmethod
    def load_json(cls, path: str | Path) -> "ArtifactBuildConfig":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))
