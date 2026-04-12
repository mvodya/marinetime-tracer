#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import torch

from mtlib.nn import (
    CombinedBCEDiceLoss,
    GridConfig,
    ResUNetAttention,
    TrackInpaintDataset,
    estimate_pos_weight,
    fit,
    get_device,
    make_loader,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Train track restoration model from prepared artifacts")
    p.add_argument("dataset_path", type=Path)
    p.add_argument("artifact_dir", type=Path)
    p.add_argument("--out-dir", type=Path, required=True)

    p.add_argument("--epochs", type=int, default=10)
    p.add_argument("--batch-size", type=int, default=16)
    p.add_argument("--num-workers", type=int, default=0)
    p.add_argument("--lr", type=float, default=1e-3)
    p.add_argument("--weight-decay", type=float, default=1e-4)

    p.add_argument("--grid-size", type=int, default=128)
    p.add_argument("--cell-m", type=float, default=100.0)
    p.add_argument("--density-cell-m", type=float, default=1000.0)

    p.add_argument("--line-radius", type=int, default=1)
    p.add_argument("--line-radius-known", type=int, default=None)
    p.add_argument("--line-radius-target", type=int, default=None)

    p.add_argument("--base-ch", type=int, default=32)
    p.add_argument("--groups", type=int, default=8)
    p.add_argument("--attn-heads", type=int, default=4)

    p.add_argument("--bce-weight", type=float, default=1.0)
    p.add_argument("--dice-weight", type=float, default=1.0)
    p.add_argument("--pred-thr", type=float, default=0.5)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--device", type=str, default=None)
    return p


def main() -> None:
    args = build_parser().parse_args()
    torch.manual_seed(args.seed)

    cfg = GridConfig(
        grid_size=args.grid_size,
        cell_m=args.cell_m,
        density_cell_m=args.density_cell_m,
        line_radius=args.line_radius,
        line_radius_known=args.line_radius_known,
        line_radius_target=args.line_radius_target,
    )

    train_frags = args.artifact_dir / "frags_train.parquet"
    if not train_frags.exists():
        train_frags = train_frags.with_suffix(".csv")

    val_frags = args.artifact_dir / "frags_val.parquet"
    if not val_frags.exists():
        val_frags = val_frags.with_suffix(".csv")

    train_ds = TrackInpaintDataset(
        dataset_path=args.dataset_path,
        frags_path=train_frags,
        track_index_path=args.artifact_dir / "track_index.pkl",
        density_path=args.artifact_dir / "density.npz",
        grid_cfg=cfg,
        seed=args.seed,
        return_meta=True,
    )
    val_ds = TrackInpaintDataset(
        dataset_path=args.dataset_path,
        frags_path=val_frags,
        track_index_path=args.artifact_dir / "track_index.pkl",
        density_path=args.artifact_dir / "density.npz",
        grid_cfg=cfg,
        seed=args.seed + 10_000_000,
        return_meta=True,
    )

    train_loader = make_loader(
        train_ds,
        batch_size=args.batch_size,
        shuffle=True,
        num_workers=args.num_workers,
        drop_last=False,
        pin_memory=False,
    )
    val_loader = make_loader(
        val_ds,
        batch_size=args.batch_size,
        shuffle=False,
        num_workers=args.num_workers,
        drop_last=False,
        pin_memory=False,
    )

    model = ResUNetAttention(
        in_ch=4,
        out_ch=1,
        base_ch=args.base_ch,
        groups=args.groups,
        attn_heads=args.attn_heads,
    )

    pos_weight = estimate_pos_weight(train_loader, max_batches=100)
    criterion = CombinedBCEDiceLoss(
        bce_weight=args.bce_weight,
        dice_weight=args.dice_weight,
        pos_weight=pos_weight,
    )

    device = get_device(args.device)
    criterion = criterion.to(device)
    model = model.to(device)

    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=args.lr,
        weight_decay=args.weight_decay,
    )

    args.out_dir.mkdir(parents=True, exist_ok=True)
    run_cfg = {
        "dataset_path": str(args.dataset_path),
        "artifact_dir": str(args.artifact_dir),
        "epochs": args.epochs,
        "batch_size": args.batch_size,
        "num_workers": args.num_workers,
        "lr": args.lr,
        "weight_decay": args.weight_decay,
        "grid": {
            "grid_size": args.grid_size,
            "cell_m": args.cell_m,
            "density_cell_m": args.density_cell_m,
            "line_radius": args.line_radius,
            "line_radius_known": args.line_radius_known,
            "line_radius_target": args.line_radius_target,
        },
        "model": {
            "base_ch": args.base_ch,
            "groups": args.groups,
            "attn_heads": args.attn_heads,
        },
        "loss": {
            "bce_weight": args.bce_weight,
            "dice_weight": args.dice_weight,
            "pos_weight": float(pos_weight.item()),
            "pred_thr": args.pred_thr,
        },
        "device": str(device),
        "seed": args.seed,
    }
    (args.out_dir / "run_config.json").write_text(
        json.dumps(run_cfg, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    history = fit(
        model,
        train_loader,
        val_loader,
        optimizer,
        criterion,
        out_dir=args.out_dir,
        epochs=args.epochs,
        device=device,
        amp_enabled=True,
        pred_thr=args.pred_thr,
        save_every=1,
        best_metric_name="iou",
    )

    print(f"Training complete. Epochs: {len(history)}")
    print(f"Artifacts saved to: {args.out_dir}")


if __name__ == "__main__":
    main()
