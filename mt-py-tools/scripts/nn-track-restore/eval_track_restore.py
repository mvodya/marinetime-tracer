#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import torch

from mtlib.nn import (
    GridConfig,
    ResUNetAttention,
    TrackInpaintDataset,
    get_device,
    load_checkpoint,
    make_loader,
    save_preview_png,
    validate,
    CombinedBCEDiceLoss,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Evaluate a trained track restoration model")
    p.add_argument("dataset_path", type=Path)
    p.add_argument("artifact_dir", type=Path)
    p.add_argument("checkpoint", type=Path)
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--split", choices=["train", "val"], default="val")
    p.add_argument("--batch-size", type=int, default=16)
    p.add_argument("--num-workers", type=int, default=0)
    p.add_argument("--device", type=str, default=None)
    p.add_argument("--grid-size", type=int, default=128)
    p.add_argument("--cell-m", type=float, default=100.0)
    p.add_argument("--density-cell-m", type=float, default=1000.0)
    p.add_argument("--line-radius", type=int, default=1)
    p.add_argument("--line-radius-known", type=int, default=None)
    p.add_argument("--line-radius-target", type=int, default=None)
    p.add_argument("--base-ch", type=int, default=32)
    p.add_argument("--groups", type=int, default=8)
    p.add_argument("--attn-heads", type=int, default=4)
    p.add_argument("--pred-thr", type=float, default=0.5)
    p.add_argument("--preview-count", type=int, default=4)
    return p


def main() -> None:
    args = build_parser().parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    cfg = GridConfig(
        grid_size=args.grid_size,
        cell_m=args.cell_m,
        density_cell_m=args.density_cell_m,
        line_radius=args.line_radius,
        line_radius_known=args.line_radius_known,
        line_radius_target=args.line_radius_target,
    )

    frags = args.artifact_dir / ("frags_train.parquet" if args.split == "train" else "frags_val.parquet")
    if not frags.exists():
        frags = frags.with_suffix(".csv")

    ds = TrackInpaintDataset(
        dataset_path=args.dataset_path,
        frags_path=frags,
        track_index_path=args.artifact_dir / "track_index.pkl",
        density_path=args.artifact_dir / "density.npz",
        grid_cfg=cfg,
        return_meta=True,
    )

    device = get_device(args.device)
    loader = make_loader(
        ds,
        batch_size=args.batch_size,
        shuffle=False,
        num_workers=args.num_workers,
        drop_last=False,
        pin_memory=(device.type == "cuda"),
    )

    model = ResUNetAttention(
        in_ch=4,
        out_ch=1,
        base_ch=args.base_ch,
        groups=args.groups,
        attn_heads=args.attn_heads,
    ).to(device)
    load_checkpoint(args.checkpoint, model=model, map_location=device)

    criterion = CombinedBCEDiceLoss().to(device)
    metrics = validate(model, loader, criterion, device=device, amp_enabled=(device.type == "cuda"), pred_thr=args.pred_thr)
    print("Eval metrics:")
    for k, v in metrics.items():
        print(f"  {k:16s}: {v:.6f}")

    preview_bs = min(args.preview_count, len(ds))
    if preview_bs <= 0:
        raise RuntimeError("Preview dataset is empty")

    preview_loader = make_loader(
        ds,
        batch_size=preview_bs,
        shuffle=False,
        num_workers=0,
        drop_last=False,
        pin_memory=(device.type == "cuda"),
    )
    batch = next(iter(preview_loader))

    print(f"preview dataset size: {len(ds)}")
    print(f"preview batch x shape: {tuple(batch['x'].shape)}")
    if "meta" in batch:
        print(f"preview batch meta count: {len(batch['meta'])}")
    x = batch["x"].to(device)
    y = batch["y"].to(device)
    with torch.no_grad():
        logits = model(x)
    save_preview_png(
        args.out_dir / "preview.png",
        x, y, logits, batch.get("meta"),
        max_items=args.preview_count,
        pred_thr=args.pred_thr,
    )
    print(f"Saved preview: {args.out_dir / 'preview.png'}")


if __name__ == "__main__":
    main()
