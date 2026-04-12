from __future__ import annotations

from pathlib import Path
from typing import Any

import torch
from tqdm.auto import tqdm

from .checkpoints import save_checkpoint, save_history_csv
from .metrics import compute_metrics


def get_device(device: str | None = None) -> torch.device:
    if device:
        return torch.device(device)
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def move_batch_to_device(batch: dict[str, Any], device: torch.device) -> tuple[torch.Tensor, torch.Tensor]:
    x = batch["x"].to(device, non_blocking=True)
    y = batch["y"].to(device, non_blocking=True)
    return x, y


def train_one_epoch(
    model,
    loader,
    optimizer,
    criterion,
    *,
    device: torch.device,
    amp_enabled: bool = True,
    pred_thr: float = 0.5,
) -> dict[str, float]:
    model.train()
    scaler = torch.amp.GradScaler("cuda", enabled=(amp_enabled and device.type == "cuda"))

    total_loss = 0.0
    total_metrics: dict[str, float] = {
        "precision": 0.0,
        "recall": 0.0,
        "iou": 0.0,
        "dice": 0.0,
        "pos_frac_target": 0.0,
        "pos_frac_pred": 0.0,
    }
    steps = 0

    pbar = tqdm(loader, desc="train", leave=False)
    for batch in pbar:
        x, y = move_batch_to_device(batch, device)
        optimizer.zero_grad(set_to_none=True)

        with torch.amp.autocast(device_type=device.type, enabled=(amp_enabled and device.type in {"cuda"})):
            logits = model(x)
            loss = criterion(logits, y)

        scaler.scale(loss).backward()
        scaler.step(optimizer)
        scaler.update()

        with torch.no_grad():
            m = compute_metrics(logits, y, pred_thr=pred_thr)

        steps += 1
        total_loss += float(loss.item())
        for k, v in m.items():
            total_metrics[k] += float(v)

        pbar.set_postfix(loss=f"{total_loss / steps:.4f}", iou=f"{total_metrics['iou'] / steps:.4f}")

    return {
        "loss": total_loss / max(1, steps),
        **{k: v / max(1, steps) for k, v in total_metrics.items()},
    }


@torch.no_grad()
def validate(
    model,
    loader,
    criterion,
    *,
    device: torch.device,
    amp_enabled: bool = True,
    pred_thr: float = 0.5,
) -> dict[str, float]:
    model.eval()

    total_loss = 0.0
    total_metrics: dict[str, float] = {
        "precision": 0.0,
        "recall": 0.0,
        "iou": 0.0,
        "dice": 0.0,
        "pos_frac_target": 0.0,
        "pos_frac_pred": 0.0,
    }
    steps = 0

    pbar = tqdm(loader, desc="val", leave=False)
    for batch in pbar:
        x, y = move_batch_to_device(batch, device)

        with torch.amp.autocast(device_type=device.type, enabled=(amp_enabled and device.type in {"cuda"})):
            logits = model(x)
            loss = criterion(logits, y)

        m = compute_metrics(logits, y, pred_thr=pred_thr)
        steps += 1
        total_loss += float(loss.item())
        for k, v in m.items():
            total_metrics[k] += float(v)

        pbar.set_postfix(loss=f"{total_loss / steps:.4f}", iou=f"{total_metrics['iou'] / steps:.4f}")

    return {
        "loss": total_loss / max(1, steps),
        **{k: v / max(1, steps) for k, v in total_metrics.items()},
    }


def fit(
    model,
    train_loader,
    val_loader,
    optimizer,
    criterion,
    *,
    out_dir: str | Path,
    epochs: int,
    device: torch.device,
    amp_enabled: bool = True,
    pred_thr: float = 0.5,
    save_every: int = 1,
    best_metric_name: str = "iou",
) -> list[dict[str, Any]]:
    out_dir = Path(out_dir)
    ckpt_dir = out_dir / "checkpoints"
    ckpt_dir.mkdir(parents=True, exist_ok=True)

    history: list[dict[str, Any]] = []
    best_metric = None

    model.to(device)

    for epoch in range(1, epochs + 1):
        tr = train_one_epoch(
            model,
            train_loader,
            optimizer,
            criterion,
            device=device,
            amp_enabled=amp_enabled,
            pred_thr=pred_thr,
        )
        va = validate(
            model,
            val_loader,
            criterion,
            device=device,
            amp_enabled=amp_enabled,
            pred_thr=pred_thr,
        )

        row = {
            "epoch": epoch,
            "train_loss": tr["loss"],
            "train_precision": tr["precision"],
            "train_recall": tr["recall"],
            "train_iou": tr["iou"],
            "train_dice": tr["dice"],
            "train_pos_frac_target": tr["pos_frac_target"],
            "train_pos_frac_pred": tr["pos_frac_pred"],
            "val_loss": va["loss"],
            "val_precision": va["precision"],
            "val_recall": va["recall"],
            "val_iou": va["iou"],
            "val_dice": va["dice"],
            "val_pos_frac_target": va["pos_frac_target"],
            "val_pos_frac_pred": va["pos_frac_pred"],
        }
        history.append(row)

        current_metric = float(row[f"val_{best_metric_name}"])
        if best_metric is None or current_metric > best_metric:
            best_metric = current_metric
            save_checkpoint(
                ckpt_dir / "best.pt",
                epoch=epoch,
                model=model,
                optimizer=optimizer,
                history=history,
                best_metric=best_metric,
                extra={"best_metric_name": best_metric_name},
            )

        if save_every > 0 and epoch % save_every == 0:
            save_checkpoint(
                ckpt_dir / f"epoch_{epoch:03d}.pt",
                epoch=epoch,
                model=model,
                optimizer=optimizer,
                history=history,
                best_metric=best_metric,
                extra={"best_metric_name": best_metric_name},
            )

        save_history_csv(out_dir / "history.csv", history)

        print(
            f"[epoch {epoch:03d}] "
            f"train_loss={tr['loss']:.4f} train_iou={tr['iou']:.4f} "
            f"val_loss={va['loss']:.4f} val_iou={va['iou']:.4f}"
        )

    save_checkpoint(
        ckpt_dir / "last.pt",
        epoch=epochs,
        model=model,
        optimizer=optimizer,
        history=history,
        best_metric=best_metric,
        extra={"best_metric_name": best_metric_name},
    )
    return history
