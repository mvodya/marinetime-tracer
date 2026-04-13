from __future__ import annotations

from pathlib import Path
from typing import Any

import torch
from tqdm.auto import tqdm

from .checkpoints import save_checkpoint, save_history_csv
from .metrics import compute_metrics
from .visualize import save_preview_png


def get_device(device: str | None = None) -> torch.device:
    if device:
        return torch.device(device)
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def get_amp_enabled(device: torch.device, requested: bool = True) -> bool:
    return bool(requested and device.type == "cuda")


def move_batch_to_device(batch: dict[str, Any], device: torch.device) -> tuple[torch.Tensor, torch.Tensor]:
    x = batch["x"].to(device, non_blocking=(device.type == "cuda"))
    y = batch["y"].to(device, non_blocking=(device.type == "cuda"))
    return x, y


@torch.no_grad()
def evaluate_fixed_batch(
    model,
    batch: dict[str, Any],
    *,
    device: torch.device,
    pred_thr: float = 0.5,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, list[dict[str, Any]] | None, dict[str, float]]:
    model.eval()
    x, y = move_batch_to_device(batch, device)
    logits = model(x)
    metrics = compute_metrics(logits, y, pred_thr=pred_thr)
    metas = batch.get("meta")
    return x, y, logits, metas, metrics


def train_one_epoch(
    model,
    loader,
    optimizer,
    criterion,
    *,
    device: torch.device,
    amp_enabled: bool = True,
    pred_thr: float = 0.5,
    writer=None,
    epoch: int | None = None,
    log_every: int = 20,
) -> dict[str, float]:
    model.train()
    scaler = torch.amp.GradScaler("cuda", enabled=amp_enabled)

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

        with torch.amp.autocast(device_type=device.type, enabled=amp_enabled):
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

        avg_loss = total_loss / steps
        avg_iou = total_metrics["iou"] / steps
        avg_dice = total_metrics["dice"] / steps
        pbar.set_postfix(loss=f"{avg_loss:.4f}", iou=f"{avg_iou:.4f}", dice=f"{avg_dice:.4f}")

        if writer is not None and epoch is not None and (steps % max(1, log_every) == 0):
            global_step = (epoch - 1) * len(loader) + steps
            writer.add_scalar("step/train_loss", float(loss.item()), global_step)
            writer.add_scalar("step/train_iou", float(m["iou"]), global_step)
            writer.add_scalar("step/train_dice", float(m["dice"]), global_step)
            writer.add_scalar("step/lr", float(optimizer.param_groups[0]["lr"]), global_step)

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

        with torch.amp.autocast(device_type=device.type, enabled=amp_enabled):
            logits = model(x)
            loss = criterion(logits, y)

        m = compute_metrics(logits, y, pred_thr=pred_thr)
        steps += 1
        total_loss += float(loss.item())
        for k, v in m.items():
            total_metrics[k] += float(v)

        avg_loss = total_loss / steps
        avg_iou = total_metrics["iou"] / steps
        avg_dice = total_metrics["dice"] / steps
        pbar.set_postfix(loss=f"{avg_loss:.4f}", iou=f"{avg_iou:.4f}", dice=f"{avg_dice:.4f}")

    return {
        "loss": total_loss / max(1, steps),
        **{k: v / max(1, steps) for k, v in total_metrics.items()},
    }


def _write_epoch_metrics(writer, row: dict[str, Any], epoch: int) -> None:
    for key, value in row.items():
        if key == "epoch":
            continue
        if isinstance(value, (int, float)):
            writer.add_scalar(f"epoch/{key}", float(value), epoch)


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
    writer=None,
    log_every: int = 20,
    fixed_batch: dict[str, Any] | None = None,
    fixed_preview_items: int = 4,
) -> list[dict[str, Any]]:
    out_dir = Path(out_dir)
    ckpt_dir = out_dir / "checkpoints"
    preview_dir = out_dir / "previews"
    ckpt_dir.mkdir(parents=True, exist_ok=True)
    preview_dir.mkdir(parents=True, exist_ok=True)

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
            writer=writer,
            epoch=epoch,
            log_every=log_every,
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
        save_history_csv(out_dir / "history.csv", history)

        if writer is not None:
            _write_epoch_metrics(writer, row, epoch)

        current_metric = float(row[f"val_{best_metric_name}"])
        improved = best_metric is None or current_metric > best_metric
        if improved:
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

        if fixed_batch is not None:
            x_fix, y_fix, logits_fix, metas_fix, fix_metrics = evaluate_fixed_batch(
                model,
                fixed_batch,
                device=device,
                pred_thr=pred_thr,
            )
            save_preview_png(
                preview_dir / f"epoch_{epoch:03d}.png",
                x_fix,
                y_fix,
                logits_fix,
                metas_fix,
                max_items=fixed_preview_items,
                pred_thr=pred_thr,
            )
            if writer is not None:
                _write_epoch_metrics(
                    writer,
                    {f"fixed_{k}": v for k, v in fix_metrics.items()},
                    epoch,
                )

        print(
            f"[epoch {epoch:03d}] "
            f"train_loss={tr['loss']:.4f} train_iou={tr['iou']:.4f} train_dice={tr['dice']:.4f} "
            f"val_loss={va['loss']:.4f} val_iou={va['iou']:.4f} val_dice={va['dice']:.4f}"
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


def make_summary_writer(log_dir: str | Path):
    try:
        from torch.utils.tensorboard import SummaryWriter
    except Exception:
        return None
    return SummaryWriter(log_dir=str(log_dir))
