from __future__ import annotations

import torch


@torch.no_grad()
def compute_metrics(logits: torch.Tensor, target: torch.Tensor, *, pred_thr: float = 0.5) -> dict[str, float]:
    prob = torch.sigmoid(logits)
    pred = (prob >= pred_thr).float()

    dims = (1, 2, 3)
    tp = (pred * target).sum(dim=dims)
    fp = (pred * (1.0 - target)).sum(dim=dims)
    fn = ((1.0 - pred) * target).sum(dim=dims)

    precision = (tp / (tp + fp + 1e-8)).mean().item()
    recall = (tp / (tp + fn + 1e-8)).mean().item()
    iou = (tp / (tp + fp + fn + 1e-8)).mean().item()
    dice = ((2.0 * tp) / (2.0 * tp + fp + fn + 1e-8)).mean().item()

    pos_frac_target = target.mean().item()
    pos_frac_pred = pred.mean().item()

    return {
        "precision": float(precision),
        "recall": float(recall),
        "iou": float(iou),
        "dice": float(dice),
        "pos_frac_target": float(pos_frac_target),
        "pos_frac_pred": float(pos_frac_pred),
    }
