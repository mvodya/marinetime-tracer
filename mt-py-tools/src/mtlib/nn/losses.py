from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F


class SoftDiceLoss(nn.Module):
    def __init__(self, smooth: float = 1.0):
        super().__init__()
        self.smooth = float(smooth)

    def forward(self, logits: torch.Tensor, target: torch.Tensor) -> torch.Tensor:
        prob = torch.sigmoid(logits)
        dims = (1, 2, 3)
        inter = (prob * target).sum(dim=dims)
        denom = prob.sum(dim=dims) + target.sum(dim=dims)
        dice = (2.0 * inter + self.smooth) / (denom + self.smooth)
        return 1.0 - dice.mean()


class CombinedBCEDiceLoss(nn.Module):
    def __init__(self, *, bce_weight: float = 1.0, dice_weight: float = 1.0, pos_weight: torch.Tensor | None = None):
        super().__init__()
        self.bce_weight = float(bce_weight)
        self.dice_weight = float(dice_weight)
        self.register_buffer("pos_weight", pos_weight if pos_weight is not None else None)
        self.dice = SoftDiceLoss()

    def forward(self, logits: torch.Tensor, target: torch.Tensor) -> torch.Tensor:
        bce = F.binary_cross_entropy_with_logits(logits, target, pos_weight=self.pos_weight)
        dice = self.dice(logits, target)
        return self.bce_weight * bce + self.dice_weight * dice


@torch.no_grad()
def estimate_pos_weight(loader, *, max_batches: int = 100) -> torch.Tensor:
    pos = 0.0
    neg = 0.0
    seen = 0
    for batch in loader:
        y = batch["y"]
        pos += float(y.sum().item())
        neg += float(y.numel() - y.sum().item())
        seen += 1
        if seen >= max_batches:
            break

    if pos <= 0:
        return torch.tensor(1.0, dtype=torch.float32)
    return torch.tensor(max(1.0, neg / pos), dtype=torch.float32)
