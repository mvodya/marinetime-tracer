from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F


class ConvGNAct(nn.Module):
    def __init__(self, in_ch: int, out_ch: int, *, k: int = 3, s: int = 1, p: int = 1, groups: int = 8):
        super().__init__()
        g = min(groups, out_ch)
        while out_ch % g != 0 and g > 1:
            g -= 1
        self.block = nn.Sequential(
            nn.Conv2d(in_ch, out_ch, kernel_size=k, stride=s, padding=p, bias=False),
            nn.GroupNorm(g, out_ch),
            nn.SiLU(inplace=True),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.block(x)


class ResBlock(nn.Module):
    def __init__(self, in_ch: int, out_ch: int, *, groups: int = 8):
        super().__init__()
        self.c1 = ConvGNAct(in_ch, out_ch, groups=groups)
        self.c2 = ConvGNAct(out_ch, out_ch, groups=groups)
        self.skip = nn.Identity() if in_ch == out_ch else nn.Conv2d(in_ch, out_ch, kernel_size=1, bias=False)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        y = self.c1(x)
        y = self.c2(y)
        return y + self.skip(x)


class Down(nn.Module):
    def __init__(self, in_ch: int, out_ch: int, *, groups: int = 8):
        super().__init__()
        self.pool = nn.MaxPool2d(2)
        self.rb = ResBlock(in_ch, out_ch, groups=groups)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.rb(self.pool(x))


class Up(nn.Module):
    def __init__(self, in_ch: int, skip_ch: int, out_ch: int, *, groups: int = 8):
        super().__init__()
        self.up = nn.Upsample(scale_factor=2, mode="bilinear", align_corners=False)
        self.rb = ResBlock(in_ch + skip_ch, out_ch, groups=groups)

    def forward(self, x: torch.Tensor, skip: torch.Tensor) -> torch.Tensor:
        x = self.up(x)
        if x.shape[-2:] != skip.shape[-2:]:
            x = F.interpolate(x, size=skip.shape[-2:], mode="bilinear", align_corners=False)
        x = torch.cat([x, skip], dim=1)
        return self.rb(x)


class SelfAttention2d(nn.Module):
    def __init__(self, channels: int, *, num_heads: int = 4):
        super().__init__()
        if channels % num_heads != 0:
            raise ValueError(f"channels={channels} must be divisible by num_heads={num_heads}")
        self.norm = nn.GroupNorm(min(8, channels), channels)
        self.attn = nn.MultiheadAttention(channels, num_heads=num_heads, batch_first=True)
        self.proj = nn.Linear(channels, channels)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        b, c, h, w = x.shape
        y = self.norm(x).flatten(2).transpose(1, 2)  # [B, HW, C]
        attn_out, _ = self.attn(y, y, y, need_weights=False)
        attn_out = self.proj(attn_out)
        attn_out = attn_out.transpose(1, 2).reshape(b, c, h, w)
        return x + attn_out


class ResUNetAttention(nn.Module):
    def __init__(
        self,
        in_ch: int = 4,
        out_ch: int = 1,
        *,
        base_ch: int = 32,
        groups: int = 8,
        attn_heads: int = 4,
    ):
        super().__init__()
        c1 = base_ch
        c2 = base_ch * 2
        c3 = base_ch * 4
        c4 = base_ch * 8

        self.in_rb = ResBlock(in_ch, c1, groups=groups)
        self.d1 = Down(c1, c2, groups=groups)
        self.d2 = Down(c2, c3, groups=groups)
        self.d3 = Down(c3, c4, groups=groups)

        self.mid1 = ResBlock(c4, c4, groups=groups)
        self.attn = SelfAttention2d(c4, num_heads=attn_heads)
        self.mid2 = ResBlock(c4, c4, groups=groups)

        self.u3 = Up(c4, c3, c3, groups=groups)
        self.u2 = Up(c3, c2, c2, groups=groups)
        self.u1 = Up(c2, c1, c1, groups=groups)

        self.out_head = nn.Sequential(
            ResBlock(c1, c1, groups=groups),
            nn.Conv2d(c1, out_ch, kernel_size=1),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x1 = self.in_rb(x)
        x2 = self.d1(x1)
        x3 = self.d2(x2)
        x4 = self.d3(x3)

        m = self.mid1(x4)
        m = self.attn(m)
        m = self.mid2(m)

        y = self.u3(m, x3)
        y = self.u2(y, x2)
        y = self.u1(y, x1)
        return self.out_head(y)
