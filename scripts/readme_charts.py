#!/usr/bin/env python3
"""Render the README benchmark charts from docs/assets/charts/benchmarks.json.

Charts are SVGs with text converted to paths, so they render identically on
GitHub regardless of installed fonts. Each chart sits on its own seine "deep"
card, which reads correctly in both GitHub light and dark themes.

    python3 scripts/readme_charts.py [--font-dir DIR] [--out docs/assets/charts]

Fonts: IBM Plex Sans + IBM Plex Mono (OFL). Pass --font-dir if they are not
installed system-wide; DejaVu is used as a fallback.
"""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib import font_manager  # noqa: E402
from matplotlib.patches import FancyBboxPatch, PathPatch  # noqa: E402
from matplotlib.path import Path as MPath  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent

# Brand tokens (docs/BRAND.md).
DEEP = "#042329"
GRID = "#0E3B40"
FOAM = "#DFFBF6"
MUTED = "#8CC9C2"
FAINT = "#6FA9A2"
LIME = "#AAFF00"

# Categorical slots, validated against DEEP (dataviz validate_palette.js, dark mode).
CATEGORY = {
    "amd": ("AMD", "#E5603E"),
    "intel": ("Intel", "#2F86E8"),
    "arm": ("Arm", "#BD8209"),
    "apple": ("Apple", "#9473EE"),
    "nvidia": ("NVIDIA", "#3F9C5A"),
}

SANS = "IBM Plex Sans"
DISPLAY = "Bricolage Grotesque"
MONO = "IBM Plex Mono"

PX = 1 / 100  # figure units: 100 px per inch


def load_fonts(font_dir):
    if font_dir:
        for f in Path(font_dir).glob("*.[ot]tf"):
            font_manager.fontManager.addfont(str(f))
    names = {f.name for f in font_manager.fontManager.ttflist}
    sans = SANS if SANS in names else "DejaVu Sans"
    mono = MONO if MONO in names else "DejaVu Sans Mono"
    plt.rcParams.update({"svg.fonttype": "path", "font.family": sans})
    display = DISPLAY if DISPLAY in names else sans
    return sans, mono, display


def bar(ax, y, width, height, color, radius):
    """Horizontal bar: square at the baseline, rounded at the data end."""
    r = min(radius, width / 2, height / 2)
    x0, x1, y0, y1 = 0, width, y - height / 2, y + height / 2
    k = 0.5523 * r
    verts = [
        (x0, y0), (x1 - r, y0),
        (x1 - r + k, y0), (x1, y0 + r - k), (x1, y0 + r),
        (x1, y1 - r),
        (x1, y1 - r + k), (x1 - r + k, y1), (x1 - r, y1),
        (x0, y1), (x0, y0),
    ]
    codes = [MPath.MOVETO, MPath.LINETO,
             MPath.CURVE4, MPath.CURVE4, MPath.CURVE4,
             MPath.LINETO,
             MPath.CURVE4, MPath.CURVE4, MPath.CURVE4,
             MPath.LINETO, MPath.CLOSEPOLY]
    ax.add_patch(PathPatch(MPath(verts, codes), facecolor=color, edgecolor="none"))


class Card:
    """A fixed-pixel canvas: deep rounded card, title block, plot area in px."""

    def __init__(self, width, height, title, subtitle, fonts):
        self.w, self.h = width, height
        self.sans, self.mono, self.display = fonts
        self.fig = plt.figure(figsize=(width * PX, height * PX), dpi=100)
        self.fig.patch.set_alpha(0)
        bg = self.fig.add_axes([0, 0, 1, 1])
        bg.set_xlim(0, width)
        bg.set_ylim(height, 0)
        bg.axis("off")
        bg.patch.set_alpha(0)
        base = self.fig.add_axes([0, 0, 1, 1], zorder=-2)
        base.set_xlim(0, width)
        base.set_ylim(height, 0)
        base.axis("off")
        base.add_patch(FancyBboxPatch((1, 1), width - 2, height - 2,
                                    boxstyle="round,pad=0,rounding_size=18",
                                    facecolor=DEEP, edgecolor="none"))
        self.bg = bg
        self.bg.text(32, 46, title, fontsize=25 * 0.75, color=FOAM, fontfamily=self.display, fontweight="heavy")
        self.text(32, 72, subtitle, 13.5, MUTED)

    def text(self, x, y, s, size, color, weight="normal", mono=False, ha="left", va="baseline",
             knockout=False):
        box = dict(facecolor=DEEP, edgecolor="none", pad=1.5) if knockout else None
        self.bg.text(x, y, s, fontsize=size * 0.75, color=color, ha=ha, va=va, bbox=box,
                     fontfamily=self.mono if mono else self.sans, fontweight=weight, zorder=5)

    def plot_axes(self, left, top, right, bottom):
        ax = self.fig.add_axes([left / self.w, 1 - bottom / self.h,
                                (right - left) / self.w, (bottom - top) / self.h])
        ax.patch.set_alpha(0)
        ax.set_zorder(-1)
        for s in ax.spines.values():
            s.set_visible(False)
        return ax

    def save(self, path):
        self.fig.savefig(path, format="svg", transparent=True, metadata={"Date": None})
        path.write_text("\n".join(line.rstrip() for line in path.read_text().splitlines()) + "\n")
        plt.close(self.fig)


def hbar_chart(rows, title, subtitle, footnote, out, fonts, groups=True, big=False):
    """Grouped horizontal bars. rows: dicts with name, sub, category, hps."""
    label_w, group_gap = 300, 34
    row_h = 54 if big else 44
    order = []
    if groups:
        seen = []
        for r in rows:
            if r["category"] not in seen:
                seen.append(r["category"])
        for c in seen:
            members = sorted([r for r in rows if r["category"] == c], key=lambda r: -r["hps"])
            order.append((c, members))
    else:
        order.append((None, sorted(rows, key=lambda r: -r["hps"])))
    n_rows = sum(len(m) for _, m in order)
    n_groups = len(order) if groups else 0
    top = 104
    height = top + n_rows * row_h + n_groups * group_gap + 96
    width = 900
    card = Card(width, height, title, subtitle, fonts)
    plot_left, plot_right = label_w + 32, width - 96
    ax = card.plot_axes(plot_left, top, plot_right, height - 84)
    vmax = max(r["hps"] for r in rows)
    step = 10 if vmax > 20 else 5
    xmax = (int(vmax / step) + 1) * step
    ax.set_xlim(0, xmax)
    plot_h = height - 84 - top
    ax.set_ylim(plot_h, 0)
    ax.set_yticks([])
    ticks = list(range(0, xmax + 1, step))
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{t}" for t in ticks])
    ax.tick_params(axis="x", length=0, colors=FAINT, labelsize=11 * 0.75, pad=8)
    for lab in ax.get_xticklabels():
        lab.set_fontfamily(card.mono)
    for t in ticks:
        ax.axvline(t, color=GRID, lw=1 * 0.75, zorder=0)
    sx = (plot_right - plot_left) / xmax  # px per H/s

    y = 0
    for cat, members in order:
        if cat is not None:
            y += group_gap
            name, color = CATEGORY[cat]
            card.bg.add_patch(FancyBboxPatch((32, top + y - 21), 10, 10,
                                             boxstyle="round,pad=0,rounding_size=2.5",
                                             facecolor=color, edgecolor="none"))
            card.text(50, top + y - 11.5, name.upper(), 11.5, MUTED, weight="semibold")
        for r in members:
            cy = y + row_h / 2
            color = CATEGORY[r["category"]][1]
            bar(ax, cy, r["hps"], 20, color, radius=4 / sx)
            if big:
                card.bg.text(32, top + cy + 1, r["name"], fontsize=21 * 0.75, color=FOAM,
                             fontfamily=card.display, fontweight="heavy")
                card.text(32, top + cy + 18, r["sub"], 11.5, FAINT, mono=True)
            else:
                card.text(32, top + cy - 1, r["name"], 15, FOAM, weight="medium")
                card.text(32, top + cy + 15, r["sub"], 11.5, FAINT)
            card.text(plot_left + r["hps"] * sx + 10, top + cy + 5.5,
                      f"{r['hps']:.1f}", 15, FOAM, weight="medium", mono=True, knockout=True)
            y += row_h
    card.text(plot_right + 74, height - 60, "H/s", 11, FAINT, mono=True, ha="right")  # axis unit
    card.text(32, height - 24, footnote, 11, FAINT)
    card.save(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data", default=ROOT / "docs/assets/charts/benchmarks.json", type=Path)
    ap.add_argument("--out", default=ROOT / "docs/assets/charts", type=Path)
    ap.add_argument("--font-dir")
    args = ap.parse_args()
    fonts = load_fonts(args.font_dir)
    data = json.loads(args.data.read_text())
    args.out.mkdir(parents=True, exist_ok=True)

    if data.get("servers"):
        hbar_chart(data["servers"], "Servers",
                   "Best measured hashrate per CPU generation on AWS, seine v0.2.22+",
                   "AWS guests; huge-page backing and measurement windows vary. Method: docs/PERFORMANCE.md",
                   args.out / "servers.svg", fonts)
    if data.get("bare_metal"):
        hbar_chart(data["bare_metal"], "AMD EPYC bare metal",
                   "Whole-invocation hashrate, fixed manual configuration",
                   "9355P: 192 GiB DDR5-4800, 12 channels. v0.2.39, two runs. Method: docs/PERFORMANCE.md",
                   args.out / "bare-metal.svg", fonts, groups=False)
    if data.get("best"):
        hbar_chart(data["best"], "Best by generation",
                   "Peak measured hashrate in the stated configurations",
                   "Bare-metal EPYC and AWS guests. Full settings: docs/PERFORMANCE.md",
                   args.out / "best.svg", fonts, groups=False, big=True)
    hbar_chart(data["desktop"], "Desktops and laptops",
               "Whole-machine CPU hashrate, seine v0.2.22+",
               "Stock hardware. Method: docs/PERFORMANCE.md",
               args.out / "desktop.svg", fonts, groups=False)


if __name__ == "__main__":
    main()
