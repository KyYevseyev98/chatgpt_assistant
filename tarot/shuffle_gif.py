# tarot/shuffle_gif.py
from __future__ import annotations

import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple, Optional, List, Dict

from PIL import Image, ImageFilter, ImageEnhance, ImageOps

CARD_ASPECT = 1.69  # height/width


# ---------------------------
# easing / math helpers
# ---------------------------
def clamp(v: float, a: float, b: float) -> float:
    return max(a, min(b, v))


def smoothstep(t: float) -> float:
    t = clamp(t, 0.0, 1.0)
    return t * t * (3 - 2 * t)


def ease_in_out(t: float) -> float:
    t = clamp(t, 0.0, 1.0)
    return 0.5 - 0.5 * math.cos(math.pi * t)


def ease_out(t: float) -> float:
    t = clamp(t, 0.0, 1.0)
    return 1 - (1 - t) * (1 - t)


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def rects_intersect(a: Tuple[int, int, int, int], b: Tuple[int, int, int, int]) -> bool:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    return not (ax2 <= bx1 or bx2 <= ax1 or ay2 <= by1 or by2 <= ay1)


# ---------------------------
# rendering helpers
# ---------------------------
def make_shadow(img_rgba: Image.Image, blur: int, offset: Tuple[int, int], opacity: int) -> Image.Image:
    w, h = img_rgba.size
    shadow = Image.new("RGBA", (w + blur * 4, h + blur * 4), (0, 0, 0, 0))
    alpha = img_rgba.split()[-1]

    sil = Image.new("RGBA", (w, h), (0, 0, 0, opacity))
    sil.putalpha(alpha)

    x = blur * 2 + offset[0]
    y = blur * 2 + offset[1]
    shadow.alpha_composite(sil, (x, y))
    return shadow.filter(ImageFilter.GaussianBlur(blur))


def premium_card(back_rgb: Image.Image, target_w: int) -> Image.Image:
    target_h = int(target_w * CARD_ASPECT)

    card = ImageOps.fit(
        back_rgb.convert("RGB"),
        (target_w, target_h),
        method=Image.LANCZOS,
        centering=(0.5, 0.5),
    ).convert("RGBA")

    card = ImageEnhance.Contrast(card).enhance(1.06)
    card = card.filter(ImageFilter.UnsharpMask(radius=1.2, percent=115, threshold=3))

    radius = max(10, target_w // 18)
    mask = Image.new("L", card.size, 0)
    try:
        from PIL import ImageDraw
        d = ImageDraw.Draw(mask)
        d.rounded_rectangle((0, 0, card.size[0], card.size[1]), radius=radius, fill=255)
    except Exception:
        mask = Image.new("L", card.size, 255)
    card.putalpha(mask)

    border = max(6, target_w // 90)
    framed = Image.new("RGBA", (card.size[0] + border * 2, card.size[1] + border * 2), (0, 0, 0, 0))

    frame = Image.new("RGBA", framed.size, (245, 245, 245, 255))
    frame_mask = Image.new("L", framed.size, 0)
    try:
        from PIL import ImageDraw
        d2 = ImageDraw.Draw(frame_mask)
        d2.rounded_rectangle((0, 0, framed.size[0], framed.size[1]), radius=radius + border, fill=255)
    except Exception:
        frame_mask = Image.new("L", framed.size, 255)

    frame.putalpha(frame_mask)
    framed.alpha_composite(frame, (0, 0))
    framed.alpha_composite(card, (border, border))

    shadow = make_shadow(
        framed,
        blur=max(10, target_w // 35),
        offset=(target_w // 60, target_w // 50),
        opacity=135,
    )

    layer = Image.new("RGBA", shadow.size, (0, 0, 0, 0))
    layer.alpha_composite(shadow, (0, 0))
    layer.alpha_composite(
        framed,
        (shadow.size[0] // 2 - framed.size[0] // 2, shadow.size[1] // 2 - framed.size[1] // 2),
    )
    return layer


# ---------------------------
# animation model
# ---------------------------
@dataclass(frozen=True)
class Flyer:
    # scatter target
    tx: float
    ty: float
    # weave target near center (causes overlaps)
    mx: float
    my: float
    # lift
    lift: float
    # rotations
    rot0: float
    rotm: float
    rot1: float
    # scaling
    s0: float
    sm: float
    s1: float
    # timing offset
    delay: float
    # small bias for depth feel (continuous)
    depth_bias: float


@dataclass
class Pose:
    idx: int
    x: float
    y: float
    rot: float
    scale: float
    bbox: Tuple[int, int, int, int]
    depth_key: float  # continuous, no flips
    img: Image.Image


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _make_glow(W: int, H: int, cx: int, cy: int) -> Image.Image:
    glow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    try:
        from PIL import ImageDraw
        g = ImageDraw.Draw(glow)
        r = int(W * 0.19)
        g.ellipse((cx - r, cy - r, cx + r, cy + r), fill=(255, 255, 255, 18))
        glow = glow.filter(ImageFilter.GaussianBlur(int(W * 0.035)))
    except Exception:
        pass
    return glow


def _phase_weights(t: float) -> Tuple[float, float, float]:
    """
    (scatter_phase, weave_phase, stack_phase) in 0..1
    0.00..0.35 scatter
    0.35..0.80 weave
    0.80..1.00 stack
    """
    t = clamp(t, 0.0, 1.0)
    p1_end = 0.35
    p2_end = 0.80

    scatter = smoothstep(clamp(t / p1_end, 0.0, 1.0))
    weave = smoothstep(clamp((t - p1_end) / (p2_end - p1_end), 0.0, 1.0))
    stack = smoothstep(clamp((t - p2_end) / (1.0 - p2_end), 0.0, 1.0))
    return scatter, weave, stack


def _flyer_pose(f: Flyer, cx: float, cy: float, t: float) -> Tuple[float, float, float, float]:
    """
    Continuous path without hard jumps:
    center -> scatter -> weave -> center
    using phase weights but blended smoothly.
    """
    scatter, weave, stack = _phase_weights(t)

    # positions
    # A: center -> scatter
    ax = lerp(cx, f.tx, scatter)
    ay = lerp(cy, f.ty, scatter)

    # B: scatter -> weave target
    bx = lerp(f.tx, f.mx, weave)
    by = lerp(f.ty, f.my, weave)

    # C: weave -> center
    cx2 = lerp(f.mx, cx, stack)
    cy2 = lerp(f.my, cy, stack)

    # blend segments smoothly
    # blend A->B around t~0.35, then B->C around t~0.80
    # use weights to avoid discontinuities
    # wA dominates early, wB mid, wC late
    wA = 1.0 - weave
    wC = stack
    wB = 1.0 - wA - wC
    wB = clamp(wB, 0.0, 1.0)

    x = ax * wA + bx * wB + cx2 * wC
    y = ay * wA + by * wB + cy2 * wC

    # arc lift hump
    hump = math.sin(math.pi * clamp((t - 0.05) / 0.90, 0.0, 1.0))
    y += -f.lift * (hump ** 1.15)

    # rotation/scale continuous blend
    # early: rot0 -> rotm, mid: rotm -> rot1, end: rot1 -> 0
    if t <= 0.35:
        rot = lerp(f.rot0, f.rotm, scatter)
        scale = lerp(f.s0, f.sm, scatter)
    elif t <= 0.80:
        rot = lerp(f.rotm, f.rot1, weave)
        scale = lerp(f.sm, f.s1, weave)
    else:
        rot = lerp(f.rot1, 0.0, stack)
        scale = lerp(f.s1, 1.0, stack)

    return x, y, rot, scale


def _cut_progress(t: float, cut_window: Tuple[float, float]) -> float:
    cut_start, cut_end = cut_window
    if t <= cut_start or t >= cut_end:
        return 0.0
    u = (t - cut_start) / (cut_end - cut_start)
    return math.sin(math.pi * u)  # 0..1..0


def _build_pose(
    idx: int,
    card_layer: Image.Image,
    x: float,
    y: float,
    rot: float,
    scale: float,
    depth_key: float,
) -> Pose:
    lw, lh = card_layer.size
    layer2 = card_layer.resize((max(1, int(lw * scale)), max(1, int(lh * scale))), Image.LANCZOS)
    layer2 = layer2.rotate(rot, resample=Image.BICUBIC, expand=True)

    px = int(x - layer2.size[0] / 2)
    py = int(y - layer2.size[1] / 2)
    bbox = (px, py, px + layer2.size[0], py + layer2.size[1])

    return Pose(
        idx=idx,
        x=x,
        y=y,
        rot=rot,
        scale=scale,
        bbox=bbox,
        depth_key=depth_key,
        img=layer2,
    )


def _stabilized_order(
    current_order: List[int],
    desired_order: List[int],
    bboxes: Dict[int, Tuple[int, int, int, int]],
) -> List[int]:
    """
    Key trick:
    - We want to move current_order toward desired_order,
      BUT we DO NOT allow swapping two items if their bboxes intersect.
    - This removes the "magic" layer teleport while cards overlap.
    """
    if current_order == desired_order:
        return current_order

    desired_rank = {idx: r for r, idx in enumerate(desired_order)}
    order = current_order[:]

    # multiple bubble passes, limited
    max_passes = 6
    for _ in range(max_passes):
        changed = False
        for i in range(len(order) - 1):
            a = order[i]
            b = order[i + 1]
            # should a be after b in desired?
            if desired_rank[a] > desired_rank[b]:
                # swap only if NO intersection
                if not rects_intersect(bboxes[a], bboxes[b]):
                    order[i], order[i + 1] = order[i + 1], order[i]
                    changed = True
        if not changed:
            break

    return order


def render_shuffle_gif(
    table_path: str,
    card_back_path: str,
    out_path: str,
    *,
    seconds: float = 2.0,
    fps: int = 18,
    deck_width_ratio: float = 0.23,
    seed: Optional[int] = 7,
    flying_cards: int = 12,
    deck_stack_layers: int = 10,
    scatter_radius_ratio: float = 0.30,
    cut_strength_ratio: float = 0.13,
    cut_window: Tuple[float, float] = (0.40, 0.74),
) -> str:
    """
    Shuffle without "magic" layer swaps:
    - Flyers can overlap visually.
    - But their draw order only changes when they are NOT overlapping.
    """
    if seed is not None:
        random.seed(seed)

    table = Image.open(table_path).convert("RGBA")
    W, H = table.size

    back = Image.open(card_back_path).convert("RGB")
    deck_w = int(W * deck_width_ratio)
    card_layer = premium_card(back, deck_w)

    center_x, center_y = int(W * 0.50), int(H * 0.56)
    glow = _make_glow(W, H, center_x, center_y)

    total = max(2, int(seconds * fps))
    frames: List[Image.Image] = []

    # Flyers (fixed for entire anim)
    scatter_R = W * scatter_radius_ratio
    flyers: List[Flyer] = []
    for i in range(flying_cards):
        ang = random.uniform(-math.pi, math.pi)
        R = random.uniform(scatter_R * 0.70, scatter_R * 1.05)

        tx = center_x + math.cos(ang) * R
        ty = center_y + math.sin(ang) * (R * 0.70)

        # weave mid targets around center, alternating sides, to create overlaps
        side = -1 if (i % 2 == 0) else 1
        mx = center_x + side * random.uniform(W * 0.05, W * 0.13)
        my = center_y + random.uniform(-H * 0.04, H * 0.06)

        lift = random.uniform(W * 0.040, W * 0.075)

        rot0 = random.uniform(-6, 6)
        rotm = random.uniform(-38, 38)
        rot1 = random.uniform(-22, 22)

        s0 = 1.00
        sm = random.uniform(1.03, 1.07)
        s1 = random.uniform(1.01, 1.05)

        delay = random.uniform(0.00, 0.16)
        depth_bias = random.uniform(-0.35, 0.35)

        flyers.append(Flyer(tx, ty, mx, my, lift, rot0, rotm, rot1, s0, sm, s1, delay, depth_bias))

    # stable draw order state for flyers
    flyer_order = list(range(flying_cards))

    # Cut params
    cut_dist = W * cut_strength_ratio

    for fi in range(total):
        t = fi / (total - 1)  # 0..1

        base = table.copy()
        base.alpha_composite(glow)

        # ---------------------------
        # DECK CUT (two mini stacks swapping)
        # ---------------------------
        cp = _cut_progress(t, cut_window)  # 0..1..0
        left_shift = +cut_dist * cp
        right_shift = -cut_dist * cp
        left_rot = -3.5 * cp
        right_rot = +3.5 * cp

        half = max(2, deck_stack_layers // 2)
        rest = deck_stack_layers - half

        settle_rot = math.sin(t * math.pi) * 0.8
        settle_y = -math.sin(t * math.pi) * (W * 0.002)

        # left stack (bottom)
        for k in range(half):
            kf = k / max(1, half - 1)
            dy = (k * 2) + (kf * 1)
            dx = -3 + (kf * 2)

            layer2 = card_layer.rotate(left_rot + settle_rot * 0.2, resample=Image.BICUBIC, expand=True)
            x = int(center_x - layer2.size[0] / 2 + left_shift + dx)
            y = int(center_y - layer2.size[1] / 2 + dy + settle_y)
            base.alpha_composite(layer2, (x, y))

        # right stack (top)
        for k in range(rest):
            kf = k / max(1, rest - 1)
            dy = (k * 2) + (kf * 1)
            dx = +3 - (kf * 2)

            layer2 = card_layer.rotate(right_rot + settle_rot * 0.2, resample=Image.BICUBIC, expand=True)
            x = int(center_x - layer2.size[0] / 2 + right_shift + dx)
            y = int(center_y - layer2.size[1] / 2 + dy + settle_y)
            base.alpha_composite(layer2, (x, y))

        # ---------------------------
        # FLYERS (no magic z swaps)
        # ---------------------------
        poses: List[Pose] = []
        bboxes: Dict[int, Tuple[int, int, int, int]] = {}

        for idx, f in enumerate(flyers):
            # per-flyer delayed time
            tt = clamp((t - f.delay) / max(1e-6, (1.0 - f.delay)), 0.0, 1.0)

            x, y, rot, scale = _flyer_pose(f, center_x, center_y, tt)

            # continuous depth key (NO flips):
            # mainly by y (lower on screen feels closer) + tiny bias + gentle wave
            depth_wave = 0.12 * math.sin((tt * 2.0 * math.pi) + idx * 0.7)
            depth_key = (y / H) + f.depth_bias + depth_wave

            pose = _build_pose(idx, card_layer, x, y, rot, scale, depth_key)
            poses.append(pose)
            bboxes[idx] = pose.bbox

        # desired order by depth_key (continuous) then y then idx
        desired = sorted(
            range(flying_cards),
            key=lambda i: (poses[i].depth_key, poses[i].y, i)
        )

        # update flyer_order with NO-overlap swap rule
        flyer_order = _stabilized_order(flyer_order, desired, bboxes)

        # draw flyers in stabilized order
        for idx in flyer_order:
            p = poses[idx]
            px, py = p.bbox[0], p.bbox[1]
            base.alpha_composite(p.img, (px, py))

        # ---------------------------
        # Motion blur (mid action), slight
        # ---------------------------
        action = math.sin(math.pi * clamp((t - 0.10) / 0.85, 0.0, 1.0))
        blur_amt = int(clamp(action * 2.2, 0, 3))
        if blur_amt > 0:
            base = base.filter(ImageFilter.GaussianBlur(blur_amt))

        base = ImageEnhance.Contrast(base).enhance(1.03)
        frames.append(base.convert("P", palette=Image.ADAPTIVE))

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    duration = int(1000 / fps)
    frames[0].save(
        str(out),
        save_all=True,
        append_images=frames[1:],
        duration=duration,
        loop=0,
        optimize=True,
        disposal=2,
    )
    return str(out)


if __name__ == "__main__":
    ROOT = _project_root()

    table = ROOT / "assets" / "table" / "table.jpg"
    back = ROOT / "assets" / "cards_back" / "back.jpg"
    out = ROOT / "shuffle.gif"  # В КОРЕНЬ ПРОЕКТА

    if not table.is_file():
        raise FileNotFoundError(f"Table not found: {table}")
    if not back.is_file():
        raise FileNotFoundError(f"Card back not found: {back}")

    result = render_shuffle_gif(
        table_path=str(table),
        card_back_path=str(back),
        out_path=str(out),

        # ДЛИННЕЕ — чтобы успевали "переложиться" и перекрыться
        seconds=2.4,          # было 2.0
        fps=20,               # чуть плавнее, можно оставить 18

        deck_width_ratio=0.23,
        seed=7,

        # БОЛЬШЕ "летающих" — больше столкновений/перекрытий
        flying_cards=16,      # было 12

        # МЕНЬШЕ "толщины" колоды — иначе она визуально съедает эффект перекрытий
        deck_stack_layers=4,  # было 10 (ключевой фикс)

        #ШИРЕ разлёт
        scatter_radius_ratio=0.42,  # было 0.30 (ставь 0.38..0.50)

        # Срез колоды — чуть сильнее и чуть длиннее, чтобы было ощущение "mix"
        cut_strength_ratio=0.16,    # было 0.13 (0.14..0.20)
        cut_window=(0.36, 0.82),    # было (0.40, 0.74) — дольше фаза "mix"
    )

    print(f"OK: {result}")