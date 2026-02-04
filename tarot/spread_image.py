# tarot/spread_image.py
from __future__ import annotations

import os
import random
from typing import List, Tuple, Dict, Optional

from PIL import Image, ImageOps, ImageFilter, ImageDraw

# ------------------------------------------------------------
# Премиум-выкладка (1–7 карт) на квадратном столе (например 1024x1024)
# ------------------------------------------------------------
#
# slot: "big" | "mid" | "sm"
# cx, cy: центр позиции (в долях от стола)
# rot: базовый поворот (градусы). Если 0 — можно добавить микро-рандом.
#
_LAYOUTS: Dict[int, List[Tuple[str, float, float, int]]] = {
    # 1: одна крупная по центру
    1: [("big", 0.50, 0.52, 0)],

    # 2: две рядом (слегка выше центра)
    2: [("mid", 0.30, 0.50, -1), ("mid", 0.70, 0.50, 1)],

    # 3: классическая линия
    3: [("mid", 0.20, 0.50, -2), ("mid", 0.50, 0.50, 0), ("mid", 0.80, 0.50, 2)],

    # 4: 2x2
    4: [
        ("sm", 0.36, 0.27, -1), ("sm", 0.64, 0.27, 1),
        ("sm", 0.36, 0.73, 1),  ("sm", 0.64, 0.73, -1),
    ],

    # 5: КРЕСТ (центр + верх/низ/лево/право)
    # центр чуть больше визуально за счёт "mid" и позиции
    5: [
        ("sm", 0.36, 0.27, -1), ("sm", 0.64, 0.27, 1),
        ("sm", 0.26, 0.73, 1),  ("sm", 0.50, 0.73, 0), ("sm", 0.74, 0.73, -1),
    ],

    # 6: 3x2
    6: [
        ("sm", 0.26, 0.27, -1), ("sm", 0.50, 0.27, 0), ("sm", 0.74, 0.27, 1),
        ("sm", 0.26, 0.73, 1),  ("sm", 0.50, 0.73, 0), ("sm", 0.74, 0.73, -1),
    ],

    # 7: верхняя линия 3 + нижняя линия 4 (премиум сетка)
    7: [
        ("sm", 0.26, 0.27, -1), ("sm", 0.50, 0.27, 0), ("sm", 0.74, 0.27, 1),
        ("sm", 0.20, 0.73, -1), ("sm", 0.40, 0.73, 0), ("sm", 0.60, 0.73, 0), ("sm", 0.80, 0.73, 1),
    ],
}

# Размер карты как доля ширины стола
# (под 1024x1024 выглядит “дорого” и не тесно)
_SIZE_MAP = {
    "big": 0.47,
    "mid": 0.34,
    "sm":  0.26,
}

# Пропорция карт (высота / ширина). Для RWS обычно близко к ~1.69
_CARD_ASPECT = 1.69


def _rounded_corners_mask(size: Tuple[int, int], radius: int) -> Image.Image:
    """Маска с закруглёнными углами (L)."""
    w, h = size
    radius = max(1, min(radius, min(w, h) // 2))
    mask = Image.new("L", (w, h), 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle((0, 0, w, h), radius=radius, fill=255)
    return mask


def _make_shadow(rgba_card: Image.Image, blur: int, offset: Tuple[int, int], opacity: int) -> Image.Image:
    """
    Тень отдельным слоем. opacity 0..255.
    """
    w, h = rgba_card.size
    shadow = Image.new("RGBA", (w + abs(offset[0]) + blur * 2, h + abs(offset[1]) + blur * 2), (0, 0, 0, 0))

    # силуэт тени: берём альфу карты
    alpha = rgba_card.split()[-1]
    shadow_sil = Image.new("RGBA", rgba_card.size, (0, 0, 0, opacity))
    shadow_sil.putalpha(alpha)

    # размещаем с учётом blur
    x = blur + max(offset[0], 0)
    y = blur + max(offset[1], 0)
    shadow.alpha_composite(shadow_sil, (x, y))

    shadow = shadow.filter(ImageFilter.GaussianBlur(blur))
    return shadow


def _premium_card_layer(
    card_rgb: Image.Image,
    *,
    corner_radius: int,
    border: int,
    border_color: Tuple[int, int, int],
    shadow_blur: int,
    shadow_offset: Tuple[int, int],
    shadow_opacity: int,
) -> Image.Image:
    """
    Делает премиум слой карты:
    - закругления
    - тонкая рамка
    - мягкая тень
    Возвращает RGBA.
    """
    # базовая карта в RGBA
    card = card_rgb.convert("RGBA")

    # закругление
    mask = _rounded_corners_mask(card.size, corner_radius)
    card.putalpha(mask)

    # рамка: делаем расширение и рисуем rounded_rectangle поверх
    w, h = card.size
    out_w, out_h = w + border * 2, h + border * 2
    framed = Image.new("RGBA", (out_w, out_h), (0, 0, 0, 0))

    # фон рамки (rounded)
    frame_mask = _rounded_corners_mask((out_w, out_h), corner_radius + border)
    frame = Image.new("RGBA", (out_w, out_h), (*border_color, 255))
    frame.putalpha(frame_mask)
    framed.alpha_composite(frame, (0, 0))

    # вклеиваем карту в центр
    framed.alpha_composite(card, (border, border))

    # тень под рамкой
    shadow = _make_shadow(framed, blur=shadow_blur, offset=shadow_offset, opacity=shadow_opacity)

    # итоговый слой: тень + рамка+карта
    layer = Image.new("RGBA", shadow.size, (0, 0, 0, 0))
    layer.alpha_composite(shadow, (0, 0))

    # позиция рамки поверх тени
    # тень расширена blur*2 и offset, поэтому ставим с учётом blur
    paste_x = shadow_blur
    paste_y = shadow_blur
    layer.alpha_composite(framed, (paste_x, paste_y))

    return layer


def _fit_card(card: Image.Image, target_w: int, target_h: int) -> Image.Image:
    """
    Вписать с кропом (чтобы все карты были одинаковыми и выглядели “чисто”).
    """
    return ImageOps.fit(card, (target_w, target_h), method=Image.LANCZOS, centering=(0.5, 0.5))


def render_spread(
    table_path: str,
    card_paths: List[str],
    out_path: str,
    *,
    seed: Optional[int] = None,
    max_side: int = 1280,
    quality: int = 85,
    micro_rotate: bool = True,
) -> str:
    """
    Делает одну картинку: table + cards overlay.
    Сохраняет JPEG (baseline, non-progressive) — дружит с Telegram.

    - seed: чтобы расклад был стабильным (например user_id)
    - micro_rotate: лёгкая “живая” подача (±2 градуса). Можно выключить.
    """
    if seed is not None:
        random.seed(seed)

    if not os.path.isfile(table_path):
        raise FileNotFoundError(f"table not found: {table_path}")

    n = max(1, min(len(card_paths), 7))
    layout = _LAYOUTS.get(n, _LAYOUTS[3])

    base = Image.open(table_path).convert("RGB")
    W, H = base.size
    canvas = base.convert("RGBA")

    # Параметры премиума (масштабируются от размера стола)
    # На 1024: border ~8, radius ~18, blur ~12
    border = max(6, int(W * 0.008))
    corner_radius = max(14, int(W * 0.018))
    shadow_blur = max(10, int(W * 0.012))
    shadow_opacity = 140  # мягко
    shadow_offset = (int(W * 0.010), int(W * 0.012))  # вниз-вправо

    # Рамка: почти белая, но не “стерильная”
    border_color = (245, 245, 245)

    for idx in range(n):
        slot, cx, cy, rot = layout[idx]
        cp = card_paths[idx]

        if not os.path.isfile(cp):
            continue

        card = Image.open(cp)
        card.load()
        card = card.convert("RGB")

        # целевой размер карты
        target_w = int(W * _SIZE_MAP[slot])
        target_h = int(target_w * _CARD_ASPECT)

        # на всякий — не вылезаем за стол
        target_w = min(target_w, int(W * 0.80))
        target_h = min(target_h, int(H * 0.90))

        card_fit = _fit_card(card, target_w, target_h)

        # Премиум слой: закругления + рамка + тень
        layer = _premium_card_layer(
            card_fit,
            corner_radius=corner_radius,
            border=border,
            border_color=border_color,
            shadow_blur=shadow_blur,
            shadow_offset=shadow_offset,
            shadow_opacity=shadow_opacity,
        )

        # микроповорот (чтобы было “живее”), но без хаоса
        if micro_rotate:
            if rot == 0:
                rot = random.choice([-2, -1, 0, 1, 2])
            layer = layer.rotate(rot, resample=Image.BICUBIC, expand=True)

        # позиция
        x = int(cx * W - layer.size[0] / 2)
        y = int(cy * H - layer.size[1] / 2)

        canvas.alpha_composite(layer, (x, y))

    out = canvas.convert("RGB")

    # ограничим размер (Telegram-friendly)
    if max(out.size) > max_side:
        out.thumbnail((max_side, max_side), Image.LANCZOS)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    # baseline JPEG (без progressive) — меньше проблем с Telegram
    out.save(
        out_path,
        "JPEG",
        quality=int(max(40, min(95, quality))),
        optimize=True,
        progressive=False,
        subsampling=2,
    )
    return out_path