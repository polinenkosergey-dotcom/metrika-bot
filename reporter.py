"""
reporter.py — формирует полный отчёт по ВУЗам.

Что делает:
  1. Для каждого ВУЗа собирает из Метрики:
     - уникальных пользователей нарастающим итогом (с начала подключения)
     - уникальных пользователей за текущий месяц
     - прирост к прошлому месяцу
     - помесячную динамику за 3 месяца по всем продуктам
  2. Строит PNG-графики (matplotlib):
     - общий: прирост уников по всем ВУЗам помесячно (grouped bar)
     - для каждого ВУЗа: динамика по продуктам помесячно (stacked bar)
  3. Формирует текстовый дайджест и отправляет в Telegram:
     - Сводная таблица по ВУЗам
     - Графики как фото
     - Детальный текст по каждому ВУЗу с трендами и рекомендациями от AI
"""

import io
import logging
import os
import time
from datetime import date, datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # без дисплея
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import requests as req_lib

from metrika import MetrikaClient, UNI_REGISTRY, _prettify_prefix

log = logging.getLogger("reporter")

# Цвета для продуктов на графике
PRODUCT_COLORS = [
    "#5B8DD9", "#E06B5A", "#5BAD84", "#E0A95A",
    "#9B7DC8", "#5BBDBD", "#D47AB3", "#8B9E6B",
]

MONTH_RU = {
    "01": "Янв", "02": "Фев", "03": "Мар", "04": "Апр",
    "05": "Май", "06": "Июн", "07": "Июл", "08": "Авг",
    "09": "Сен", "10": "Окт", "11": "Ноя", "12": "Дек",
}


def month_label(ym: str) -> str:
    """'2026-03' → 'Мар 2026'"""
    y, m = ym.split("-")
    return f"{MONTH_RU.get(m, m)} {y}"


# ── Сбор данных ───────────────────────────────────────────────────────────────

def collect_uni_stats(metrika: MetrikaClient, uni: dict, months: int = 3) -> dict:
    """
    Собирает все метрики для одного ВУЗа.
    uni — элемент из discover_unis().
    """
    hosts = uni.get("hosts") or [uni["host"]]
    slug = uni["slug"]
    name = uni["name"]

    log.info("📊 Сбор данных: %s (%s)", name, hosts)

    # 1. Нарастающий итог уников (с 2024-01-01)
    cumulative = metrika.get_cumulative_users(filter_hosts=hosts, since="2024-01-01")

    # 2. Помесячная динамика уников за N месяцев
    monthly = metrika.get_users_by_month(months=months + 1, filter_hosts=hosts)
    # Текущий месяц и прошлый
    this_month_users = monthly[-1]["users"] if monthly else 0
    prev_month_users = monthly[-2]["users"] if len(monthly) >= 2 else 0
    growth = this_month_users - prev_month_users
    growth_pct = round(growth / prev_month_users * 100, 1) if prev_month_users else None

    # 3. Активные пользователи = уники за текущий месяц (те кто посетил хотя бы раз)
    active_users = this_month_users

    # 4. Динамика по продуктам помесячно
    product_monthly = metrika.get_users_by_product_monthly(
        filter_hosts=hosts, months=months
    )

    return {
        "slug": slug,
        "name": name,
        "hosts": hosts,
        "cumulative_users": cumulative,
        "this_month_users": this_month_users,
        "prev_month_users": prev_month_users,
        "growth": growth,
        "growth_pct": growth_pct,
        "active_users": active_users,
        "monthly": monthly,          # [{month, users}]
        "product_monthly": product_monthly,  # {prefix: [{month, users}]}
    }


# ── Графики ───────────────────────────────────────────────────────────────────

def _apply_style(ax, title: str):
    ax.set_title(title, fontsize=13, fontweight="bold", pad=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}".replace(",", " ")))
    ax.grid(axis="y", alpha=0.3, linestyle="--")


def chart_all_unis_growth(uni_stats: list[dict], months: int = 3) -> bytes:
    """
    Grouped bar chart: прирост уников по всем ВУЗам помесячно.
    Возвращает PNG bytes.
    """
    # Собираем все месяцы
    all_months: list[str] = []
    for s in uni_stats:
        for m in s["monthly"][-months:]:
            if m["month"] not in all_months:
                all_months.append(m["month"])
    all_months = sorted(all_months)[-months:]

    unis_to_show = [s for s in uni_stats if s["cumulative_users"] > 0]
    if not unis_to_show:
        unis_to_show = uni_stats

    n_unis = len(unis_to_show)
    n_months = len(all_months)
    x = np.arange(n_months)
    bar_w = min(0.8 / max(n_unis, 1), 0.15)

    fig, ax = plt.subplots(figsize=(max(10, n_months * 2), 6))

    colors = plt.cm.tab20.colors

    for i, s in enumerate(unis_to_show):
        month_map = {m["month"]: m["users"] for m in s["monthly"]}
        values = [month_map.get(m, 0) for m in all_months]
        offset = (i - n_unis / 2 + 0.5) * bar_w
        bars = ax.bar(x + offset, values, bar_w, label=s["name"], color=colors[i % len(colors)], alpha=0.85)
        # Подписи только если значение > 0
        for bar, v in zip(bars, values):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                        str(v), ha="center", va="bottom", fontsize=7)

    ax.set_xticks(x)
    ax.set_xticklabels([month_label(m) for m in all_months])
    _apply_style(ax, "Уникальные пользователи по ВУЗам (помесячно)")
    ax.legend(loc="upper left", fontsize=8, ncol=2)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def chart_uni_products(s: dict, months: int = 3) -> bytes | None:
    """
    Stacked bar chart: динамика по продуктам конкретного ВУЗа помесячно.
    """
    product_monthly = s.get("product_monthly", {})
    if not product_monthly:
        return None

    # Берём все месяцы
    all_months_set: set[str] = set()
    for records in product_monthly.values():
        for r in records:
            all_months_set.add(r["month"])
    all_months = sorted(all_months_set)[-months:]

    if not all_months:
        return None

    # Топ-8 продуктов по суммарным пользователям
    product_totals = {
        prefix: sum(r["users"] for r in records)
        for prefix, records in product_monthly.items()
    }
    top_products = sorted(product_totals, key=lambda p: -product_totals[p])[:8]

    x = np.arange(len(all_months))
    bar_w = 0.5

    fig, ax = plt.subplots(figsize=(max(8, len(all_months) * 2), 5))
    bottom = np.zeros(len(all_months))

    for i, prefix in enumerate(top_products):
        month_map = {r["month"]: r["users"] for r in product_monthly.get(prefix, [])}
        values = np.array([month_map.get(m, 0) for m in all_months], dtype=float)
        label = _prettify_prefix(prefix, "")
        color = PRODUCT_COLORS[i % len(PRODUCT_COLORS)]
        ax.bar(x, values, bar_w, bottom=bottom, label=label, color=color, alpha=0.85)
        bottom += values

    ax.set_xticks(x)
    ax.set_xticklabels([month_label(m) for m in all_months])
    _apply_style(ax, f"{s['name']} — пользователи по продуктам")
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ── Текстовый отчёт ───────────────────────────────────────────────────────────

def format_summary_table(uni_stats: list[dict]) -> str:
    """Сводная таблица по всем ВУЗам."""
    sorted_stats = sorted(uni_stats, key=lambda s: -s["cumulative_users"])

    lines = ["📊 *Сводный отчёт по ВУЗам*\n"]
    today = datetime.now().strftime("%d.%m.%Y")
    lines.append(f"_{today}_\n")

    total_cumulative = sum(s["cumulative_users"] for s in uni_stats)
    total_active = sum(s["active_users"] for s in uni_stats)
    lines.append(f"🏛 ВУЗов: *{len(uni_stats)}*")
    lines.append(f"👥 Всего уников (накопленно): *{total_cumulative:,}*".replace(",", " "))
    lines.append(f"🟢 Активных за месяц: *{total_active:,}*\n".replace(",", " "))
    lines.append("─" * 30)

    for s in sorted_stats:
        name = s["name"]
        cum = s["cumulative_users"]
        active = s["active_users"]
        growth = s["growth"]
        growth_pct = s["growth_pct"]

        if growth_pct is not None:
            if growth > 0:
                trend = f"📈 +{growth} (+{growth_pct}%)"
            elif growth < 0:
                trend = f"📉 {growth} ({growth_pct}%)"
            else:
                trend = "➡️ без изменений"
        else:
            trend = "—"

        lines.append(
            f"\n🎓 *{name}*\n"
            f"  Уников накопленно: `{cum:,}`\n".replace(",", " ") +
            f"  Активных за месяц: `{active:,}`\n".replace(",", " ") +
            f"  Прирост к пред. месяцу: {trend}"
        )

    return "\n".join(lines)


def format_uni_detail(s: dict) -> str:
    """Детальный текст по одному ВУЗу."""
    name = s["name"]
    monthly = s["monthly"]
    product_monthly = s.get("product_monthly", {})

    lines = [f"🎓 *{name}*\n"]

    # Тренд по месяцам
    if len(monthly) >= 2:
        lines.append("📅 *Динамика уников:*")
        for m in monthly[-3:]:
            bar = "█" * min(int(m["users"] / max(1, max(x["users"] for x in monthly)) * 10), 10)
            lines.append(f"  {month_label(m['month'])}: `{m['users']:,}` {bar}".replace(",", " "))
        lines.append("")

    # Топ продуктов за последний месяц
    if product_monthly:
        last_month = sorted({r["month"] for recs in product_monthly.values() for r in recs})[-1] if product_monthly else None
        if last_month:
            product_last = {
                prefix: next((r["users"] for r in recs if r["month"] == last_month), 0)
                for prefix, recs in product_monthly.items()
            }
            top = sorted(product_last.items(), key=lambda x: -x[1])[:5]
            if top:
                lines.append(f"🏆 *Топ продуктов ({month_label(last_month)}):*")
                for prefix, users in top:
                    pname = _prettify_prefix(prefix, "")
                    lines.append(f"  • {pname}: `{users:,}`".replace(",", " "))
                lines.append("")

    # Рекомендации на основе данных
    lines.append("💡 *Наблюдения:*")
    growth_pct = s.get("growth_pct")
    if growth_pct is not None:
        if growth_pct > 20:
            lines.append(f"  ✅ Сильный рост +{growth_pct}% — продукт набирает аудиторию")
        elif growth_pct > 0:
            lines.append(f"  📈 Умеренный рост +{growth_pct}%")
        elif growth_pct < -20:
            lines.append(f"  🔴 Значительный отток {growth_pct}% — требует внимания")
        elif growth_pct < 0:
            lines.append(f"  📉 Небольшое снижение {growth_pct}%")
        else:
            lines.append("  ➡️ Аудитория стабильна")

    active = s["active_users"]
    cum = s["cumulative_users"]
    if cum > 0:
        engagement = round(active / cum * 100)
        lines.append(f"  👥 Вовлечённость: {engagement}% от накопленной базы вернулись в этом месяце")
        if engagement < 20:
            lines.append("  ⚠️ Низкая вовлечённость — стоит проверить онбординг и уведомления")
        elif engagement > 60:
            lines.append("  ✅ Высокая вовлечённость — пользователи регулярно возвращаются")

    return "\n".join(lines)


# ── Telegram отправка ─────────────────────────────────────────────────────────

def send_report_to_telegram(
    token: str,
    chat_id: str | int,
    uni_stats: list[dict],
    months: int = 3,
):
    """
    Отправляет полный отчёт в Telegram:
    1. Сводный текст
    2. Общий график по всем ВУЗам
    3. По каждому ВУЗу: текст + график продуктов
    """
    base_url = f"https://api.telegram.org/bot{token}"

    def send_text(text: str, parse_mode: str = "Markdown"):
        for chunk in _split(text):
            req_lib.post(
                f"{base_url}/sendMessage",
                json={"chat_id": chat_id, "text": chunk, "parse_mode": parse_mode},
                timeout=15,
            )
            time.sleep(0.3)

    def send_photo(png_bytes: bytes, caption: str = ""):
        req_lib.post(
            f"{base_url}/sendPhoto",
            data={"chat_id": chat_id, "caption": caption, "parse_mode": "Markdown"},
            files={"photo": ("chart.png", png_bytes, "image/png")},
            timeout=30,
        )
        time.sleep(0.5)

    # 1. Сводная таблица
    send_text(format_summary_table(uni_stats))

    # 2. Общий график
    log.info("🖼 Генерирую общий график...")
    try:
        png = chart_all_unis_growth(uni_stats, months)
        send_photo(png, "📊 Динамика уникальных пользователей по ВУЗам")
    except Exception as e:
        log.error("Ошибка общего графика: %s", e)

    # 3. Детали по каждому ВУЗу
    sorted_stats = sorted(uni_stats, key=lambda s: -s["cumulative_users"])
    for s in sorted_stats:
        if s["cumulative_users"] == 0 and s["active_users"] == 0:
            continue  # пропускаем пустые

        # Текст
        send_text(format_uni_detail(s))

        # График по продуктам
        try:
            png = chart_uni_products(s, months)
            if png:
                send_photo(png, f"📦 {s['name']} — продукты по месяцам")
        except Exception as e:
            log.warning("График продуктов %s: %s", s["name"], e)

    log.info("✅ Отчёт отправлен")


def save_report_files(uni_stats: list[dict], months: int = 3) -> list[Path]:
    """Сохраняет PNG и текстовый отчёт в папку reports/."""
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    saved: list[Path] = []

    # Общий график
    try:
        png = chart_all_unis_growth(uni_stats, months)
        p = reports_dir / f"{ts}_unis_growth.png"
        p.write_bytes(png)
        saved.append(p)
    except Exception as e:
        log.error("Ошибка сохранения общего графика: %s", e)

    # Графики по ВУЗам
    for s in uni_stats:
        try:
            png = chart_uni_products(s, months)
            if png:
                safe = s["slug"].replace("/", "")
                p = reports_dir / f"{ts}_{safe}_products.png"
                p.write_bytes(png)
                saved.append(p)
        except Exception as e:
            log.warning("График %s: %s", s["slug"], e)

    # Текстовый отчёт
    text = format_summary_table(uni_stats) + "\n\n"
    text += "\n\n".join(format_uni_detail(s) for s in uni_stats)
    p = reports_dir / f"{ts}_uni_report.md"
    p.write_text(text.replace("*", "").replace("`", ""), encoding="utf-8")
    saved.append(p)

    return saved


def _split(text: str, limit: int = 4000) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks, current, cur_len = [], [], 0
    for line in text.split("\n"):
        if cur_len + len(line) + 1 > limit:
            chunks.append("\n".join(current))
            current, cur_len = [], 0
        current.append(line)
        cur_len += len(line) + 1
    if current:
        chunks.append("\n".join(current))
    return chunks
