"""
reporter.py — отчёт по ВУЗам.

Формат вывода:
  - Markdown-файл (reports/YYYY-MM-DD_uni_report.md) — отправляется в Telegram как документ
  - PNG-графики — отправляются как фото
  - Текстовый дайджест в сообщениях

Активные пользователи = ym:s:users (уникальные пользователи за период,
т.е. те кто посетил сайт хотя бы раз за месяц).
"""

import io
import logging
import os
import time
import traceback
from datetime import date, datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import requests as req_lib

from metrika import MetrikaClient, UNI_REGISTRY, _prettify_prefix

log = logging.getLogger("reporter")

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
    y, m = ym.split("-")
    return f"{MONTH_RU.get(m, m)} {y}"


# ── Сбор данных ───────────────────────────────────────────────────────────────

def collect_uni_stats(metrika: MetrikaClient, uni: dict, months: int = 3) -> dict:
    hosts = uni.get("hosts") or [uni["host"]]
    slug  = uni["slug"]
    name  = uni["name"]

    log.info("📊 Сбор данных: %s | hosts=%s", name, hosts)

    cumulative = metrika.get_cumulative_users(filter_hosts=hosts, since="2024-01-01")

    monthly = metrika.get_users_by_month(months=months + 1, filter_hosts=hosts)
    this_month_users = monthly[-1]["users"] if monthly else 0
    prev_month_users = monthly[-2]["users"] if len(monthly) >= 2 else 0
    growth           = this_month_users - prev_month_users
    growth_pct       = round(growth / prev_month_users * 100, 1) if prev_month_users else None

    product_monthly = metrika.get_users_by_product_monthly(
        filter_hosts=hosts, months=months
    )

    return {
        "slug":             slug,
        "name":             name,
        "hosts":            hosts,
        "cumulative_users": cumulative,
        "this_month_users": this_month_users,
        "prev_month_users": prev_month_users,
        "growth":           growth,
        "growth_pct":       growth_pct,
        "active_users":     this_month_users,
        "monthly":          monthly,
        "product_monthly":  product_monthly,
    }


# ── Графики ───────────────────────────────────────────────────────────────────

def _style(ax, title: str):
    ax.set_title(title, fontsize=12, fontweight="bold", pad=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{int(x):,}".replace(",", "\u202f"))
    )
    ax.grid(axis="y", alpha=0.3, linestyle="--")


def chart_all_unis_growth(uni_stats: list[dict], months: int = 3) -> bytes:
    """Grouped bar: прирост уников по всем ВУЗам помесячно."""
    all_months: list[str] = []
    for s in uni_stats:
        for m in s["monthly"][-months:]:
            if m["month"] not in all_months:
                all_months.append(m["month"])
    all_months = sorted(all_months)[-months:]

    active = [s for s in uni_stats if any(m["users"] > 0 for m in s["monthly"])]
    if not active:
        active = uni_stats

    n_unis   = len(active)
    n_months = len(all_months)
    x        = np.arange(n_months)
    bar_w    = min(0.8 / max(n_unis, 1), 0.15)
    colors   = plt.cm.tab20.colors

    fig, ax = plt.subplots(figsize=(max(10, n_months * 2.5), 6))

    for i, s in enumerate(active):
        month_map = {m["month"]: m["users"] for m in s["monthly"]}
        values    = [month_map.get(m, 0) for m in all_months]
        offset    = (i - n_unis / 2 + 0.5) * bar_w
        bars      = ax.bar(x + offset, values, bar_w,
                           label=s["name"], color=colors[i % len(colors)], alpha=0.85)
        for bar, v in zip(bars, values):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + 0.5,
                        str(v), ha="center", va="bottom", fontsize=7)

    ax.set_xticks(x)
    ax.set_xticklabels([month_label(m) for m in all_months])
    _style(ax, "Уникальные пользователи по ВУЗам (помесячно)")
    ax.legend(loc="upper left", fontsize=8, ncol=2)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def chart_uni_products(s: dict, months: int = 3) -> bytes | None:
    """Stacked bar: продукты ВУЗа помесячно."""
    pm = s.get("product_monthly", {})
    if not pm:
        return None

    all_months_set: set[str] = set()
    for recs in pm.values():
        for r in recs:
            all_months_set.add(r["month"])
    all_months = sorted(all_months_set)[-months:]
    if not all_months:
        return None

    totals      = {p: sum(r["users"] for r in recs) for p, recs in pm.items()}
    top_products = sorted(totals, key=lambda p: -totals[p])[:8]

    x     = np.arange(len(all_months))
    bar_w = 0.5
    fig, ax = plt.subplots(figsize=(max(8, len(all_months) * 2.5), 5))
    bottom = np.zeros(len(all_months))

    for i, prefix in enumerate(top_products):
        month_map = {r["month"]: r["users"] for r in pm.get(prefix, [])}
        values    = np.array([month_map.get(m, 0) for m in all_months], dtype=float)
        label     = _prettify_prefix(prefix, "")
        ax.bar(x, values, bar_w, bottom=bottom,
               label=label, color=PRODUCT_COLORS[i % len(PRODUCT_COLORS)], alpha=0.85)
        bottom += values

    ax.set_xticks(x)
    ax.set_xticklabels([month_label(m) for m in all_months])
    _style(ax, f"{s['name']} — пользователи по продуктам")
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ── Markdown-отчёт ────────────────────────────────────────────────────────────

def render_markdown(uni_stats: list[dict], months: int = 3) -> str:
    today     = datetime.now().strftime("%d.%m.%Y")
    sorted_s  = sorted(uni_stats, key=lambda s: -s["cumulative_users"])
    total_cum = sum(s["cumulative_users"] for s in uni_stats)
    total_act = sum(s["active_users"] for s in uni_stats)

    # Текущий месяц для заголовка
    cur_month = month_label(datetime.now().strftime("%Y-%m"))

    lines = [
        f"# 📊 Отчёт по ВУЗам — {today}",
        "",
        f"**Активных пользователей** = уникальные пользователи Яндекс Метрики за месяц (хотя бы 1 визит).",
        "",
        "---",
        "",
        "## Сводка",
        "",
        f"| Показатель | Значение |",
        f"|---|---|",
        f"| Подключено ВУЗов | {len(uni_stats)} |",
        f"| Уников накопленно (с 2024-01-01) | {total_cum:,} |".replace(",", "\u202f"),
        f"| Активных за {cur_month} | {total_act:,} |".replace(",", "\u202f"),
        "",
        "---",
        "",
        "## По ВУЗам",
        "",
        "| ВУЗ | Накопленно | Активных за месяц | Прирост к пред. месяцу |",
        "|---|---|---|---|",
    ]

    for s in sorted_s:
        name    = s["name"]
        cum     = f"{s['cumulative_users']:,}".replace(",", "\u202f")
        active  = f"{s['active_users']:,}".replace(",", "\u202f")
        g       = s["growth"]
        gp      = s["growth_pct"]
        if gp is not None:
            sign  = "+" if g >= 0 else ""
            trend = f"{sign}{g} ({sign}{gp}%)"
        else:
            trend = "—"
        lines.append(f"| {name} | {cum} | {active} | {trend} |")

    lines += ["", "---", "", "## Детали по каждому ВУЗу", ""]

    for s in sorted_s:
        name    = s["name"]
        monthly = s["monthly"]
        pm      = s.get("product_monthly", {})

        lines.append(f"### 🎓 {name}")
        lines.append("")

        # Таблица по месяцам
        if monthly:
            lines.append("**Динамика уникальных пользователей:**")
            lines.append("")
            lines.append("| Месяц | Уников |")
            lines.append("|---|---|")
            for m in monthly[-months:]:
                lines.append(f"| {month_label(m['month'])} | {m['users']:,} |".replace(",", "\u202f"))
            lines.append("")

        # Топ продуктов за последний месяц
        if pm:
            all_ms = sorted({r["month"] for recs in pm.values() for r in recs})
            if all_ms:
                last_m       = all_ms[-1]
                product_last = {p: next((r["users"] for r in recs if r["month"] == last_m), 0)
                                for p, recs in pm.items()}
                top          = sorted(product_last.items(), key=lambda x: -x[1])[:6]
                if any(v > 0 for _, v in top):
                    lines.append(f"**Топ продуктов ({month_label(last_m)}):**")
                    lines.append("")
                    lines.append("| Продукт | Пользователей |")
                    lines.append("|---|---|")
                    for prefix, users in top:
                        if users > 0:
                            pname = _prettify_prefix(prefix, "")
                            lines.append(f"| {pname} | {users:,} |".replace(",", "\u202f"))
                    lines.append("")

        # Наблюдения
        obs = []
        gp  = s.get("growth_pct")
        if gp is not None:
            if gp > 20:
                obs.append(f"✅ Сильный рост +{gp}% — продукт набирает аудиторию")
            elif gp > 0:
                obs.append(f"📈 Умеренный рост +{gp}%")
            elif gp < -20:
                obs.append(f"🔴 Значительный отток {gp}% — требует внимания")
            elif gp < 0:
                obs.append(f"📉 Небольшое снижение {gp}%")
            else:
                obs.append("➡️ Аудитория стабильна")

        cum = s["cumulative_users"]
        act = s["active_users"]
        if cum > 0:
            eng = round(act / cum * 100)
            obs.append(f"👥 Вовлечённость: {eng}% от накопленной базы вернулись в этом месяце")
            if eng < 20:
                obs.append("⚠️ Низкая вовлечённость — стоит проверить онбординг")
            elif eng > 60:
                obs.append("✅ Высокая вовлечённость")

        if obs:
            lines.append("**Наблюдения:**")
            lines.append("")
            for o in obs:
                lines.append(f"- {o}")
            lines.append("")

        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def save_markdown(uni_stats: list[dict], months: int = 3) -> Path:
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    ts   = datetime.now().strftime("%Y-%m-%d_%H-%M")
    path = reports_dir / f"{ts}_uni_report.md"
    path.write_text(render_markdown(uni_stats, months), encoding="utf-8")
    log.info("📄 Markdown сохранён: %s", path)
    return path


def save_report_files(uni_stats: list[dict], months: int = 3) -> list[Path]:
    saved: list[Path] = []
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")

    md_path = save_markdown(uni_stats, months)
    saved.append(md_path)

    try:
        png = chart_all_unis_growth(uni_stats, months)
        p   = reports_dir / f"{ts}_unis_growth.png"
        p.write_bytes(png)
        saved.append(p)
    except Exception:
        log.error("Ошибка общего графика:\n%s", traceback.format_exc())

    for s in uni_stats:
        try:
            png = chart_uni_products(s, months)
            if png:
                p = reports_dir / f"{ts}_{s['slug']}_products.png"
                p.write_bytes(png)
                saved.append(p)
        except Exception:
            log.warning("График %s:\n%s", s["slug"], traceback.format_exc())

    return saved


# ── Telegram ──────────────────────────────────────────────────────────────────

def _tg_post(base_url: str, method: str, **kwargs) -> dict:
    try:
        r = req_lib.post(f"{base_url}/{method}", timeout=30, **kwargs)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error("Telegram %s error: %s", method, e)
        return {}


def send_report_to_telegram(
    token: str,
    chat_id: str | int,
    uni_stats: list[dict],
    months: int = 3,
):
    base = f"https://api.telegram.org/bot{token}"

    def send_text(text: str):
        for chunk in _split(text, 4000):
            _tg_post(base, "sendMessage",
                     json={"chat_id": chat_id, "text": chunk, "parse_mode": "Markdown"})
            time.sleep(0.3)

    def send_photo(png: bytes, caption: str = ""):
        log.info("📤 Отправка графика (%d байт)...", len(png))
        result = _tg_post(base, "sendPhoto",
                          data={"chat_id": chat_id, "caption": caption},
                          files={"photo": ("chart.png", png, "image/png")})
        if result.get("ok"):
            log.info("✅ График отправлен")
        else:
            log.error("❌ Ошибка отправки графика: %s", result)
        time.sleep(0.5)

    def send_document(path: Path, caption: str = ""):
        log.info("📤 Отправка документа %s...", path.name)
        result = _tg_post(base, "sendDocument",
                          data={"chat_id": chat_id, "caption": caption},
                          files={"document": (path.name, path.read_bytes(),
                                              "text/markdown")})
        if result.get("ok"):
            log.info("✅ Документ отправлен")
        else:
            log.error("❌ Ошибка отправки документа: %s", result)

    # 1. Короткий текстовый дайджест
    send_text(_format_short_digest(uni_stats))

    # 2. Общий график
    log.info("🖼 Генерирую общий график...")
    try:
        png = chart_all_unis_growth(uni_stats, months)
        send_photo(png, "📊 Динамика уников по ВУЗам за 3 месяца")
    except Exception:
        log.error("Ошибка общего графика:\n%s", traceback.format_exc())

    # 3. Markdown-файл как документ
    md_path = save_markdown(uni_stats, months)
    send_document(md_path, "📄 Полный отчёт по ВУЗам")

    # 4. Графики по каждому ВУЗу (только с данными)
    sorted_stats = sorted(uni_stats, key=lambda s: -s["cumulative_users"])
    for s in sorted_stats:
        if s["cumulative_users"] == 0 and s["active_users"] == 0:
            continue
        try:
            png = chart_uni_products(s, months)
            if png:
                send_photo(png, f"📦 {s['name']} — продукты по месяцам")
        except Exception:
            log.warning("График %s:\n%s", s["slug"], traceback.format_exc())

    log.info("✅ Отчёт отправлен в chat_id=%s", chat_id)


def _format_short_digest(uni_stats: list[dict]) -> str:
    """Короткий текст-дайджест для сообщения в Telegram."""
    sorted_s  = sorted(uni_stats, key=lambda s: -s["cumulative_users"])
    total_cum = sum(s["cumulative_users"] for s in uni_stats)
    total_act = sum(s["active_users"] for s in uni_stats)
    cur_month = month_label(datetime.now().strftime("%Y-%m"))

    lines = [
        f"📊 *Отчёт по ВУЗам — {datetime.now().strftime('%d.%m.%Y')}*",
        "",
        f"🏛 ВУЗов подключено: *{len(uni_stats)}*",
        f"👥 Уников накопленно: *{total_cum:,}*".replace(",", "\u202f"),
        f"🟢 Активных за {cur_month}: *{total_act:,}*".replace(",", "\u202f"),
        "",
        "─" * 28,
    ]

    for s in sorted_s:
        cum  = s["cumulative_users"]
        act  = s["active_users"]
        g    = s["growth"]
        gp   = s["growth_pct"]
        if gp is not None:
            sign  = "+" if g >= 0 else ""
            trend = f"{sign}{g} ({sign}{gp}%)"
            emoji = "📈" if g > 0 else ("📉" if g < 0 else "➡️")
        else:
            trend, emoji = "—", "➖"

        lines += [
            f"\n🎓 *{s['name']}*",
            f"  Накопленно: `{cum:,}`".replace(",", "\u202f"),
            f"  Активных:   `{act:,}`".replace(",", "\u202f"),
            f"  Прирост:    {emoji} {trend}",
        ]

    return "\n".join(lines)


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
