"""
reporter.py — отчёт по ВУЗам.

Логика периодов:
  - "Последний полный месяц" = прошлый завершённый месяц (если сейчас апрель → март)
  - "Предыдущий месяц"       = позапрошлый (февраль)
  - Прирост = сравнение двух завершённых месяцев, не текущего незавершённого

Формат:
  Таблица Markdown с колонками:
  №, ВУЗ, Новые польз. за период, Активные за период, Рекомендации

  Все ВУЗы из UNI_REGISTRY показываются даже без данных.
"""

import io
import logging
import time
import traceback
from calendar import monthrange
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
    "01": "янв", "02": "фев", "03": "мар", "04": "апр",
    "05": "май", "06": "июн", "07": "июл", "08": "авг",
    "09": "сен", "10": "окт", "11": "ноя", "12": "дек",
}


def month_label(ym: str) -> str:
    """'2026-03' → 'мар 2026'"""
    y, m = ym.split("-")
    return f"{MONTH_RU.get(m, m)} {y}"


def last_full_months(n: int = 2) -> list[tuple[str, str, str]]:
    """
    Возвращает последние N завершённых месяцев.
    Если сейчас апрель 2026 → [(2026-02, ...), (2026-03, ...)]
    Формат каждого: (ym_label, date_from, date_to)
    """
    today = date.today()
    # Прошлый месяц (последний полный)
    result = []
    for offset in range(n, 0, -1):
        month = today.month - offset
        year  = today.year
        while month <= 0:
            month += 12
            year  -= 1
        last_day = monthrange(year, month)[1]
        ym    = f"{year:04d}-{month:02d}"
        d1    = f"{year:04d}-{month:02d}-01"
        d2    = f"{year:04d}-{month:02d}-{last_day:02d}"
        result.append((ym, d1, d2))
    return result


# ── Сбор данных ───────────────────────────────────────────────────────────────

def collect_uni_stats(metrika: MetrikaClient, uni: dict) -> dict:
    """
    Собирает метрики для одного ВУЗа за два последних ПОЛНЫХ месяца.
    Возвращает данные для таблицы.
    """
    hosts = uni.get("hosts") or ([uni["host"]] if uni.get("host") else [])
    slug  = uni["slug"]
    name  = uni["name"]

    log.info("📊 %s | hosts=%s", name, hosts)

    # Два последних полных месяца
    periods = last_full_months(2)
    last_ym, last_d1, last_d2       = periods[-1]  # март
    prev_ym, prev_d1, prev_d2       = periods[-2]  # февраль

    def fetch_users(d1: str, d2: str) -> int:
        flt = MetrikaClient._make_filter(None, None, hosts)
        params = {
            "ids": metrika.counter_id,
            "metrics": "ym:s:users",
            "date1": d1,
            "date2": d2,
            "accuracy": "full",
        }
        if flt:
            params["filters"] = flt
        try:
            data = metrika._get("/stat/v1/data", params)
            return round(data.get("totals", [0])[0])
        except Exception as e:
            log.warning("fetch_users %s-%s: %s", d1, d2, e)
            return 0

    last_users = fetch_users(last_d1, last_d2)
    prev_users = fetch_users(prev_d1, prev_d2)

    # Нарастающий итог с начала подключения
    cumulative = metrika.get_cumulative_users(filter_hosts=hosts, since="2024-01-01")

    # Прирост (два завершённых месяца)
    growth     = last_users - prev_users
    growth_pct = round(growth / prev_users * 100, 1) if prev_users else None

    # Помесячная динамика для графиков (3 месяца)
    monthly = metrika.get_users_by_month(months=4, filter_hosts=hosts)
    # Берём только завершённые месяцы (убираем текущий)
    today_ym = date.today().strftime("%Y-%m")
    monthly  = [m for m in monthly if m["month"] != today_ym]

    # Продукты за последний полный месяц
    product_monthly = metrika.get_users_by_product_monthly(filter_hosts=hosts, months=3)

    # Топ продуктов за последний полный месяц
    top_products = {}
    if product_monthly:
        for prefix, records in product_monthly.items():
            v = next((r["users"] for r in records if r["month"] == last_ym), 0)
            if v > 0:
                top_products[_prettify_prefix(prefix, "")] = v
        top_products = dict(sorted(top_products.items(), key=lambda x: -x[1])[:5])

    return {
        "slug":          slug,
        "name":          name,
        "hosts":         hosts,
        "cumulative":    cumulative,
        "last_ym":       last_ym,
        "last_users":    last_users,    # активные за последний полный месяц
        "prev_ym":       prev_ym,
        "prev_users":    prev_users,    # активные за предыдущий полный месяц
        "growth":        growth,
        "growth_pct":    growth_pct,
        "monthly":       monthly,
        "top_products":  top_products,
    }


def make_recommendation(s: dict) -> str:
    """Генерирует текстовую рекомендацию на основе данных."""
    parts = []
    gp   = s.get("growth_pct")
    g    = s.get("growth", 0)
    last = s.get("last_users", 0)
    cum  = s.get("cumulative", 0)

    if last == 0 and cum == 0:
        return "Нет данных — ВУЗ не подключён или не активен"

    if last == 0:
        return "Активности в последнем месяце нет — проверить доступность платформы для студентов"

    if gp is not None:
        if gp >= 50:
            parts.append(f"🚀 Сильный рост +{gp}% — зафиксировать что сработало и масштабировать")
        elif gp >= 10:
            parts.append(f"📈 Умеренный рост +{gp}% — положительная динамика")
        elif gp >= -10:
            parts.append("➡️ Стабильная аудитория")
        elif gp >= -30:
            parts.append(f"📉 Снижение {gp}% — выяснить причину оттока")
        else:
            parts.append(f"⚠️ Значительный спад {gp}% — требует внимания")

    if cum > 0:
        eng = round(last / cum * 100)
        if eng < 15:
            parts.append(f"Вовлечённость {eng}% — низкая, улучшить онбординг")
        elif eng > 60:
            parts.append(f"Вовлечённость {eng}% — высокая ✓")
        else:
            parts.append(f"Вовлечённость {eng}%")

    top = s.get("top_products", {})
    if top:
        top_name = next(iter(top))
        parts.append(f"Топ продукт: {top_name}")

    return "; ".join(parts) if parts else "—"


# ── Markdown-таблица ──────────────────────────────────────────────────────────

def render_markdown_table(uni_stats: list[dict]) -> str:
    """
    Формирует Markdown-отчёт в виде таблицы.
    Показывает все ВУЗы из UNI_REGISTRY, даже без данных.
    """
    if not uni_stats:
        return "Нет данных"

    # Периоды из первого ВУЗа с данными
    sample     = next((s for s in uni_stats if s.get("last_ym")), uni_stats[0])
    last_ym    = sample.get("last_ym", "")
    prev_ym    = sample.get("prev_ym", "")
    last_label = month_label(last_ym) if last_ym else "—"
    prev_label = month_label(prev_ym) if prev_ym else "—"
    today_str  = datetime.now().strftime("%d.%m.%Y")

    # Строим словарь slug → stats
    stats_map = {s["slug"]: s for s in uni_stats}

    # Полный список ВУЗов из реестра (в порядке убывания активных)
    all_slugs = list(UNI_REGISTRY.keys())
    # Убираем дубли (fta → fa уже в реестре)
    seen_names: set[str] = set()
    unique_slugs: list[str] = []
    for slug in all_slugs:
        name = UNI_REGISTRY[slug]
        if name not in seen_names:
            seen_names.add(name)
            unique_slugs.append(slug)

    # Сортируем: у кого есть данные — по убыванию активных, остальные в конце
    def sort_key(slug):
        s = stats_map.get(slug)
        return -(s["last_users"] if s else 0)
    unique_slugs.sort(key=sort_key)

    # Итоги
    total_last = sum(s["last_users"] for s in uni_stats)
    total_cum  = sum(s["cumulative"] for s in uni_stats)

    lines = [
        f"# 📊 Отчёт по ВУЗам — {today_str}",
        "",
        f"**Период отчёта:** {prev_label} → {last_label}",
        f"**Активные пользователи** = уникальные посетители за месяц (Яндекс Метрика)",
        "",
        "## Сводка",
        "",
        f"| | Значение |",
        f"|---|---|",
        f"| ВУЗов в реестре | {len(unique_slugs)} |",
        f"| Уников накопленно (с 2024-01-01) | **{total_cum}** |",
        f"| Активных за {last_label} | **{total_last}** |",
        "",
        "---",
        "",
        "## Таблица по ВУЗам",
        "",
        f"Период: **{last_label}** (новые и активные пользователи)",
        f"Прирост: сравнение с **{prev_label}**",
        "",
        f"| № | ВУЗ | Накопленно | Активных за {last_label} | Прирост vs {prev_label} | Рекомендации |",
        f"|---|---|---|---|---|---|",
    ]

    for i, slug in enumerate(unique_slugs, 1):
        s    = stats_map.get(slug)
        name = UNI_REGISTRY[slug]

        if s:
            cum   = s["cumulative"]
            last  = s["last_users"]
            g     = s["growth"]
            gp    = s["growth_pct"]
            rec   = make_recommendation(s)

            if gp is not None:
                sign   = "+" if g >= 0 else ""
                trend  = f"{sign}{g} ({sign}{gp}%)"
                emoji  = "📈" if g > 0 else ("📉" if g < 0 else "➡️")
                growth_cell = f"{emoji} {trend}"
            else:
                growth_cell = "—"
        else:
            cum         = "—"
            last        = "—"
            growth_cell = "—"
            rec         = "Нет данных в Метрике"

        lines.append(f"| {i} | {name} | {cum} | {last} | {growth_cell} | {rec} |")

    lines += [
        "",
        "---",
        "",
        "## Динамика по месяцам",
        "",
    ]

    # Детальная динамика по каждому ВУЗу с данными
    for slug in unique_slugs:
        s = stats_map.get(slug)
        if not s or not s.get("monthly"):
            continue
        if all(m["users"] == 0 for m in s["monthly"]):
            continue

        name = s["name"]
        lines += [f"### 🎓 {name}", ""]
        lines += ["| Месяц | Активных пользователей |", "|---|---|"]
        for m in s["monthly"][-3:]:
            lines.append(f"| {month_label(m['month'])} | {m['users']} |")

        top = s.get("top_products", {})
        if top:
            lines += ["", f"**Топ продуктов за {last_label}:**", ""]
            lines += ["| Продукт | Пользователей |", "|---|---|"]
            for pname, pcount in top.items():
                lines.append(f"| {pname} | {pcount} |")
        lines.append("")

    return "\n".join(lines)


def save_markdown(uni_stats: list[dict]) -> Path:
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    ts   = datetime.now().strftime("%Y-%m-%d_%H-%M")
    path = reports_dir / f"{ts}_uni_report.md"
    path.write_text(render_markdown_table(uni_stats), encoding="utf-8")
    log.info("📄 Markdown: %s", path)
    return path


def save_report_files(uni_stats: list[dict]) -> list[Path]:
    saved = []
    try:
        md = save_markdown(uni_stats)
        saved.append(md)
    except Exception:
        log.error("Ошибка markdown:\n%s", traceback.format_exc())

    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")

    try:
        png = chart_all_unis_growth(uni_stats)
        p   = reports_dir / f"{ts}_unis_growth.png"
        p.write_bytes(png)
        saved.append(p)
    except Exception:
        log.error("Ошибка общего графика:\n%s", traceback.format_exc())

    for s in uni_stats:
        try:
            png = chart_uni_products(s)
            if png:
                p = reports_dir / f"{ts}_{s['slug']}_products.png"
                p.write_bytes(png)
                saved.append(p)
        except Exception:
            log.warning("График %s:\n%s", s["slug"], traceback.format_exc())

    return saved


# ── Графики ───────────────────────────────────────────────────────────────────

def _style(ax, title: str):
    ax.set_title(title, fontsize=12, fontweight="bold", pad=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{int(x):,}".replace(",", "\u202f"))
    )
    ax.grid(axis="y", alpha=0.3, linestyle="--")


def chart_all_unis_growth(uni_stats: list[dict]) -> bytes:
    """Grouped bar по последним 3 завершённым месяцам."""
    active = [s for s in uni_stats if s.get("monthly") and any(m["users"] > 0 for m in s["monthly"])]
    if not active:
        active = uni_stats

    # Собираем месяцы (только завершённые)
    today_ym = date.today().strftime("%Y-%m")
    all_months_set: set[str] = set()
    for s in active:
        for m in s.get("monthly", []):
            if m["month"] != today_ym:
                all_months_set.add(m["month"])
    all_months = sorted(all_months_set)[-3:]

    if not all_months:
        # Fallback — пустой график
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.text(0.5, 0.5, "Нет данных", ha="center", va="center", transform=ax.transAxes)
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=100)
        plt.close(fig)
        buf.seek(0)
        return buf.read()

    n_unis   = len(active)
    n_months = len(all_months)
    x        = np.arange(n_months)
    bar_w    = min(0.8 / max(n_unis, 1), 0.15)
    colors   = plt.cm.tab20.colors

    fig, ax = plt.subplots(figsize=(max(10, n_months * 3), 6))

    for i, s in enumerate(active):
        month_map = {m["month"]: m["users"] for m in s.get("monthly", [])}
        values    = [month_map.get(m, 0) for m in all_months]
        offset    = (i - n_unis / 2 + 0.5) * bar_w
        bars      = ax.bar(x + offset, values, bar_w,
                           label=s["name"], color=colors[i % len(colors)], alpha=0.85)
        for bar, v in zip(bars, values):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + 0.3,
                        str(v), ha="center", va="bottom", fontsize=7)

    labels = [f"{month_label(m)}" for m in all_months]
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    _style(ax, "Активные пользователи по ВУЗам (завершённые месяцы)")
    ax.legend(loc="upper left", fontsize=8, ncol=2)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def chart_uni_products(s: dict) -> bytes | None:
    """Stacked bar: продукты ВУЗа по завершённым месяцам."""
    pm = s.get("product_monthly", {})
    if not pm:
        return None

    today_ym = date.today().strftime("%Y-%m")
    all_ms   = sorted({r["month"] for recs in pm.values() for r in recs if r["month"] != today_ym})[-3:]
    if not all_ms:
        return None

    totals       = {p: sum(r["users"] for r in recs if r["month"] != today_ym) for p, recs in pm.items()}
    top_products = sorted(totals, key=lambda p: -totals[p])[:8]
    if not any(totals[p] > 0 for p in top_products):
        return None

    x     = np.arange(len(all_ms))
    bar_w = 0.5
    fig, ax = plt.subplots(figsize=(max(8, len(all_ms) * 3), 5))
    bottom  = np.zeros(len(all_ms))

    for i, prefix in enumerate(top_products):
        month_map = {r["month"]: r["users"] for r in pm.get(prefix, [])}
        values    = np.array([month_map.get(m, 0) for m in all_ms], dtype=float)
        if values.sum() == 0:
            continue
        label = _prettify_prefix(prefix, "")
        ax.bar(x, values, bar_w, bottom=bottom,
               label=label, color=PRODUCT_COLORS[i % len(PRODUCT_COLORS)], alpha=0.85)
        bottom += values

    ax.set_xticks(x)
    ax.set_xticklabels([month_label(m) for m in all_ms], fontsize=10)
    _style(ax, f"{s['name']} — пользователи по продуктам")
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ── Telegram ──────────────────────────────────────────────────────────────────

def _tg_post(base_url: str, method: str, **kwargs) -> dict:
    try:
        r = req_lib.post(f"{base_url}/{method}", timeout=30, **kwargs)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error("Telegram %s: %s", method, e)
        return {}


def send_report_to_telegram(token: str, chat_id, uni_stats: list[dict]):
    base = f"https://api.telegram.org/bot{token}"

    def send_text(text: str):
        for chunk in _split(text, 4000):
            _tg_post(base, "sendMessage",
                     json={"chat_id": chat_id, "text": chunk, "parse_mode": "Markdown"})
            time.sleep(0.3)

    def send_photo(png: bytes, caption: str = ""):
        log.info("📤 Отправка графика %d байт...", len(png))
        r = _tg_post(base, "sendPhoto",
                     data={"chat_id": chat_id, "caption": caption},
                     files={"photo": ("chart.png", png, "image/png")})
        if r.get("ok"):
            log.info("✅ График отправлен")
        else:
            log.error("❌ sendPhoto: %s", r)
        time.sleep(0.5)

    def send_document(path: Path, caption: str = ""):
        log.info("📤 Отправка документа %s...", path.name)
        r = _tg_post(base, "sendDocument",
                     data={"chat_id": chat_id, "caption": caption},
                     files={"document": (path.name, path.read_bytes(), "text/markdown")})
        if r.get("ok"):
            log.info("✅ Документ отправлен")
        else:
            log.error("❌ sendDocument: %s", r)

    # 1. Короткий дайджест текстом
    sample = next((s for s in uni_stats if s.get("last_ym")), None)
    if sample:
        last_lbl = month_label(sample["last_ym"])
        prev_lbl = month_label(sample["prev_ym"])
    else:
        last_lbl = prev_lbl = "—"

    total_last = sum(s["last_users"] for s in uni_stats)
    total_cum  = sum(s["cumulative"] for s in uni_stats)

    digest = [
        f"📊 *Отчёт по ВУЗам — {datetime.now().strftime('%d.%m.%Y')}*",
        f"Период: *{prev_lbl}* → *{last_lbl}*",
        "",
        f"🏛 ВУЗов: *{len(set(UNI_REGISTRY.values()))}* в реестре",
        f"👥 Уников накопленно: *{total_cum}*",
        f"🟢 Активных за {last_lbl}: *{total_last}*",
        "",
        "Подробный отчёт с таблицами — в прикреплённом файле 👇",
    ]
    send_text("\n".join(digest))

    # 2. Общий график
    try:
        png = chart_all_unis_growth(uni_stats)
        send_photo(png, f"📊 Активные пользователи по ВУЗам")
    except Exception:
        log.error("Общий график:\n%s", traceback.format_exc())

    # 3. Markdown-файл
    md_path = save_markdown(uni_stats)
    send_document(md_path, "📄 Полный отчёт по ВУЗам (таблицы + рекомендации)")

    # 4. Графики по ВУЗам
    sorted_stats = sorted(uni_stats, key=lambda s: -s.get("last_users", 0))
    for s in sorted_stats:
        if s.get("last_users", 0) == 0:
            continue
        try:
            png = chart_uni_products(s)
            if png:
                send_photo(png, f"📦 {s['name']}")
        except Exception:
            log.warning("График %s:\n%s", s["slug"], traceback.format_exc())

    log.info("✅ Отчёт отправлен")


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
