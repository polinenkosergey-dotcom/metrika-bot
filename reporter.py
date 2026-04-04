"""
reporter.py — отчёт по ВУЗам.

Определения:
  Новые пользователи   = ym:s:newUsers (первый визит за всю историю счётчика)
  Активные             = пользователи с 3+ визитами за месяц (visitNumber >= 3)
  Все посетители       = ym:s:users (уникальные за период, хотя бы 1 визит)
  Накопленно           = ym:s:users с 2024-01-01 по сегодня

Период: два последних ЗАВЕРШЁННЫХ месяца (не текущий).
Прирост: сравнение последнего полного месяца с предыдущим по метрике "Все посетители".
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

from metrika import MetrikaClient, UNI_REGISTRY, UNI_SLUG_MERGE, _prettify_prefix

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

# Активный = 3+ визитов за период
ACTIVE_MIN_VISITS = 3


def month_label(ym: str) -> str:
    y, m = ym.split("-")
    return f"{MONTH_RU.get(m, m)} {y}"


def last_full_months(n: int = 2) -> list[tuple[str, str, str]]:
    """Последние N завершённых месяцев: [(ym, date_from, date_to), ...]"""
    today = date.today()
    result = []
    for offset in range(n, 0, -1):
        month = today.month - offset
        year  = today.year
        while month <= 0:
            month += 12
            year  -= 1
        last_day = monthrange(year, month)[1]
        ym = f"{year:04d}-{month:02d}"
        result.append((ym, f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"))
    return result


# ── Сбор данных ───────────────────────────────────────────────────────────────

def collect_uni_stats(metrika: MetrikaClient, uni: dict) -> dict:
    hosts = uni.get("hosts") or ([uni["host"]] if uni.get("host") else [])
    slug  = uni["slug"]
    name  = uni["name"]

    if not hosts:
        return _empty_stats(slug, name)

    log.info("📊 %s | hosts=%s", name, hosts)

    periods  = last_full_months(2)
    last_ym, last_d1, last_d2 = periods[-1]
    prev_ym, prev_d1, prev_d2 = periods[-2]

    # Все посетители за каждый месяц
    last_all  = _fetch(metrika, "ym:s:users",    last_d1, last_d2, hosts)
    prev_all  = _fetch(metrika, "ym:s:users",    prev_d1, prev_d2, hosts)

    # Новые пользователи за последний полный месяц
    last_new  = _fetch(metrika, "ym:s:newUsers", last_d1, last_d2, hosts)

    # Активные (3+ визитов) за последний полный месяц
    last_active = metrika.get_active_users(last_d1, last_d2,
                                           filter_hosts=hosts,
                                           min_visits=ACTIVE_MIN_VISITS)

    # Нарастающий итог
    cumulative = metrika.get_cumulative_users(filter_hosts=hosts, since="2024-01-01")

    # Прирост посетителей
    growth     = last_all - prev_all
    growth_pct = round(growth / prev_all * 100, 1) if prev_all else None

    # Помесячная динамика для графиков
    monthly = _fetch_monthly(metrika, hosts, months=4)
    today_ym = date.today().strftime("%Y-%m")
    monthly  = [m for m in monthly if m["month"] != today_ym]

    # Продукты за последний месяц
    product_monthly = metrika.get_users_by_product_monthly(filter_hosts=hosts, months=3)
    top_products = _top_products(product_monthly, last_ym)

    return {
        "slug":           slug,
        "name":           name,
        "hosts":          hosts,
        "cumulative":     cumulative,
        "last_ym":        last_ym,
        "last_all":       last_all,      # все посетители
        "last_new":       last_new,      # новые (первый визит)
        "last_active":    last_active,   # активные (3+ визитов)
        "prev_ym":        prev_ym,
        "prev_all":       prev_all,
        "growth":         growth,
        "growth_pct":     growth_pct,
        "monthly":        monthly,
        "top_products":   top_products,
        "product_monthly": product_monthly,
    }


def _empty_stats(slug: str, name: str) -> dict:
    periods = last_full_months(2)
    return {
        "slug": slug, "name": name, "hosts": [],
        "cumulative": 0,
        "last_ym": periods[-1][0], "last_all": 0, "last_new": 0, "last_active": 0,
        "prev_ym": periods[-2][0], "prev_all": 0,
        "growth": 0, "growth_pct": None,
        "monthly": [], "top_products": {}, "product_monthly": {},
    }


def _fetch(metrika: MetrikaClient, metric: str, d1: str, d2: str,
           hosts: list[str]) -> int:
    flt = MetrikaClient._make_filter(None, None, hosts)
    params = {"ids": metrika.counter_id, "metrics": metric,
              "date1": d1, "date2": d2, "accuracy": "full"}
    if flt:
        params["filters"] = flt
    try:
        data = metrika._get("/stat/v1/data", params)
        return round(data.get("totals", [0])[0])
    except Exception as e:
        log.warning("_fetch %s: %s", metric, e)
        return 0


def _fetch_monthly(metrika: MetrikaClient, hosts: list[str], months: int) -> list[dict]:
    from calendar import monthrange as mr
    today  = date.today()
    result = []
    for i in range(months - 1, -1, -1):
        month = today.month - i
        year  = today.year
        while month <= 0:
            month += 12; year -= 1
        ym  = f"{year:04d}-{month:02d}"
        d1  = f"{year:04d}-{month:02d}-01"
        d2  = f"{year:04d}-{month:02d}-{mr(year, month)[1]:02d}"
        users = _fetch(metrika, "ym:s:users", d1, d2, hosts)
        result.append({"month": ym, "users": users})
    return result


def _top_products(product_monthly: dict, last_ym: str) -> dict:
    result = {}
    for prefix, records in product_monthly.items():
        v = next((r["users"] for r in records if r["month"] == last_ym), 0)
        if v > 0:
            result[_prettify_prefix(prefix, "")] = v
    return dict(sorted(result.items(), key=lambda x: -x[1])[:5])


def make_recommendation(s: dict) -> str:
    parts = []
    last   = s.get("last_all", 0)
    new_u  = s.get("last_new", 0)
    active = s.get("last_active", 0)
    cum    = s.get("cumulative", 0)
    gp     = s.get("growth_pct")

    if last == 0 and cum == 0:
        return "Нет данных — ВУЗ не подключён или не активен"
    if last == 0:
        return "Активности нет — проверить доступность платформы"

    if gp is not None:
        if gp >= 50:
            parts.append(f"🚀 Сильный рост +{gp}%")
        elif gp >= 10:
            parts.append(f"📈 Рост +{gp}%")
        elif gp >= -10:
            parts.append("➡️ Стабильно")
        elif gp >= -30:
            parts.append(f"📉 Снижение {gp}% — выяснить причину")
        else:
            parts.append(f"⚠️ Спад {gp}% — требует внимания")

    if last > 0:
        act_rate = round(active / last * 100)
        if act_rate < 20:
            parts.append(f"Активных {act_rate}% — низко, улучшить retention")
        elif act_rate > 50:
            parts.append(f"Активных {act_rate}% — высокая вовлечённость ✓")
        else:
            parts.append(f"Активных {act_rate}%")

    top = s.get("top_products", {})
    if top:
        parts.append(f"Топ: {next(iter(top))}")

    return "; ".join(parts) if parts else "—"


# ── Markdown-таблица ──────────────────────────────────────────────────────────

def render_markdown_table(uni_stats: list[dict]) -> str:
    sample    = next((s for s in uni_stats if s.get("last_ym")), None)
    last_ym   = sample["last_ym"] if sample else ""
    prev_ym   = sample["prev_ym"] if sample else ""
    last_lbl  = month_label(last_ym) if last_ym else "—"
    prev_lbl  = month_label(prev_ym) if prev_ym else "—"
    today_str = datetime.now().strftime("%d.%m.%Y")

    stats_map = {s["slug"]: s for s in uni_stats}

    # Уникальные ВУЗы из реестра без дублей
    seen: set[str] = set()
    unique_slugs: list[str] = []
    for slug, name in UNI_REGISTRY.items():
        canonical = UNI_SLUG_MERGE.get(slug, slug)
        if name not in seen:
            seen.add(name)
            unique_slugs.append(canonical if canonical in stats_map else slug)

    unique_slugs.sort(key=lambda sl: -(stats_map[sl]["last_all"] if sl in stats_map else 0))

    total_all    = sum(s["last_all"]    for s in uni_stats)
    total_new    = sum(s["last_new"]    for s in uni_stats)
    total_active = sum(s["last_active"] for s in uni_stats)
    total_cum    = sum(s["cumulative"]  for s in uni_stats)

    lines = [
        f"# 📊 Отчёт по ВУЗам — {today_str}",
        "",
        f"**Период:** {last_lbl}",
        f"**Сравнение с:** {prev_lbl}",
        "",
        f"> **Активный пользователь** = совершил 3 и более визитов за месяц",
        f"> **Новый пользователь** = первый визит за всю историю (Яндекс Метрика)",
        "",
        "## Сводка",
        "",
        "| Показатель | Значение |",
        "|---|---|",
        f"| ВУЗов в реестре | {len(unique_slugs)} |",
        f"| Накопленно уников (с 2024-01-01) | **{total_cum}** |",
        f"| Всего посетителей за {last_lbl} | **{total_all}** |",
        f"| Новых за {last_lbl} | **{total_new}** |",
        f"| Активных (3+ визитов) за {last_lbl} | **{total_active}** |",
        "",
        "---",
        "",
        "## Таблица по ВУЗам",
        "",
        f"| № | ВУЗ | Накопленно | Новых за {last_lbl} | Активных за {last_lbl} | Все посетители за {last_lbl} | Прирост vs {prev_lbl} | Рекомендации |",
        "|---|---|---|---|---|---|---|---|",
    ]

    for i, slug in enumerate(unique_slugs, 1):
        s    = stats_map.get(slug)
        name = UNI_REGISTRY.get(slug, slug)

        if s and s["last_all"] > 0:
            cum    = s["cumulative"]
            new_u  = s["last_new"]
            active = s["last_active"]
            all_u  = s["last_all"]
            g      = s["growth"]
            gp     = s["growth_pct"]
            rec    = make_recommendation(s)

            if gp is not None:
                sign  = "+" if g >= 0 else ""
                emoji = "📈" if g > 0 else ("📉" if g < 0 else "➡️")
                trend = f"{emoji} {sign}{g} ({sign}{gp}%)"
            else:
                trend = "—"
        else:
            cum = all_u = new_u = active = "—"
            trend = "—"
            rec = "Нет данных в Метрике"

        lines.append(f"| {i} | {name} | {cum} | {new_u} | {active} | {all_u} | {trend} | {rec} |")

    lines += ["", "---", "", "## Динамика по месяцам", ""]

    for slug in unique_slugs:
        s = stats_map.get(slug)
        if not s or not s.get("monthly"):
            continue
        if all(m["users"] == 0 for m in s["monthly"]):
            continue

        lines += [f"### 🎓 {s['name']}", "",
                  "| Месяц | Все посетители |", "|---|---|"]
        for m in s["monthly"][-3:]:
            lines.append(f"| {month_label(m['month'])} | {m['users']} |")

        top = s.get("top_products", {})
        if top:
            lines += ["", f"**Топ продуктов за {last_lbl}:**", "",
                      "| Продукт | Посетителей |", "|---|---|"]
            for pname, cnt in top.items():
                lines.append(f"| {pname} | {cnt} |")
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
        saved.append(save_markdown(uni_stats))
    except Exception:
        log.error("Markdown:\n%s", traceback.format_exc())

    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")

    try:
        png = chart_all_unis(uni_stats)
        p   = reports_dir / f"{ts}_unis_growth.png"
        p.write_bytes(png); saved.append(p)
    except Exception:
        log.error("Общий график:\n%s", traceback.format_exc())

    for s in uni_stats:
        try:
            png = chart_uni_products(s)
            if png:
                p = reports_dir / f"{ts}_{s['slug']}_products.png"
                p.write_bytes(png); saved.append(p)
        except Exception:
            log.warning("График %s:\n%s", s["slug"], traceback.format_exc())

    return saved


# ── Графики ───────────────────────────────────────────────────────────────────

def _style(ax, title: str):
    ax.set_title(title, fontsize=12, fontweight="bold", pad=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: str(int(x)))
    )
    ax.grid(axis="y", alpha=0.3, linestyle="--")


def chart_all_unis(uni_stats: list[dict]) -> bytes:
    """Grouped bar: все/новые/активные за последний месяц по каждому ВУЗу."""
    active = [s for s in uni_stats if s.get("last_all", 0) > 0]
    if not active:
        return _empty_chart("Нет данных")

    active = sorted(active, key=lambda s: -s["last_all"])[:10]
    names  = [s["name"].split("(")[0].strip()[:20] for s in active]
    all_v  = [s["last_all"]    for s in active]
    new_v  = [s["last_new"]    for s in active]
    act_v  = [s["last_active"] for s in active]

    x     = np.arange(len(active))
    w     = 0.25
    fig, ax = plt.subplots(figsize=(max(10, len(active) * 1.5), 6))

    ax.bar(x - w, all_v, w, label="Все посетители", color="#5B8DD9", alpha=0.85)
    ax.bar(x,     new_v, w, label="Новые",           color="#5BAD84", alpha=0.85)
    ax.bar(x + w, act_v, w, label="Активные (3+)",   color="#E06B5A", alpha=0.85)

    for xi, (a, n, ac) in enumerate(zip(all_v, new_v, act_v)):
        for offset, v in [(-w, a), (0, n), (w, ac)]:
            if v > 0:
                ax.text(xi + offset, v + 0.2, str(v),
                        ha="center", va="bottom", fontsize=7)

    sample   = next((s for s in uni_stats if s.get("last_ym")), None)
    last_lbl = month_label(sample["last_ym"]) if sample else ""

    ax.set_xticks(x)
    ax.set_xticklabels(names, rotation=20, ha="right", fontsize=9)
    _style(ax, f"Пользователи по ВУЗам — {last_lbl}")
    ax.legend(fontsize=9)
    fig.tight_layout()

    return _fig_to_bytes(fig)


def chart_uni_products(s: dict) -> bytes | None:
    pm = s.get("product_monthly", {})
    if not pm:
        return None

    today_ym = date.today().strftime("%Y-%m")
    all_ms   = sorted({r["month"] for recs in pm.values() for r in recs
                       if r["month"] != today_ym})[-3:]
    if not all_ms:
        return None

    totals       = {p: sum(r["users"] for r in recs if r["month"] != today_ym)
                    for p, recs in pm.items()}
    top_products = sorted(totals, key=lambda p: -totals[p])[:8]
    if not any(totals[p] > 0 for p in top_products):
        return None

    x      = np.arange(len(all_ms))
    bar_w  = 0.5
    fig, ax = plt.subplots(figsize=(max(8, len(all_ms) * 2.5), 5))
    bottom  = np.zeros(len(all_ms))

    for i, prefix in enumerate(top_products):
        month_map = {r["month"]: r["users"] for r in pm.get(prefix, [])}
        values    = np.array([month_map.get(m, 0) for m in all_ms], dtype=float)
        if values.sum() == 0:
            continue
        ax.bar(x, values, bar_w, bottom=bottom,
               label=_prettify_prefix(prefix, ""),
               color=PRODUCT_COLORS[i % len(PRODUCT_COLORS)], alpha=0.85)
        bottom += values

    ax.set_xticks(x)
    ax.set_xticklabels([month_label(m) for m in all_ms])
    _style(ax, f"{s['name']} — посетители по продуктам")
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()
    return _fig_to_bytes(fig)


def _empty_chart(text: str) -> bytes:
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.text(0.5, 0.5, text, ha="center", va="center", transform=ax.transAxes, fontsize=14)
    ax.axis("off")
    return _fig_to_bytes(fig)


def _fig_to_bytes(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ── Telegram ──────────────────────────────────────────────────────────────────

def _tg(base: str, method: str, **kwargs) -> dict:
    try:
        r = req_lib.post(f"{base}/{method}", timeout=30, **kwargs)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error("Telegram %s: %s", method, e)
        return {}


def send_report_to_telegram(token: str, chat_id, uni_stats: list[dict]):
    base = f"https://api.telegram.org/bot{token}"

    def text(t: str):
        for chunk in _split(t):
            _tg(base, "sendMessage",
                json={"chat_id": chat_id, "text": chunk, "parse_mode": "Markdown"})
            time.sleep(0.3)

    def photo(png: bytes, cap: str = ""):
        log.info("📤 График %d байт...", len(png))
        r = _tg(base, "sendPhoto",
                data={"chat_id": chat_id, "caption": cap},
                files={"photo": ("chart.png", png, "image/png")})
        log.info("✅ График" if r.get("ok") else f"❌ sendPhoto: {r}")
        time.sleep(0.5)

    def document(path: Path, cap: str = ""):
        log.info("📤 Документ %s...", path.name)
        r = _tg(base, "sendDocument",
                data={"chat_id": chat_id, "caption": cap},
                files={"document": (path.name, path.read_bytes(), "text/markdown")})
        log.info("✅ Документ" if r.get("ok") else f"❌ sendDocument: {r}")

    sample   = next((s for s in uni_stats if s.get("last_ym")), None)
    last_lbl = month_label(sample["last_ym"]) if sample else "—"
    prev_lbl = month_label(sample["prev_ym"]) if sample else "—"

    total_all    = sum(s["last_all"]    for s in uni_stats)
    total_new    = sum(s["last_new"]    for s in uni_stats)
    total_active = sum(s["last_active"] for s in uni_stats)
    total_cum    = sum(s["cumulative"]  for s in uni_stats)

    digest = "\n".join([
        f"📊 *Отчёт по ВУЗам — {datetime.now().strftime('%d.%m.%Y')}*",
        f"Период: *{last_lbl}* | Сравнение с *{prev_lbl}*",
        "",
        f"👥 Накопленно уников: *{total_cum}*",
        f"🆕 Новых за {last_lbl}: *{total_new}*",
        f"🔥 Активных (3+ визитов): *{total_active}*",
        f"👁 Всего посетителей: *{total_all}*",
        "",
        "📄 Подробная таблица — в прикреплённом файле",
    ])
    text(digest)

    try:
        photo(chart_all_unis(uni_stats), f"Пользователи по ВУЗам — {last_lbl}")
    except Exception:
        log.error("Общий график:\n%s", traceback.format_exc())

    md_path = save_markdown(uni_stats)
    document(md_path, f"Отчёт по ВУЗам {last_lbl}")

    for s in sorted(uni_stats, key=lambda s: -s.get("last_all", 0)):
        if s.get("last_all", 0) == 0:
            continue
        try:
            png = chart_uni_products(s)
            if png:
                photo(png, s["name"])
        except Exception:
            log.warning("График %s:\n%s", s["slug"], traceback.format_exc())

    log.info("✅ Отчёт отправлен")


def _split(text: str, limit: int = 4000) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks, cur, cur_len = [], [], 0
    for line in text.split("\n"):
        if cur_len + len(line) + 1 > limit:
            chunks.append("\n".join(cur)); cur, cur_len = [], 0
        cur.append(line); cur_len += len(line) + 1
    if cur:
        chunks.append("\n".join(cur))
    return chunks
