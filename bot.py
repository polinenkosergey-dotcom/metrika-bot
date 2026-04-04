"""
Telegram-бот — AI-аналитик платформы Сфера.

Два режима в зависимости от счётчика:

FLAT (обычный счётчик, напр. 101072037):
  /start → выбор счётчика → обнаружение продуктов → чекбоксы → анализ

LAYERED (счётчик с ВУЗами, напр. 102372602):
  /start → выбор счётчика → обнаружение ВУЗов → выбор ВУЗа →
           обнаружение продуктов ВУЗа → чекбоксы → анализ
  Также доступен сводный анализ по всем ВУЗам.
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode, ChatAction

from metrika import MetrikaClient, LAYERED_COUNTERS
from analyst import AnalystAgent
from reporter import collect_uni_stats, send_report_to_telegram, save_report_files

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bot")

# ── Конфигурация ──────────────────────────────────────────────────────────────

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
ANTHROPIC_KEY  = os.environ["ANTHROPIC_API_KEY"]
METRIKA_TOKEN  = os.environ["METRIKA_TOKEN"]
DEFAULT_COUNTER_ID = int(os.getenv("METRIKA_COUNTER_ID", "0")) or None

_raw = os.getenv("ALLOWED_USER_IDS", "")
ALLOWED_USERS: set[int] = (
    {int(x.strip()) for x in _raw.split(",") if x.strip()} if _raw else set()
)

PER_PAGE = 8

# Chat ID для автоматической ежемесячной рассылки отчёта
# Если не задан — автоотчёт отключён
REPORT_CHAT_ID = os.getenv("REPORT_CHAT_ID", "")

# ── Состояние пользователей ───────────────────────────────────────────────────
# { user_id: {
#     counter_id: int,
#     mode: "flat" | "layered",
#     unis: [...],          # только layered
#     selected_uni: dict,   # только layered
#     products: [...],
#     selected: [...],
#     product_page: int,
#     uni_page: int,
# }}
user_state: dict[int, dict] = {}

base_metrika = MetrikaClient(METRIKA_TOKEN, DEFAULT_COUNTER_ID)
agent = AnalystAgent(ANTHROPIC_KEY, base_metrika)


def state(uid: int) -> dict:
    user_state.setdefault(uid, {})
    return user_state[uid]


def get_metrika(uid: int) -> MetrikaClient:
    cid = state(uid).get("counter_id") or DEFAULT_COUNTER_ID
    return base_metrika.with_counter(cid) if cid else base_metrika


# ── Утилиты ───────────────────────────────────────────────────────────────────

def is_allowed(update: Update) -> bool:
    return not ALLOWED_USERS or update.effective_user.id in ALLOWED_USERS


def split_message(text: str, limit: int = 4000) -> list[str]:
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


async def send_long(update: Update, text: str):
    for chunk in split_message(text):
        await update.effective_message.reply_text(chunk)


async def typing_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id, action=ChatAction.TYPING
    )


def save_report(text: str, label: str):
    p = Path("reports")
    p.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    safe = label.replace(" ", "_").replace("/", "")
    (p / f"{ts}_{safe}.md").write_text(text, encoding="utf-8")


# ── Клавиатуры ────────────────────────────────────────────────────────────────

def counters_keyboard(counters: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for c in counters:
        label = f"#{c['id']} — {c['name']}"
        if c.get("layered"):
            label += " 🎓"  # метка что счётчик с ВУЗами
        rows.append([InlineKeyboardButton(label, callback_data=f"counter:{c['id']}")])
    return InlineKeyboardMarkup(rows)


def unis_keyboard(unis: list[dict], page: int = 0) -> InlineKeyboardMarkup:
    total_pages = max(1, (len(unis) + PER_PAGE - 1) // PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * PER_PAGE
    visible = unis[start: start + PER_PAGE]

    rows = [
        [InlineKeyboardButton(
            f"🎓 {u['name']} ({u['visits']:,} визитов)",
            callback_data=f"uni:{start + i}"
        )]
        for i, u in enumerate(visible)
    ]

    # Кнопка «Все ВУЗы сразу»
    rows.append([InlineKeyboardButton("📊 Анализ по всем ВУЗам", callback_data="uni_all")])

    nav = []
    if total_pages > 1:
        if page > 0:
            nav.append(InlineKeyboardButton("◀", callback_data=f"upage:{page - 1}"))
        nav.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("▶", callback_data=f"upage:{page + 1}"))
    if nav:
        rows.insert(-1, nav)  # навигация перед «Все ВУЗы»

    return InlineKeyboardMarkup(rows)


def products_keyboard(products: list[dict], selected_prefixes: set[str], page: int = 0) -> InlineKeyboardMarkup:
    total_pages = max(1, (len(products) + PER_PAGE - 1) // PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * PER_PAGE
    visible = products[start: start + PER_PAGE]

    rows = []
    for i, p in enumerate(visible):
        mark = "✅" if p["url_prefix"] in selected_prefixes else "⬜"
        rows.append([InlineKeyboardButton(
            f"{mark} {p['name']} ({p['visits']:,})",
            callback_data=f"toggle:{start + i}"
        )])

    nav = []
    if total_pages > 1:
        if page > 0:
            nav.append(InlineKeyboardButton("◀", callback_data=f"ppage:{page - 1}"))
        nav.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("▶", callback_data=f"ppage:{page + 1}"))

    ctrl = [
        InlineKeyboardButton("✅ Все", callback_data="select_all"),
        InlineKeyboardButton("🚀 Начать анализ", callback_data="start_analysis"),
    ]
    if nav:
        rows.append(nav)
    rows.append(ctrl)
    return InlineKeyboardMarkup(rows)


def product_list_keyboard(products: list[dict], page: int = 0) -> InlineKeyboardMarkup:
    total_pages = max(1, (len(products) + PER_PAGE - 1) // PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * PER_PAGE
    visible = products[start: start + PER_PAGE]

    rows = [
        [InlineKeyboardButton(
            f"{p['name']} ({p['visits']:,})",
            callback_data=f"analyze:{start + i}"
        )]
        for i, p in enumerate(visible)
    ]
    nav = []
    if total_pages > 1:
        if page > 0:
            nav.append(InlineKeyboardButton("◀", callback_data=f"lpage:{page - 1}"))
        nav.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("▶", callback_data=f"lpage:{page + 1}"))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(rows)


# ── Команды ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await _show_counter_selection(update, context)


async def cmd_counter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await _show_counter_selection(update, context)


async def _show_counter_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    counters = [
        {"id": 101072037, "name": "Платформа Сфера", "site": "sfera-t1.ru", "layered": False},
        {"id": 102372602, "name": "Сфера для вузов", "site": "saas.sferaplatform.ru", "layered": True},
    ]
    kb = counters_keyboard(counters)
    await update.effective_message.reply_text(
        "📊 Выберите счётчик:\n\n🎓 — счётчик с разбивкой по ВУЗам",
        reply_markup=kb,
    )


async def _after_counter_selected(update: Update, context: ContextTypes.DEFAULT_TYPE, counter_id: int):
    """Запускает нужный флоу в зависимости от типа счётчика."""
    uid = update.effective_user.id
    metrika = base_metrika.with_counter(counter_id)

    if metrika.is_layered():
        # LAYERED: сначала показываем ВУЗы
        state(uid)["mode"] = "layered"
        await _discover_unis(update, context, metrika)
    else:
        # FLAT: сразу показываем продукты
        state(uid)["mode"] = "flat"
        await _discover_products(update, context, metrika)


# ── FLAT флоу ─────────────────────────────────────────────────────────────────

async def _discover_products(update: Update, context: ContextTypes.DEFAULT_TYPE, metrika: MetrikaClient, uni_context: str = ""):
    """Обнаруживает продукты и показывает чекбоксы."""
    uid = update.effective_user.id
    title = f"для *{uni_context}*" if uni_context else "в счётчике"

    msg = await update.effective_message.reply_text(
        f"🔍 Обнаруживаю продукты {title}...\n⏳ ~10 секунд"
    )

    try:
        loop = asyncio.get_event_loop()
        products = await loop.run_in_executor(None, metrika.discover_products)
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка: {e}")
        return

    if not products:
        await msg.edit_text("❌ Продукты не найдены.")
        return

    state(uid)["products"] = products
    state(uid)["selected"] = []
    state(uid)["product_page"] = 0

    kb = products_keyboard(products, set(), 0)
    await msg.edit_text(
        f"✅ Обнаружено продуктов: *{len(products)}*\n\n"
        "Выберите продукты и нажмите *🚀 Начать анализ*:",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )


# ── LAYERED флоу ──────────────────────────────────────────────────────────────

async def _discover_unis(update: Update, context: ContextTypes.DEFAULT_TYPE, metrika: MetrikaClient):
    """Обнаруживает ВУЗы и показывает список."""
    uid = update.effective_user.id

    msg = await update.effective_message.reply_text(
        "🔍 Обнаруживаю ВУЗы в счётчике...\n⏳ ~10 секунд"
    )

    try:
        loop = asyncio.get_event_loop()
        unis = await loop.run_in_executor(None, metrika.discover_unis)
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка: {e}")
        return

    if not unis:
        await msg.edit_text("❌ ВУЗы не найдены. Попробуйте flat-режим.")
        return

    state(uid)["unis"] = unis
    state(uid)["uni_page"] = 0

    kb = unis_keyboard(unis, 0)
    await msg.edit_text(
        f"🎓 Найдено ВУЗов: *{len(unis)}*\n\n"
        "Выберите ВУЗ для анализа или запустите *анализ по всем ВУЗам*:",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )


# ── Команды ───────────────────────────────────────────────────────────────────

async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Полный отчёт по ВУЗам: уники, прирост, графики."""
    if not is_allowed(update):
        return
    uid = update.effective_user.id

    # Всегда используем layered счётчик для этого отчёта
    LAYERED_COUNTER = 102372602
    metrika = base_metrika.with_counter(LAYERED_COUNTER)

    status_msg = await update.message.reply_text(
        "📊 Формирую отчёт по ВУЗам...\n\n"
        "⏳ Собираю данные из Метрики (~1-2 мин)"
    )

    try:
        loop = asyncio.get_event_loop()
        from metrika import UNI_REGISTRY, UNI_SLUG_MERGE, _is_gateway_slug

        # Обнаруживаем ВУЗы из Метрики
        unis = await loop.run_in_executor(None, metrika.discover_unis)

        # Дополняем ВУЗами из реестра у которых нет данных в Метрике
        found_slugs = {u["slug"] for u in unis}
        for slug, name in UNI_REGISTRY.items():
            canonical = UNI_SLUG_MERGE.get(slug, slug)
            if canonical not in found_slugs and slug not in found_slugs:
                # ВУЗ из реестра, но нет данных — добавляем с пустым хостом
                unis.append({"slug": slug, "name": name, "hosts": [], "host": ""})
                found_slugs.add(slug)

        await status_msg.edit_text(
            f"📊 ВУЗов в реестре: *{len(set(UNI_REGISTRY.values()))}*\n"
            f"⏳ Собираю метрики...",
            parse_mode=ParseMode.MARKDOWN,
        )

        # Собираем статистику только по ВУЗам с хостами
        uni_stats = []
        unis_with_hosts = [u for u in unis if u.get("hosts") or u.get("host")]
        for i, uni in enumerate(unis_with_hosts):
            await status_msg.edit_text(
                f"📊 {i+1}/{len(unis_with_hosts)}: *{uni['name']}*...",
                parse_mode=ParseMode.MARKDOWN,
            )
            s = await loop.run_in_executor(
                None, lambda u=uni: collect_uni_stats(metrika, u)
            )
            uni_stats.append(s)

        await status_msg.edit_text("🖼 Строю графики и отправляю отчёт...")

        tg_token = os.environ["TELEGRAM_TOKEN"]
        chat_id  = update.effective_chat.id

        # Отправляем
        await loop.run_in_executor(
            None,
            lambda: send_report_to_telegram(tg_token, chat_id, uni_stats)
        )

        await status_msg.edit_text("✅ Отчёт сформирован и отправлен")

    except Exception as e:
        log.exception("Ошибка /report")
        await status_msg.edit_text(f"❌ Ошибка: {e}")



async def cmd_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    uid = update.effective_user.id
    products = state(uid).get("selected") or state(uid).get("products", [])

    if not products:
        await update.message.reply_text("⚠️ Сначала выберите счётчик через /start")
        return

    uni = state(uid).get("selected_uni")
    title = f"продукты ВУЗа *{uni['name']}*" if uni else f"продукты ({len(products)} шт.)"
    kb = product_list_keyboard(products, 0)
    await update.message.reply_text(
        f"Выберите продукт для анализа — {title}:",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_unis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать список ВУЗов (только для layered-счётчика)."""
    if not is_allowed(update):
        return
    uid = update.effective_user.id
    unis = state(uid).get("unis", [])
    if not unis:
        await update.message.reply_text("⚠️ ВУЗы не загружены. Используйте /start")
        return
    kb = unis_keyboard(unis, 0)
    await update.message.reply_text(
        f"🎓 ВУЗы ({len(unis)} шт.):", reply_markup=kb
    )


async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    question = " ".join(context.args) if context.args else ""
    if not question:
        await update.message.reply_text(
            "Укажите вопрос:\n`/ask Почему упал трафик?`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    await _run_question(update, context, question)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    uid = update.effective_user.id
    cid = state(uid).get("counter_id") or DEFAULT_COUNTER_ID
    if not cid:
        await update.message.reply_text("⚠️ Счётчик не выбран. /start")
        return
    await typing_action(update, context)
    try:
        metrika = base_metrika.with_counter(cid)
        d1, d2 = MetrikaClient.week_range(0)
        summary = metrika.get_summary()
        visits = summary.get("this_week", {}).get("visits", "?")
        mode = state(uid).get("mode", "—")
        uni = state(uid).get("selected_uni")
        uni_info = f"\nВУЗ: *{uni['name']}*" if uni else ""
        products = state(uid).get("products", [])
        selected = state(uid).get("selected", [])
        await update.message.reply_text(
            f"✅ *Метрика подключена*\n"
            f"Счётчик: `{cid}` | режим: `{mode}`{uni_info}\n"
            f"Неделя: {d1} — {d2}\n"
            f"Визитов: *{visits:,}*\n"
            f"Продуктов: *{len(products)}* | выбрано: *{len(selected)}*",
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: `{e}`", parse_mode=ParseMode.MARKDOWN)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await update.message.reply_text(
        "🤖 *Как пользоваться*\n\n"
        "/start — выбрать счётчик\n"
        "/counter — сменить счётчик\n"
        "/product — анализ продукта\n"
        "/unis — выбрать ВУЗ (для счётчика с ВУЗами)\n"
        "/ask — вопрос по данным\n"
        "/status — состояние\n\n"
        "*Примеры вопросов:*\n"
        "`Почему упал трафик на /tasks?`\n"
        "`Сравни /sd и /knowledge`\n"
        "`Какой ВУЗ показал лучший рост?`",
        parse_mode=ParseMode.MARKDOWN,
    )


# ── Callback-и ────────────────────────────────────────────────────────────────

async def callback_counter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_allowed(update):
        return

    counter_id = int(query.data.split(":")[1])
    uid = update.effective_user.id
    # Сбрасываем состояние при смене счётчика
    user_state[uid] = {"counter_id": counter_id}

    is_layered = counter_id in LAYERED_COUNTERS
    label = "🎓 ВУЗы + продукты" if is_layered else "продукты"
    await query.edit_message_text(
        f"✅ Счётчик `{counter_id}` выбран ({label}).\n🔍 Анализирую структуру...",
        parse_mode=ParseMode.MARKDOWN,
    )
    await _after_counter_selected(update, context, counter_id)


async def callback_uni(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь выбрал конкретный ВУЗ."""
    query = update.callback_query
    await query.answer()

    uid = update.effective_user.id
    idx = int(query.data.split(":")[1])
    unis = state(uid).get("unis", [])
    if idx >= len(unis):
        return

    uni = unis[idx]
    state(uid)["selected_uni"] = uni

    await query.edit_message_text(
        f"🎓 ВУЗ: *{uni['name']}*\n🔍 Загружаю продукты...",
        parse_mode=ParseMode.MARKDOWN,
    )

    metrika = get_metrika(uid)
    loop = asyncio.get_event_loop()
    try:
        products = await loop.run_in_executor(
            None, lambda: metrika.discover_products_for_uni(uni["slug"])
        )
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Ошибка: {e}")
        return

    if not products:
        await update.effective_message.reply_text(
            f"❌ Продукты для ВУЗа *{uni['name']}* не найдены.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    state(uid)["products"] = products
    state(uid)["selected"] = []
    state(uid)["product_page"] = 0

    kb = products_keyboard(products, set(), 0)
    await update.effective_message.reply_text(
        f"🎓 *{uni['name']}* — продуктов: *{len(products)}*\n\n"
        "Выберите продукты и нажмите *🚀 Начать анализ*:",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )


async def callback_uni_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Анализ по всем ВУЗам сразу."""
    query = update.callback_query
    await query.answer()

    uid = update.effective_user.id
    state(uid)["selected_uni"] = None  # все ВУЗы

    unis = state(uid).get("unis", [])
    metrika = get_metrika(uid)

    status_msg = await query.edit_message_text(
        f"📊 Готовлю сводный анализ по *{len(unis)}* ВУЗам...\n⏳ Это займёт несколько минут",
        parse_mode=ParseMode.MARKDOWN,
    )

    d1, d2 = MetrikaClient.week_range(0)

    # Список ВУЗов с хостами для промпта
    unis_list = "\n".join(
        f"  {u['name']}: filter_host='{u['host']}', url_prefix='' ({u['visits']:,} визитов)"
        for u in unis
    )

    prompt = (
        f"Проведи сводный анализ по всем ВУЗам в счётчике за неделю {d1}–{d2}.\n\n"
        f"Список ВУЗов (для каждого указан filter_host для фильтрации):\n{unis_list}\n\n"
        "Для каждого ВУЗа вызови get_summary_metrics с его filter_host (url_prefix='').\n"
        "Затем дай:\n"
        "• Топ-3 ВУЗа по трафику\n"
        "• ВУЗы с падением трафика >20%\n"
        "• ВУЗы с аномальным bounce rate >70%\n"
        "• Общий вывод: где проблемы, где рост"
    )

    local_agent = AnalystAgent(ANTHROPIC_KEY, metrika)
    loop = asyncio.get_event_loop()

    async def set_status(text: str):
        try:
            await status_msg.edit_text(f"⚙️ {text}", parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass

    def progress(msg: str):
        asyncio.run_coroutine_threadsafe(set_status(msg), loop)

    try:
        result = await loop.run_in_executor(None, lambda: local_agent.run(prompt, progress))
        await status_msg.edit_text("✅ Сводный анализ по ВУЗам готов")
        await send_long(update, result)
        save_report(result, "all_unis")
    except Exception as e:
        log.exception("Ошибка uni_all")
        await status_msg.edit_text(f"❌ Ошибка: {e}")


async def callback_upage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пагинация в списке ВУЗов."""
    query = update.callback_query
    await query.answer()
    uid = update.effective_user.id
    page = int(query.data.split(":")[1])
    state(uid)["uni_page"] = page
    unis = state(uid).get("unis", [])
    kb = unis_keyboard(unis, page)
    await query.edit_message_reply_markup(reply_markup=kb)


async def callback_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    uid = update.effective_user.id
    idx = int(query.data.split(":")[1])
    products = state(uid).get("products", [])
    if idx >= len(products):
        return

    prefix = products[idx]["url_prefix"]
    selected = state(uid).get("selected", [])
    sel_prefixes = {p["url_prefix"] for p in selected}

    if prefix in sel_prefixes:
        selected = [p for p in selected if p["url_prefix"] != prefix]
    else:
        selected.append(products[idx])
    state(uid)["selected"] = selected

    page = state(uid).get("product_page", 0)
    sel_prefixes = {p["url_prefix"] for p in selected}
    kb = products_keyboard(products, sel_prefixes, page)

    uni = state(uid).get("selected_uni")
    uni_info = f"🎓 *{uni['name']}* — " if uni else ""
    await query.edit_message_text(
        f"{uni_info}Выбрано: *{len(selected)}* из *{len(products)}*\n\n"
        "Нажмите *🚀 Начать анализ*:",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )


async def callback_select_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    uid = update.effective_user.id
    products = state(uid).get("products", [])
    state(uid)["selected"] = list(products)

    page = state(uid).get("product_page", 0)
    sel_prefixes = {p["url_prefix"] for p in products}
    kb = products_keyboard(products, sel_prefixes, page)

    uni = state(uid).get("selected_uni")
    uni_info = f"🎓 *{uni['name']}* — " if uni else ""
    await query.edit_message_text(
        f"{uni_info}Выбраны все *{len(products)}* продуктов.\n\n"
        "Нажмите *🚀 Начать анализ*:",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )


async def callback_start_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = update.effective_user.id
    selected = state(uid).get("selected", [])

    if not selected:
        await query.answer("⚠️ Выберите хотя бы один продукт!", show_alert=True)
        return

    await query.answer()
    uni = state(uid).get("selected_uni")
    uni_info = f" (ВУЗ: {uni['name']})" if uni else ""
    names = "\n".join(f"• {p['name']}" for p in selected)

    await query.edit_message_text(
        f"🚀 *Выбрано для анализа{uni_info}: {len(selected)} продуктов*\n\n{names}",
        parse_mode=ParseMode.MARKDOWN,
    )

    kb = product_list_keyboard(selected, 0)
    await update.effective_message.reply_text(
        "Выберите продукт для детального отчёта:",
        reply_markup=kb,
    )


async def callback_ppage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = update.effective_user.id
    page = int(query.data.split(":")[1])
    state(uid)["product_page"] = page
    products = state(uid).get("products", [])
    selected = state(uid).get("selected", [])
    sel_prefixes = {p["url_prefix"] for p in selected}
    kb = products_keyboard(products, sel_prefixes, page)
    await query.edit_message_reply_markup(reply_markup=kb)


async def callback_lpage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = update.effective_user.id
    page = int(query.data.split(":")[1])
    products = state(uid).get("selected") or state(uid).get("products", [])
    kb = product_list_keyboard(products, page)
    await query.edit_message_reply_markup(reply_markup=kb)


async def callback_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_allowed(update):
        return

    uid = update.effective_user.id
    idx = int(query.data.split(":")[1])
    products = state(uid).get("selected") or state(uid).get("products", [])

    if idx >= len(products):
        await query.edit_message_text("❌ Продукт не найден")
        return

    product = products[idx]
    name = product["name"]
    prefix = product["url_prefix"]
    uni = state(uid).get("selected_uni")
    uni_context = f" (ВУЗ: {uni['name']})" if uni else ""

    status_msg = await query.edit_message_text(
        f"🔍 *{name}*{uni_context} (`{prefix}`)\n\n⏳ Запрашиваю данные...",
        parse_mode=ParseMode.MARKDOWN,
    )

    d1, d2 = MetrikaClient.week_range(0)

    # Для layered-продуктов передаём filter_host чтобы агент использовал его в инструментах
    filter_host = product.get("filter_host") or ""
    filter_hosts = product.get("filter_hosts") or ([filter_host] if filter_host else [])
    host_hint = ""
    if filter_hosts:
        hosts_str = ", ".join(f"'{h}'" for h in filter_hosts)
        host_hint = (
            f"\nВАЖНО: это продукт ВУЗа. При каждом вызове инструментов передавай:\n"
            f"  url_prefix='{prefix}'\n"
            f"  filter_hosts=[{hosts_str}]\n"
            + (f"  (и filter_host='{filter_hosts[0]}' как основной)\n" if filter_hosts else "")
        )

    prompt = (
        f"Проанализируй продукт «{name}»{uni_context}.\n"
        f"url_prefix: {prefix}\n"
        f"filter_host: {filter_host or '(не задан)'}\n"
        f"Период: {d1}–{d2}\n"
        f"{host_hint}\n"
        "Получи:\n"
        "1. get_summary_metrics — метрики + WoW\n"
        "2. get_traffic_sources — источники\n"
        "3. get_top_pages — топ страниц\n"
        "4. get_devices — устройства\n\n"
        "Ответ:\n"
        "🏥 Health score X/10 + обоснование\n"
        "📊 Ключевые цифры\n"
        "🔴 Проблемы с конкретными страницами и цифрами\n"
        "🟢 Позитивное\n"
        "🎯 Одно действие на следующую неделю"
    )

    metrika = get_metrika(uid)
    local_agent = AnalystAgent(ANTHROPIC_KEY, metrika)
    loop = asyncio.get_event_loop()

    async def set_status(text: str):
        try:
            await status_msg.edit_text(
                f"🔍 *{name}*\n\n{text}", parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass

    def progress(msg: str):
        asyncio.run_coroutine_threadsafe(set_status(msg), loop)

    try:
        result = await loop.run_in_executor(None, lambda: local_agent.run(prompt, progress))
        await status_msg.edit_text(f"✅ *{name}* — готово", parse_mode=ParseMode.MARKDOWN)
        await send_long(update, result)
        save_report(result, name)
    except Exception as e:
        log.exception("Ошибка анализа: %s", name)
        await status_msg.edit_text(f"❌ Ошибка: {e}")


async def callback_noop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()


# ── Свободный вопрос ──────────────────────────────────────────────────────────

async def handle_free_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await _run_question(update, context, update.message.text)


async def _run_question(update: Update, context: ContextTypes.DEFAULT_TYPE, question: str):
    await typing_action(update, context)
    uid = update.effective_user.id
    products = state(uid).get("selected") or state(uid).get("products", [])
    unis = state(uid).get("unis", [])

    ctx_parts = []
    if unis:
        ctx_parts.append("ВУЗы: " + ", ".join(f"{u['name']} ({u['url_prefix']})" for u in unis))
    if products:
        ctx_parts.append("Продукты:\n" + "\n".join(f"  {p['name']} → {p['url_prefix']}" for p in products))

    ctx_str = "\n".join(ctx_parts)
    prompt = f"{ctx_str}\n\nВопрос: {question}\n\nИспользуй инструменты и дай точный ответ с цифрами."

    status_msg = await update.effective_message.reply_text("🤔 Анализирую...")
    metrika = get_metrika(uid)
    local_agent = AnalystAgent(ANTHROPIC_KEY, metrika)
    loop = asyncio.get_event_loop()

    async def set_status(text: str):
        try:
            await status_msg.edit_text(f"⚙️ {text}")
        except Exception:
            pass

    def progress(msg: str):
        asyncio.run_coroutine_threadsafe(set_status(msg), loop)

    try:
        result = await loop.run_in_executor(None, lambda: local_agent.run(prompt, progress))
        await status_msg.delete()
        await send_long(update, result)
    except Exception as e:
        log.exception("Ошибка вопроса")
        await status_msg.edit_text(f"❌ Ошибка: {e}")


# ── Запуск ────────────────────────────────────────────────────────────────────

async def _run_monthly_report(context) -> None:
    """Автоматический ежемесячный отчёт по ВУЗам."""
    chat_id = REPORT_CHAT_ID
    if not chat_id:
        return
    log.info("⏰ Запускаю автоматический ежемесячный отчёт...")
    try:
        metrika = base_metrika.with_counter(102372602)
        loop = asyncio.get_event_loop()
        unis = await loop.run_in_executor(None, metrika.discover_unis)
        uni_stats = []
        for uni in unis:
            s = await loop.run_in_executor(
                None, lambda u=uni: collect_uni_stats(metrika, u, months=3)
            )
            uni_stats.append(s)
        save_report_files(uni_stats)
        await loop.run_in_executor(
            None,
            lambda: send_report_to_telegram(TELEGRAM_TOKEN, chat_id, uni_stats, months=3)
        )
        log.info("✅ Автоотчёт отправлен в %s", chat_id)
    except Exception as e:
        log.exception("Ошибка автоотчёта: %s", e)


async def post_init(app: Application):
    await app.bot.set_my_commands([
        BotCommand("start",   "Выбрать счётчик и продукты"),
        BotCommand("counter", "Сменить счётчик"),
        BotCommand("report",  "Отчёт по ВУЗам с графиками"),
        BotCommand("product", "Анализ продукта"),
        BotCommand("unis",    "Выбрать ВУЗ"),
        BotCommand("ask",     "Вопрос по данным"),
        BotCommand("status",  "Состояние"),
        BotCommand("help",    "Справка"),
    ])

    # Ежемесячный автоотчёт — 1-го числа каждого месяца в 09:00
    if REPORT_CHAT_ID:
        from datetime import time as dtime
        app.job_queue.run_monthly(
            _run_monthly_report,
            when=dtime(9, 0),
            day=1,
            name="monthly_uni_report",
        )
        log.info("⏰ Ежемесячный автоотчёт настроен → chat_id=%s", REPORT_CHAT_ID)
    else:
        log.info("Автоотчёт отключён (REPORT_CHAT_ID не задан)")


def main():
    log.info("Запуск бота...")
    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("counter", cmd_counter))
    app.add_handler(CommandHandler("help",    cmd_help))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("report",  cmd_report))
    app.add_handler(CommandHandler("product", cmd_product))
    app.add_handler(CommandHandler("unis",    cmd_unis))
    app.add_handler(CommandHandler("ask",     cmd_ask))

    app.add_handler(CallbackQueryHandler(callback_counter,        pattern=r"^counter:\d+$"))
    app.add_handler(CallbackQueryHandler(callback_uni,            pattern=r"^uni:\d+$"))
    app.add_handler(CallbackQueryHandler(callback_uni_all,        pattern=r"^uni_all$"))
    app.add_handler(CallbackQueryHandler(callback_upage,          pattern=r"^upage:\d+$"))
    app.add_handler(CallbackQueryHandler(callback_toggle,         pattern=r"^toggle:\d+$"))
    app.add_handler(CallbackQueryHandler(callback_select_all,     pattern=r"^select_all$"))
    app.add_handler(CallbackQueryHandler(callback_start_analysis, pattern=r"^start_analysis$"))
    app.add_handler(CallbackQueryHandler(callback_ppage,          pattern=r"^ppage:\d+$"))
    app.add_handler(CallbackQueryHandler(callback_lpage,          pattern=r"^lpage:\d+$"))
    app.add_handler(CallbackQueryHandler(callback_analyze,        pattern=r"^analyze:\d+$"))
    app.add_handler(CallbackQueryHandler(callback_noop,           pattern=r"^noop$"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_free_text))

    log.info("Бот запущен ✅")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
