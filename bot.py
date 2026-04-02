"""
Telegram-бот — AI-аналитик платформы Сфера.

Команды:
  /start    — приветствие
  /product  — выбор продукта кнопками → отчёт
  /ask      — вопрос агенту в свободной форме
  /status   — проверка подключения к Метрике
  /help     — справка

Любое текстовое сообщение без команды тоже уходит агенту.

Запуск:
  python bot.py
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

from metrika import MetrikaClient
from analyst import AnalystAgent

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
COUNTER_ID     = int(os.environ["METRIKA_COUNTER_ID"])

_raw = os.getenv("ALLOWED_USER_IDS", "")
ALLOWED_USERS: set[int] = (
    {int(x.strip()) for x in _raw.split(",") if x.strip()} if _raw else set()
)

PRODUCTS_PER_PAGE = 8
PRODUCTS_FILE     = Path("products.json")

# ── Инициализация ─────────────────────────────────────────────────────────────

metrika = MetrikaClient(METRIKA_TOKEN, COUNTER_ID)
agent   = AnalystAgent(ANTHROPIC_KEY, metrika)


def load_products() -> list[dict]:
    if PRODUCTS_FILE.exists():
        return json.loads(PRODUCTS_FILE.read_text(encoding="utf-8"))
    return []


# ── Утилиты ───────────────────────────────────────────────────────────────────

def is_allowed(update: Update) -> bool:
    return not ALLOWED_USERS or update.effective_user.id in ALLOWED_USERS


def split_message(text: str, limit: int = 4000) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    current: list[str] = []
    cur_len = 0
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


def products_keyboard(page: int = 0) -> tuple[InlineKeyboardMarkup, int]:
    """
    Строит клавиатуру с пагинацией.
    Возвращает (markup, total_pages).
    """
    products    = load_products()
    total       = len(products)
    total_pages = max(1, (total + PRODUCTS_PER_PAGE - 1) // PRODUCTS_PER_PAGE)
    page        = max(0, min(page, total_pages - 1))
    start       = page * PRODUCTS_PER_PAGE
    visible     = products[start : start + PRODUCTS_PER_PAGE]

    rows = [
        [InlineKeyboardButton(p["name"], callback_data=f"product:{start + i}")]
        for i, p in enumerate(visible)
    ]

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀ Назад", callback_data=f"page:{page - 1}"))
        nav.append(InlineKeyboardButton(f"{page + 1} / {total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("Вперёд ▶", callback_data=f"page:{page + 1}"))
        rows.append(nav)

    return InlineKeyboardMarkup(rows), total_pages


def save_report(text: str, label: str):
    p = Path("reports")
    p.mkdir(exist_ok=True)
    ts   = datetime.now().strftime("%Y-%m-%d_%H-%M")
    safe = label.replace(" ", "_").replace("/", "")
    (p / f"{ts}_{safe}.md").write_text(text, encoding="utf-8")
    log.info("📁 reports/%s_%s.md", ts, safe)


# ── Команды ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await update.message.reply_text(
        "👋 *Аналитик платформы Сфера*\n\n"
        "Подключён к Яндекс Метрике. Анализирую продукты по URL-сегментам.\n\n"
        "📋 *Что умею:*\n"
        "/product — выбрать продукт и получить недельный отчёт\n"
        "/ask — задать любой вопрос по данным Метрики\n"
        "/status — проверить подключение\n\n"
        "Или просто напишите вопрос 👇",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await update.message.reply_text(
        "🤖 *Как пользоваться*\n\n"
        "*Отчёт по продукту:*\n"
        "/product → выберите из списка\n\n"
        "*Свободный вопрос (примеры):*\n"
        "`Почему упал трафик на /hr на прошлой неделе?`\n"
        "`Сравни bounce rate для /finance и /dms`\n"
        "`Какой продукт показал лучший рост?`\n"
        "`Есть ли проблемы с мобильным трафиком?`\n"
        "`Откуда идёт трафик на /procurement?`\n\n"
        "/status — проверить соединение с Метрикой",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await typing_action(update, context)
    try:
        d1, d2   = MetrikaClient.week_range(0)
        summary  = metrika.get_summary()
        visits   = summary.get("this_week", {}).get("visits", "?")
        products = load_products()
        await update.message.reply_text(
            f"✅ *Метрика подключена*\n"
            f"Счётчик: `{COUNTER_ID}`\n"
            f"Неделя: {d1} — {d2}\n"
            f"Визитов по счётчику: *{visits:,}*\n"
            f"Продуктов в реестре: *{len(products)}*",
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        await update.message.reply_text(
            f"❌ Ошибка подключения:\n`{e}`",
            parse_mode=ParseMode.MARKDOWN,
        )


async def cmd_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    products = load_products()
    if not products:
        await update.message.reply_text(
            "⚠️ Список продуктов пуст. Заполните `products.json`.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    kb, _ = products_keyboard(0)
    await update.message.reply_text(
        f"Выберите продукт ({len(products)} шт.):",
        reply_markup=kb,
    )


async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    question = " ".join(context.args) if context.args else ""
    if not question:
        await update.message.reply_text(
            "Укажите вопрос:\n`/ask Почему упал трафик на /hr?`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    await _run_question(update, context, question)


async def handle_free_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await _run_question(update, context, update.message.text)


# ── Callback-и ────────────────────────────────────────────────────────────────

async def callback_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page     = int(query.data.split(":")[1])
    products = load_products()
    kb, _    = products_keyboard(page)
    await query.edit_message_text(
        f"Выберите продукт ({len(products)} шт.):",
        reply_markup=kb,
    )


async def callback_noop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()


async def callback_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_allowed(update):
        return

    idx      = int(query.data.split(":")[1])
    products = load_products()
    if idx >= len(products):
        await query.edit_message_text("❌ Продукт не найден")
        return

    product = products[idx]
    name    = product["name"]
    prefix  = product["url_prefix"]

    status_msg = await query.edit_message_text(
        f"🔍 *{name}* (`{prefix}`)\n\n⏳ Запрашиваю данные…",
        parse_mode=ParseMode.MARKDOWN,
    )

    d1, d2 = MetrikaClient.week_range(0)
    prompt = (
        f"Проанализируй продукт «{name}» (URL-префикс: {prefix}) "
        f"за неделю {d1}–{d2}.\n\n"
        "Получи последовательно:\n"
        "1. get_summary_metrics — сводные метрики + WoW-сравнение\n"
        "2. get_traffic_sources — источники трафика\n"
        "3. get_top_pages — топ страниц\n"
        "4. get_devices — устройства\n\n"
        "Структура ответа:\n"
        "🏥 Health score X/10 + одна фраза почему\n"
        "📊 Ключевые цифры: визиты, WoW-дельта, bounce rate\n"
        "🔴 Проблемы: конкретная страница/метрика/цифра + рекомендация\n"
        "🟢 Что работает хорошо\n"
        "🎯 Одно действие на следующую неделю\n\n"
        "Пиши кратко. Эмодзи для структуры. Только факты и цифры."
    )

    loop = asyncio.get_event_loop()

    async def set_status(text: str):
        try:
            await status_msg.edit_text(
                f"🔍 *{name}*\n\n{text}",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass

    def progress(msg: str):
        asyncio.run_coroutine_threadsafe(set_status(msg), loop)

    try:
        result = await loop.run_in_executor(
            None, lambda: agent.run(prompt, progress)
        )
        await status_msg.edit_text(
            f"✅ *{name}* — отчёт готов", parse_mode=ParseMode.MARKDOWN
        )
        await send_long(update, result)
        save_report(result, name)
    except Exception as e:
        log.exception("Ошибка анализа: %s", name)
        await status_msg.edit_text(f"❌ Ошибка при анализе {name}:\n{e}")


# ── Агент на произвольный вопрос ──────────────────────────────────────────────

async def _run_question(update: Update, context: ContextTypes.DEFAULT_TYPE, question: str):
    await typing_action(update, context)

    products = load_products()
    ctx_str  = (
        "Продукты платформы (название → URL-префикс):\n"
        + "\n".join(f"  {p['name']} → {p['url_prefix']}" for p in products)
        if products else ""
    )

    prompt = (
        f"{ctx_str}\n\n"
        f"Вопрос: {question}\n\n"
        "Используй инструменты чтобы получить данные из Метрики и дай точный ответ с цифрами. "
        "Если вопрос о конкретном продукте — запроси его метрики. "
        "Если сравнение — запроси оба. Отвечай кратко и по делу."
    )

    status_msg = await update.effective_message.reply_text("🤔 Анализирую…")
    loop       = asyncio.get_event_loop()

    async def set_status(text: str):
        try:
            await status_msg.edit_text(f"⚙️ {text}")
        except Exception:
            pass

    def progress(msg: str):
        asyncio.run_coroutine_threadsafe(set_status(msg), loop)

    try:
        result = await loop.run_in_executor(
            None, lambda: agent.run(prompt, progress)
        )
        await status_msg.delete()
        await send_long(update, result)
    except Exception as e:
        log.exception("Ошибка вопроса")
        await status_msg.edit_text(f"❌ Ошибка: {e}")


# ── Запуск ────────────────────────────────────────────────────────────────────

async def post_init(app: Application):
    await app.bot.set_my_commands([
        BotCommand("product", "Отчёт по продукту"),
        BotCommand("ask",     "Вопрос по данным Метрики"),
        BotCommand("status",  "Проверить подключение"),
        BotCommand("help",    "Справка"),
        BotCommand("start",   "Начало"),
    ])


def main():
    log.info("Запуск бота…")
    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("help",    cmd_help))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("product", cmd_product))
    app.add_handler(CommandHandler("ask",     cmd_ask))

    app.add_handler(CallbackQueryHandler(callback_product, pattern=r"^product:\d+$"))
    app.add_handler(CallbackQueryHandler(callback_page,    pattern=r"^page:\d+$"))
    app.add_handler(CallbackQueryHandler(callback_noop,    pattern=r"^noop$"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_free_text))

    log.info("Бот запущен ✅")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
