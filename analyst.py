"""
AI-агент аналитики.
Содержит инструменты (tools) и агентный цикл на базе Claude API.
Не зависит от Telegram — принимает запрос строкой, возвращает текст.
"""

import json
import logging
from typing import Any

import anthropic

from metrika import MetrikaClient

log = logging.getLogger("analyst")

TOOLS = [
    {
        "name": "get_summary_metrics",
        "description": (
            "Получить сводные метрики продукта за текущую и прошлую неделю: "
            "визиты, уники, bounce rate, глубина просмотра, время на сайте, WoW-дельта."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string", "description": "Название продукта"},
                "url_prefix": {
                    "type": "string",
                    "description": "URL-префикс продукта, например /hr или /finance. Пустая строка — весь счётчик.",
                },
            },
            "required": ["product_name", "url_prefix"],
        },
    },
    {
        "name": "get_traffic_sources",
        "description": "Получить разбивку трафика по источникам (organic, direct, referral, social, ad и др.).",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string"},
                "url_prefix": {"type": "string"},
            },
            "required": ["product_name", "url_prefix"],
        },
    },
    {
        "name": "get_top_pages",
        "description": "Получить топ страниц по визитам с bounce rate для каждой страницы.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string"},
                "url_prefix": {"type": "string"},
                "limit": {
                    "type": "integer",
                    "description": "Количество страниц, по умолчанию 10",
                    "default": 10,
                },
            },
            "required": ["product_name", "url_prefix"],
        },
    },
    {
        "name": "get_devices",
        "description": "Получить разбивку трафика по типам устройств.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string"},
                "url_prefix": {"type": "string"},
            },
            "required": ["product_name", "url_prefix"],
        },
    },
]

SYSTEM_PROMPT = """Ты — AI-аналитик продуктовой платформы «Сфера». 
Твой токен Яндекс Метрики даёт доступ к одному счётчику, в котором все продукты разделены по URL-префиксам.

При анализе ищи и явно называй:
• Падение трафика >20% за неделю → критично
• Bounce rate >70% → проблема с UX или релевантностью
• Рост трафика >30% → нужно убедиться что инфраструктура справляется  
• Доминирование одного источника >80% → риск зависимости
• Страницы с аномально высоким bounce rate в топе

Давай конкретные рекомендации: не «улучшить UX», а «на /hr/onboarding bounce 82% — проверить форму регистрации».

Пиши кратко и по делу. Используй эмодзи для наглядности.
Отвечай на русском языке."""


class AnalystAgent:
    def __init__(self, anthropic_key: str, metrika_client: MetrikaClient):
        self.claude = anthropic.Anthropic(api_key=anthropic_key)
        self.metrika = metrika_client

    def _run_tool(self, name: str, args: dict) -> Any:
        prefix = args.get("url_prefix") or None
        log.info("🔧 %s | prefix=%s", name, prefix)

        if name == "get_summary_metrics":
            return self.metrika.get_summary(prefix)
        elif name == "get_traffic_sources":
            return self.metrika.get_traffic_sources(prefix)
        elif name == "get_top_pages":
            return self.metrika.get_top_pages(prefix, args.get("limit", 10))
        elif name == "get_devices":
            return self.metrika.get_devices(prefix)
        else:
            return {"error": f"unknown tool: {name}"}

    def run(self, user_message: str, status_callback=None) -> str:
        """
        Запускает агентный цикл.
        status_callback(text) — вызывается при промежуточных статусах
        (можно использовать для отправки «печатает...» в Telegram).
        """
        messages = [{"role": "user", "content": user_message}]
        final_text_parts = []

        for iteration in range(50):  # защита от бесконечного цикла
            response = self.claude.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=messages,
            )

            messages.append({"role": "assistant", "content": response.content})

            # Собираем текстовые блоки
            for block in response.content:
                if block.type == "text" and block.text.strip():
                    final_text_parts.append(block.text.strip())
                    if status_callback and iteration > 0:
                        # Промежуточный текст — информируем пользователя
                        status_callback(f"💭 {block.text[:200]}")

            if response.stop_reason == "end_turn":
                break

            if response.stop_reason == "tool_use":
                tool_results = []
                tool_names = []
                for block in response.content:
                    if block.type == "tool_use":
                        result = self._run_tool(block.name, block.input)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": json.dumps(result, ensure_ascii=False),
                        })
                        tool_names.append(block.name)

                if status_callback:
                    status_callback(f"🔍 Запрашиваю: {', '.join(tool_names)}...")

                messages.append({"role": "user", "content": tool_results})

        return "\n\n".join(final_text_parts) or "Анализ завершён без результатов."
