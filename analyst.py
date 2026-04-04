"""
AI-агент аналитики.
Инструменты поддерживают filter_host для layered-счётчиков
(ВУЗ = субдомен, продукт = путь).
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
            "Получить сводные метрики за текущую и прошлую неделю: "
            "визиты, уники, bounce rate, глубина, время на сайте, WoW-дельта."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string"},
                "url_prefix": {
                    "type": "string",
                    "description": "Путь продукта, например /tasks. Пустая строка — весь счётчик/ВУЗ.",
                },
                "filter_host": {
                    "type": "string",
                    "description": (
                        "Основной хост для фильтрации (layered-счётчики). "
                        "Например: mai.saas.sferaplatform.ru."
                    ),
                },
                "filter_hosts": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Список всех хостов ВУЗа (если несколько, напр. fa и fta). "
                        "Используй вместо filter_host когда у ВУЗа несколько инстансов."
                    ),
                },
            },
            "required": ["product_name", "url_prefix"],
        },
    },
    {
        "name": "get_traffic_sources",
        "description": "Разбивка трафика по источникам (organic, direct, referral и др.).",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string"},
                "url_prefix": {"type": "string"},
                "filter_host": {"type": "string"},
            },
            "required": ["product_name", "url_prefix"],
        },
    },
    {
        "name": "get_top_pages",
        "description": "Топ страниц по визитам с bounce rate.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string"},
                "url_prefix": {"type": "string"},
                "filter_host": {"type": "string"},
                "limit": {"type": "integer", "default": 10},
            },
            "required": ["product_name", "url_prefix"],
        },
    },
    {
        "name": "get_devices",
        "description": "Разбивка по типам устройств.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string"},
                "url_prefix": {"type": "string"},
                "filter_host": {"type": "string"},
            },
            "required": ["product_name", "url_prefix"],
        },
    },
]

SYSTEM_PROMPT = """Ты — AI-аналитик продуктовой платформы «Сфера».

Важно: при анализе продуктов с layered-счётчиком (ВУЗы) всегда передавай
filter_host (хост ВУЗа) вместе с url_prefix (путь продукта).
Это обеспечивает точную фильтрацию по конкретному ВУЗу.

При анализе ищи:
• Падение трафика >20% за неделю → критично
• Bounce rate >70% → проблема с UX
• Рост >30% → проверить инфраструктуру
• Один источник трафика >80% → риск зависимости

Давай конкретные рекомендации с цифрами и URL.
Пиши кратко. Эмодзи для структуры. Отвечай на русском."""


class AnalystAgent:
    def __init__(self, anthropic_key: str, metrika_client: MetrikaClient):
        self.claude = anthropic.Anthropic(api_key=anthropic_key)
        self.metrika = metrika_client

    def set_counter(self, counter_id: int):
        self.metrika = self.metrika.with_counter(counter_id)

    def _run_tool(self, name: str, args: dict) -> Any:
        prefix = args.get("url_prefix") or None
        host = args.get("filter_host") or None
        hosts_raw = args.get("filter_hosts")

        # Нормализуем: Claude иногда передаёт строку вместо списка
        if isinstance(hosts_raw, str) and hosts_raw:
            hosts = [hosts_raw]
        elif isinstance(hosts_raw, list) and hosts_raw:
            hosts = hosts_raw
        else:
            hosts = None

        # Если hosts не задан, но есть одиночный host — используем его как список
        effective_hosts = hosts or ([host] if host else None)

        log.info("🔧 %s | prefix=%s | hosts=%s", name, prefix, effective_hosts)

        if name == "get_summary_metrics":
            return self.metrika.get_summary(prefix, host, effective_hosts)
        elif name == "get_traffic_sources":
            return self.metrika.get_traffic_sources(prefix, host, effective_hosts)
        elif name == "get_top_pages":
            return self.metrika.get_top_pages(prefix, host, effective_hosts, args.get("limit", 10))
        elif name == "get_devices":
            return self.metrika.get_devices(prefix, host, effective_hosts)
        return {"error": f"unknown tool: {name}"}

    def run(self, user_message: str, status_callback=None) -> str:
        messages = [{"role": "user", "content": user_message}]
        final_text_parts = []

        for _ in range(50):
            response = self.claude.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=messages,
            )
            messages.append({"role": "assistant", "content": response.content})

            for block in response.content:
                if block.type == "text" and block.text.strip():
                    final_text_parts.append(block.text.strip())

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
                    status_callback(f"🔍 {', '.join(tool_names)}...")

                messages.append({"role": "user", "content": tool_results})

        return "\n\n".join(final_text_parts) or "Анализ завершён."
