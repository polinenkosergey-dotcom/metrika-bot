# 📊 AI-аналитик платформы Сфера (Telegram-бот)

Агент подключён к Яндекс Метрике, анализирует продукты по URL-сегментам
внутри одного счётчика и отвечает в Telegram.

---

## Быстрый старт

### 1. Установить зависимости

```bash
pip install -r requirements.txt
```

### 2. Настроить переменные

```bash
cp .env.example .env
# Открыть .env и заполнить все значения
```

| Переменная | Где взять |
|---|---|
| `METRIKA_TOKEN` | [Яндекс OAuth](https://oauth.yandex.ru/authorize?response_type=token&client_id=1d0b9dd4d652455a9eb710d450ff456a) |
| `METRIKA_COUNTER_ID` | В URL счётчика: `metrika.yandex.ru/stat?id=`**XXXXXXXX** |
| `ANTHROPIC_API_KEY` | [console.anthropic.com](https://console.anthropic.com/settings/keys) |
| `TELEGRAM_TOKEN` | @BotFather → `/newbot` |
| `ALLOWED_USER_IDS` | Опционально. Свой ID: @userinfobot |

### 3. Настроить продукты

Отредактировать `products.json` — добавить все продукты платформы:

```json
[
  { "name": "Документооборот", "url_prefix": "/dms",        "owner": "@ivanov" },
  { "name": "Кадровый портал", "url_prefix": "/hr",         "owner": "@petrova" },
  { "name": "Финансовый модуль","url_prefix": "/finance",   "owner": "@sidorov" },
  { "name": "Закупки",         "url_prefix": "/procurement","owner": "@kozlov"  }
]
```

`url_prefix` — начало URL-пути страниц продукта (проверить в Метрике: Источники → Страницы).

### 4. Запустить

```bash
python bot.py
```

---

## Команды бота

| Команда | Что происходит |
|---|---|
| `/product` | Список продуктов кнопками (с пагинацией для 30+) → выбрать → получить отчёт |
| `/ask <вопрос>` | Агент запрашивает нужные данные и отвечает с цифрами |
| `/status` | Проверить соединение с Метрикой, показать число визитов |
| `/help` | Примеры вопросов |

**Свободный текст** (без команды) работает так же, как `/ask`.

### Примеры вопросов

```
Почему упал трафик на /hr на прошлой неделе?
Сравни bounce rate для /finance и /dms
Какой продукт показал лучший рост за неделю?
Откуда идёт трафик на /procurement?
Есть ли проблемы с мобильным трафиком?
Какие страницы /dms самые проблемные?
```

---

## Структура проекта

```
metrika_agent/
├── bot.py           # Telegram-бот (точка входа)
├── analyst.py       # AI-агент: инструменты + цикл Claude API
├── metrika.py       # Клиент Яндекс Метрики API
├── products.json    # Реестр продуктов ← редактировать
├── requirements.txt
├── .env.example
└── reports/         # Автосохранение отчётов (создаётся автоматически)
```

---

## Как работает агент

```
Пользователь → Telegram → bot.py
                               ↓
                          analyst.py (агентный цикл)
                          Claude решает, что запросить
                               ↓
                          metrika.py → Яндекс Метрика API
                               ↓
                          Claude анализирует данные
                               ↓
                          Ответ → Telegram
```

Агент сам определяет, какие инструменты вызвать и сколько раз.
Для вопроса «сравни /finance и /dms» — запросит метрики обоих продуктов.
Для «топ страниц» — вызовет get_top_pages.

---

## Запуск как сервис (systemd)

```ini
# /etc/systemd/system/metrika-bot.service
[Unit]
Description=Metrika AI Bot
After=network.target

[Service]
WorkingDirectory=/opt/metrika_agent
ExecStart=/usr/bin/python3 bot.py
Restart=always
EnvironmentFile=/opt/metrika_agent/.env

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable metrika-bot
sudo systemctl start metrika-bot
sudo journalctl -u metrika-bot -f   # логи
```
