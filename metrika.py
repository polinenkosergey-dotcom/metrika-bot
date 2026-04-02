"""
Клиент Яндекс Метрики.

Два режима счётчиков:
  flat    — продукты на первом уровне пути:  sfera-t1.ru/tasks/...
  layered — ВУЗ = субдомен, продукт = путь: mai.saas.sferaplatform.ru/tasks/...

Режим задаётся через LAYERED_COUNTERS.
"""

import time
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlparse

import requests

log = logging.getLogger("metrika")

BASE_URL = "https://api-metrika.yandex.net"
MIN_VISITS_THRESHOLD = 50

# Счётчики где ВУЗ = субдомен, продукт = первый сегмент пути
LAYERED_COUNTERS: set[int] = {102372602}

# Базовые домены для layered-счётчика (субдомен = ВУЗ)
LAYERED_BASE_DOMAINS = [
    "saas.sferaplatform.ru",
    "sferaplatform.ru",
]


class MetrikaClient:
    def __init__(self, token: str, counter_id: int | None = None):
        self.token = token
        self.counter_id = counter_id
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"OAuth {token}"

    def with_counter(self, counter_id: int) -> "MetrikaClient":
        return MetrikaClient(self.token, counter_id)

    def is_layered(self) -> bool:
        return self.counter_id in LAYERED_COUNTERS

    def _get(self, path: str, params: dict) -> dict:
        url = f"{BASE_URL}{path}"
        resp = self.session.get(url, params=params, timeout=20)
        resp.raise_for_status()
        time.sleep(0.25)
        return resp.json()

    @staticmethod
    def week_range(offset: int = 0) -> tuple[str, str]:
        today = datetime.now().date()
        last_monday = today - timedelta(days=today.weekday() + 7 * (1 + offset))
        return str(last_monday), str(last_monday + timedelta(days=6))

    # ── Список счётчиков ──────────────────────────────────────────────────────

    def get_counters(self) -> list[dict]:
        try:
            data = self._get("/management/v1/counters", {"per_page": 100})
            return [
                {
                    "id": c["id"],
                    "name": c.get("name", f"Счётчик {c['id']}"),
                    "site": c.get("site", ""),
                    "layered": c["id"] in LAYERED_COUNTERS,
                }
                for c in data.get("counters", [])
            ]
        except Exception as e:
            log.error("Ошибка get_counters: %s", e)
            return []

    # ── Получение сырых URL ───────────────────────────────────────────────────

    def _fetch_top_urls(self, days: int = 60, limit: int = 500) -> list[dict]:
        date_to = datetime.now().date()
        date_from = date_to - timedelta(days=days)
        try:
            data = self._get(
                "/stat/v1/data",
                {
                    "ids": self.counter_id,
                    "metrics": "ym:s:visits",
                    "dimensions": "ym:s:startURL",
                    "date1": str(date_from),
                    "date2": str(date_to),
                    "sort": "-ym:s:visits",
                    "limit": limit,
                    "accuracy": "full",
                },
            )
            return [
                {
                    "url": row["dimensions"][0].get("name", ""),
                    "visits": round(row["metrics"][0]),
                }
                for row in data.get("data", [])
            ]
        except Exception as e:
            log.error("Ошибка _fetch_top_urls: %s", e)
            return []

    # ── Flat-режим ────────────────────────────────────────────────────────────

    def discover_products(self, days: int = 60) -> list[dict]:
        """
        Flat: группирует по первому сегменту пути.
        sfera-t1.ru/tasks/area/... → продукт /tasks
        """
        assert self.counter_id
        rows = self._fetch_top_urls(days)

        prefix_visits: dict[str, int] = defaultdict(int)
        prefix_hosts: dict[str, str] = {}

        for row in rows:
            parsed = _safe_parse(row["url"])
            if not parsed:
                continue
            host, parts = parsed
            prefix = "/" + parts[0] if parts else "/"
            key = f"{host}{prefix}"
            prefix_visits[key] += row["visits"]
            prefix_hosts[key] = host

        products = []
        for key, visits in sorted(prefix_visits.items(), key=lambda x: -x[1]):
            if visits < MIN_VISITS_THRESHOLD:
                continue
            host = prefix_hosts[key]
            prefix = key[len(host):]
            products.append({
                "name": _prettify_prefix(prefix, host),
                "url_prefix": prefix,
                "host": host,
                "filter_host": None,  # flat: фильтруем только по пути
                "visits": visits,
            })
        return products

    # ── Layered-режим: ВУЗ = субдомен ────────────────────────────────────────
    # URL: https://mai.saas.sferaplatform.ru/tasks/area/...
    #               ^^^                      ^^^^^^
    #               ВУЗ (субдомен)           Продукт (первый сегмент пути)

    def _uni_slug_from_host(self, host: str) -> str | None:
        """
        mai.saas.sferaplatform.ru → 'mai'
        saas.sferaplatform.ru     → None (корневой домен)
        """
        for base in LAYERED_BASE_DOMAINS:
            if host == base:
                return None
            if host.endswith("." + base):
                subdomain = host[: -(len(base) + 1)]
                return subdomain.split(".")[0]  # берём только первый сегмент
        # Неизвестный домен — первый сегмент субдомена
        parts = host.split(".")
        return parts[0] if len(parts) > 2 else None

    def discover_unis(self, days: int = 60) -> list[dict]:
        """
        Layered: возвращает список ВУЗов по субдоменам.
        mai.saas.sferaplatform.ru/... → ВУЗ 'MAI'
        """
        assert self.counter_id
        rows = self._fetch_top_urls(days)

        uni_visits: dict[str, int] = defaultdict(int)
        uni_hosts: dict[str, str] = {}

        for row in rows:
            parsed = _safe_parse(row["url"])
            if not parsed:
                continue
            host, _ = parsed
            slug = self._uni_slug_from_host(host)
            if not slug:
                continue
            uni_visits[slug] += row["visits"]
            uni_hosts[slug] = host  # запоминаем полный хост

        unis = []
        for slug, visits in sorted(uni_visits.items(), key=lambda x: -x[1]):
            if visits < MIN_VISITS_THRESHOLD:
                continue
            unis.append({
                "name": slug.upper(),           # 'mai' → 'MAI'
                "slug": slug,                   # 'mai'
                "host": uni_hosts[slug],        # 'mai.saas.sferaplatform.ru'
                "url_prefix": None,             # фильтр по хосту, не по пути
                "visits": visits,
            })
        return unis

    def discover_products_for_uni(self, uni_slug: str, days: int = 60) -> list[dict]:
        """
        Layered: продукты конкретного ВУЗа = первый сегмент пути на его субдомене.
        mai.saas.sferaplatform.ru/tasks/... → продукт 'Задачи', prefix='/tasks'
        Фильтр в Метрике: startURL содержит 'https://mai.saas.sferaplatform.ru/tasks'
        """
        assert self.counter_id
        rows = self._fetch_top_urls(days)

        prefix_visits: dict[str, int] = defaultdict(int)
        uni_host = ""

        for row in rows:
            parsed = _safe_parse(row["url"])
            if not parsed:
                continue
            host, parts = parsed
            if self._uni_slug_from_host(host) != uni_slug:
                continue
            uni_host = host
            prefix = "/" + parts[0] if parts else "/"
            prefix_visits[prefix] += row["visits"]

        products = []
        for prefix, visits in sorted(prefix_visits.items(), key=lambda x: -x[1]):
            if visits < MIN_VISITS_THRESHOLD:
                continue
            products.append({
                "name": _prettify_prefix(prefix, ""),
                "url_prefix": prefix,       # '/tasks'
                "uni": uni_slug,            # 'mai'
                "filter_host": uni_host,    # 'mai.saas.sferaplatform.ru' — для фильтра
                "visits": visits,
            })
        return products

    def discover_products_all_unis(self, days: int = 60) -> dict[str, list[dict]]:
        unis = self.discover_unis(days)
        result = {}
        for uni in unis:
            result[uni["slug"]] = self.discover_products_for_uni(uni["slug"], days)
        return result

    # ── Построение фильтра ────────────────────────────────────────────────────

    @staticmethod
    def _make_filter(url_prefix: str | None, filter_host: str | None) -> str | None:
        """
        Строит строку фильтра для Метрики.
        filter_host='mai.saas.sferaplatform.ru', url_prefix='/tasks'
          → "ym:s:startURL=@'https://mai.saas.sferaplatform.ru/tasks'"
        filter_host='mai.saas.sferaplatform.ru', url_prefix=None
          → "ym:s:startURL=@'https://mai.saas.sferaplatform.ru/'"
        filter_host=None, url_prefix='/tasks'
          → "ym:s:startURL=@'/tasks'"
        """
        if filter_host and url_prefix:
            return f"ym:s:startURL=@'https://{filter_host}{url_prefix}'"
        if filter_host:
            return f"ym:s:startURL=@'https://{filter_host}/'"
        if url_prefix:
            return f"ym:s:startURL=@'{url_prefix}'"
        return None

    # ── Метрики ───────────────────────────────────────────────────────────────

    def get_summary(self, url_prefix: str | None = None, filter_host: str | None = None) -> dict:
        assert self.counter_id
        metrics = "ym:s:visits,ym:s:users,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds"
        result = {}

        flt = self._make_filter(url_prefix, filter_host)

        for label, offset in [("this_week", 0), ("last_week", 1)]:
            d1, d2 = self.week_range(offset)
            params = {
                "ids": self.counter_id,
                "metrics": metrics,
                "date1": d1,
                "date2": d2,
                "accuracy": "full",
            }
            if flt:
                params["filters"] = flt
            try:
                data = self._get("/stat/v1/data", params)
                t = data.get("totals", [])
                result[label] = {
                    "date_from": d1,
                    "date_to": d2,
                    "visits": round(t[0]) if t else 0,
                    "users": round(t[1]) if len(t) > 1 else 0,
                    "bounce_rate": round(t[2], 1) if len(t) > 2 else None,
                    "page_depth": round(t[3], 2) if len(t) > 3 else None,
                    "avg_duration_sec": round(t[4]) if len(t) > 4 else None,
                }
            except Exception as e:
                log.warning("get_summary error (%s): %s", label, e)
                result[label] = {"error": str(e)}

        tw = result.get("this_week", {})
        lw = result.get("last_week", {})
        deltas = {}
        for key in ["visits", "users"]:
            v_new, v_old = tw.get(key, 0), lw.get(key, 0)
            if v_old and v_old > 0:
                deltas[f"{key}_delta_pct"] = round((v_new - v_old) / v_old * 100, 1)
        result["wow_delta"] = deltas
        return result

    def get_traffic_sources(self, url_prefix: str | None = None, filter_host: str | None = None) -> list[dict]:
        assert self.counter_id
        d1, d2 = self.week_range(0)
        params = {
            "ids": self.counter_id,
            "metrics": "ym:s:visits",
            "dimensions": "ym:s:trafficSource",
            "date1": d1,
            "date2": d2,
            "sort": "-ym:s:visits",
            "limit": 10,
        }
        flt = self._make_filter(url_prefix, filter_host)
        if flt:
            params["filters"] = flt
        try:
            data = self._get("/stat/v1/data", params)
            return [
                {"source": row["dimensions"][0].get("name", ""), "visits": round(row["metrics"][0])}
                for row in data.get("data", [])
            ]
        except Exception as e:
            log.warning("get_traffic_sources error: %s", e)
            return []

    def get_top_pages(self, url_prefix: str | None = None, filter_host: str | None = None, limit: int = 10) -> list[dict]:
        assert self.counter_id
        d1, d2 = self.week_range(0)
        params = {
            "ids": self.counter_id,
            "metrics": "ym:s:visits,ym:s:bounceRate",
            "dimensions": "ym:s:startURL",
            "date1": d1,
            "date2": d2,
            "sort": "-ym:s:visits",
            "limit": limit,
        }
        flt = self._make_filter(url_prefix, filter_host)
        if flt:
            params["filters"] = flt
        try:
            data = self._get("/stat/v1/data", params)
            return [
                {
                    "url": row["dimensions"][0].get("name", ""),
                    "visits": round(row["metrics"][0]),
                    "bounce_rate": round(row["metrics"][1], 1),
                }
                for row in data.get("data", [])
            ]
        except Exception as e:
            log.warning("get_top_pages error: %s", e)
            return []

    def get_devices(self, url_prefix: str | None = None, filter_host: str | None = None) -> list[dict]:
        assert self.counter_id
        d1, d2 = self.week_range(0)
        params = {
            "ids": self.counter_id,
            "metrics": "ym:s:visits",
            "dimensions": "ym:s:deviceCategory",
            "date1": d1,
            "date2": d2,
        }
        flt = self._make_filter(url_prefix, filter_host)
        if flt:
            params["filters"] = flt
        try:
            data = self._get("/stat/v1/data", params)
            return [
                {"device": row["dimensions"][0].get("name", ""), "visits": round(row["metrics"][0])}
                for row in data.get("data", [])
            ]
        except Exception as e:
            log.warning("get_devices error: %s", e)
            return []


# ── Утилиты ───────────────────────────────────────────────────────────────────

def _safe_parse(url_str: str) -> tuple[str, list[str]] | None:
    try:
        parsed = urlparse(url_str)
        host = parsed.netloc
        parts = [p for p in parsed.path.rstrip("/").split("/") if p]
        return host, parts
    except Exception:
        return None


def _prettify_prefix(prefix: str, host: str) -> str:
    known = {
        "/tasks": "Задачи",
        "/sd": "Service Desk",
        "/knowledge": "База знаний",
        "/orchestration": "Оркестрация",
        "/dove": "Dove ITSM",
        "/documents": "Документы",
        "/sourcecode": "Исходный код",
        "/testing": "Тестирование",
        "/configurations": "Конфигурации (CMDB)",
        "/teams": "Команды",
        "/releases": "Релизы",
        "/accidents": "Аварии",
        "/portal": "Портал",
        "/approvals": "Согласования",
        "/profile": "Профиль",
        "/": "Главная",
    }
    seg = "/" + prefix.lstrip("/").split("/")[0] if prefix.lstrip("/") else "/"
    name = known.get(seg) or known.get(prefix) or prefix.lstrip("/").replace("-", " ").capitalize()

    if host:
        if "preprod" in host:
            name += " (preprod)"
        elif "imp" in host:
            name += " (imp)"

    return name
