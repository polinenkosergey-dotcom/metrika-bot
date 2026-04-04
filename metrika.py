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

# Субдомен → человекочитаемое название ВУЗа
UNI_REGISTRY: dict[str, str] = {
    "mai":   "МАИ",
    "urfu":  "УрФУ им. Ельцина (Екатеринбург)",
    "sut":   "СПбГУТ им. Бонч-Бруевича (Санкт-Петербург)",
    "nsu":   "НГУ (Новосибирск)",
    "istu":  "ИжГТУ им. Калашникова (Ижевск)",
    "kubsu": "КубГУ (Краснодар)",
    "kstu":  "КНИТУ (Казань)",
    "rsreu": "РГРТУ им. Уткина (Рязань)",
    "bmstu": "МГТУ им. Баумана",
    "unn":   "ННГУ им. Лобачевского (Нижний Новгород)",
    "fa":    "Финансовый университет (Москва)",
    "fta":   "Финансовый университет (Москва)",
    # Подключены, но трафика пока нет:
    # "innopolis" -> Университет Иннополис
    # hse-nn, hse-perm, ssau, ngieu — при появлении добавить сюда
}

# Субдомены которые сливаются в один ВУЗ: slug -> канонический slug
UNI_SLUG_MERGE: dict[str, str] = {
    "fta": "fa",  # хакатонный инстанс -> Финансовый университет
}

# Служебные субдомены — не ВУЗы
# gateway-* — прокси/технические инстансы, не учебные
UNI_SKIP_SLUGS: set[str] = {
    "lk", "start", "saas", "gamma",
    "gateway-codemetrics", "gateway-test", "gateway-beta",
    "gateway-mai", "gateway-sut", "gateway-istu", "gateway-nsu",
    "gateway-fta", "gateway-fa", "gateway-kstu", "gateway-urfu",
    "gateway-kubsu", "gateway-iu10", "gateway-nnov", "gateway-rsreu",
    "gateway-unn", "gateway-bmstu", "gateway-innopolis", "gateway-hkton",
    "gateway-ngieu", "gateway-iu10",
}

def _is_gateway_slug(slug: str) -> bool:
    """Любой субдомен начинающийся с gateway- считается служебным."""
    return slug.startswith("gateway-")


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
        Использует UNI_REGISTRY для человекочитаемых названий.
        Объединяет субдомены из UNI_SLUG_MERGE (fta -> fa).
        Пропускает служебные субдомены из UNI_SKIP_SLUGS.
        """
        assert self.counter_id
        rows = self._fetch_top_urls(days)

        # canonical_slug -> visits
        uni_visits: dict[str, int] = defaultdict(int)
        # canonical_slug -> список хостов (для построения фильтра)
        uni_hosts: dict[str, list[str]] = defaultdict(list)

        for row in rows:
            parsed = _safe_parse(row["url"])
            if not parsed:
                continue
            host, _ = parsed
            slug = self._uni_slug_from_host(host)
            if not slug or slug in UNI_SKIP_SLUGS or _is_gateway_slug(slug):
                continue
            # Применяем merge: fta -> fa
            canonical = UNI_SLUG_MERGE.get(slug, slug)
            uni_visits[canonical] += row["visits"]
            if host not in uni_hosts[canonical]:
                uni_hosts[canonical].append(host)

        unis = []
        for slug, visits in sorted(uni_visits.items(), key=lambda x: -x[1]):
            if visits < MIN_VISITS_THRESHOLD:
                continue
            name = UNI_REGISTRY.get(slug, slug.upper())
            hosts = uni_hosts[slug]
            unis.append({
                "name": name,
                "slug": slug,
                "hosts": hosts,       # список всех хостов (может быть несколько после merge)
                "host": hosts[0],     # основной хост (для обратной совместимости)
                "url_prefix": None,
                "visits": visits,
            })
        return unis

    def discover_products_for_uni(self, uni_slug: str, days: int = 60) -> list[dict]:
        """
        Layered: продукты конкретного ВУЗа.
        Учитывает merge: если uni_slug='fa', собирает данные и с fa, и с fta хостов.
        filter_hosts — список всех хостов ВУЗа (используется при построении фильтра).
        """
        assert self.counter_id
        rows = self._fetch_top_urls(days)

        # Собираем все slug-и которые относятся к этому ВУЗу
        target_slugs = {uni_slug}
        for src, dst in UNI_SLUG_MERGE.items():
            if dst == uni_slug:
                target_slugs.add(src)

        prefix_visits: dict[str, int] = defaultdict(int)
        found_hosts: list[str] = []

        for row in rows:
            parsed = _safe_parse(row["url"])
            if not parsed:
                continue
            host, parts = parsed
            raw_slug = self._uni_slug_from_host(host)
            if raw_slug not in target_slugs:
                continue
            if host not in found_hosts:
                found_hosts.append(host)
            prefix = "/" + parts[0] if parts else "/"
            prefix_visits[prefix] += row["visits"]

        uni_name = UNI_REGISTRY.get(uni_slug, uni_slug.upper())

        products = []
        for prefix, visits in sorted(prefix_visits.items(), key=lambda x: -x[1]):
            if visits < MIN_VISITS_THRESHOLD:
                continue
            products.append({
                "name": _prettify_prefix(prefix, ""),
                "url_prefix": prefix,
                "uni": uni_slug,
                "uni_name": uni_name,
                "filter_hosts": found_hosts,   # все хосты ВУЗа
                "filter_host": found_hosts[0] if found_hosts else "",
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
    def _make_filter(
        url_prefix: str | None,
        filter_host: str | None,
        filter_hosts: list[str] | None = None,
    ) -> str | None:
        """
        Строит строку фильтра для Метрики.

        filter_hosts — список хостов (для ВУЗов с несколькими инстансами, напр. fa+fta).
        Если передан filter_hosts, строит OR-фильтр по всем хостам.

        Примеры:
          filter_host='mai...', url_prefix='/tasks'
            → "ym:s:startURL=@'https://mai.../tasks'"
          filter_hosts=['fa...', 'fta...'], url_prefix='/tasks'
            → "ym:s:startURL=@'https://fa.../tasks' OR ym:s:startURL=@'https://fta.../tasks'"
          filter_host='mai...', url_prefix=None
            → "ym:s:startURL=@'https://mai.../'  "
          filter_host=None, url_prefix='/tasks'
            → "ym:s:startURL=@'/tasks'"
        """
        hosts = filter_hosts or ([filter_host] if filter_host else [])

        if hosts and url_prefix:
            # Фильтруем по хосту И пути — без https:// чтобы ловить все страницы домена
            parts = [f"ym:s:startURL=@'{h}{url_prefix}'" for h in hosts]
            return " OR ".join(parts)
        if hosts:
            # Только по хосту — все страницы домена
            parts = [f"ym:s:startURL=@'{h}'" for h in hosts]
            return " OR ".join(parts)
        if url_prefix:
            return f"ym:s:startURL=@'{url_prefix}'"
        return None

    # ── Метрики ───────────────────────────────────────────────────────────────

    def get_summary(self, url_prefix: str | None = None, filter_host: str | None = None, filter_hosts: list[str] | None = None) -> dict:
        assert self.counter_id
        metrics = "ym:s:visits,ym:s:users,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds"
        result = {}

        flt = self._make_filter(url_prefix, filter_host, filter_hosts)

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

    def get_traffic_sources(self, url_prefix: str | None = None, filter_host: str | None = None, filter_hosts: list[str] | None = None) -> list[dict]:
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
        flt = self._make_filter(url_prefix, filter_host, filter_hosts)
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

    def get_top_pages(self, url_prefix: str | None = None, filter_host: str | None = None, filter_hosts: list[str] | None = None, limit: int = 10) -> list[dict]:
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
        flt = self._make_filter(url_prefix, filter_host, filter_hosts)
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

    def get_devices(self, url_prefix: str | None = None, filter_host: str | None = None, filter_hosts: list[str] | None = None) -> list[dict]:
        assert self.counter_id
        d1, d2 = self.week_range(0)
        params = {
            "ids": self.counter_id,
            "metrics": "ym:s:visits",
            "dimensions": "ym:s:deviceCategory",
            "date1": d1,
            "date2": d2,
        }
        flt = self._make_filter(url_prefix, filter_host, filter_hosts)
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

    # ── Исторические данные для отчётов ──────────────────────────────────────

    def get_users_by_month(
        self,
        months: int = 3,
        filter_hosts: list[str] | None = None,
        filter_host: str | None = None,
    ) -> list[dict]:
        """
        Уникальные пользователи помесячно за последние N месяцев.
        Возвращает список {"month": "2026-02", "users": 1234}.
        """
        assert self.counter_id
        from datetime import date
        today = date.today()
        result = []
        for i in range(months - 1, -1, -1):
            # Первый и последний день месяца
            if today.month - i <= 0:
                year = today.year - 1
                month = today.month - i + 12
            else:
                year = today.year
                month = today.month - i
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            d1 = f"{year:04d}-{month:02d}-01"
            d2 = f"{year:04d}-{month:02d}-{last_day:02d}"
            label = f"{year:04d}-{month:02d}"

            flt = self._make_filter(None, filter_host, filter_hosts)
            params = {
                "ids": self.counter_id,
                "metrics": "ym:s:users",
                "date1": d1,
                "date2": d2,
                "accuracy": "full",
            }
            if flt:
                params["filters"] = flt
            try:
                data = self._get("/stat/v1/data", params)
                users = round(data.get("totals", [0])[0])
            except Exception as e:
                log.warning("get_users_by_month %s: %s", label, e)
                users = 0
            result.append({"month": label, "users": users})
        return result

    def get_cumulative_users(
        self,
        filter_hosts: list[str] | None = None,
        filter_host: str | None = None,
        since: str = "2024-01-01",
    ) -> int:
        """
        Уникальные пользователи нарастающим итогом с даты since по сегодня.
        Метрика считает уников за период — используем максимально широкий диапазон.
        """
        assert self.counter_id
        from datetime import date
        today = str(date.today())
        flt = self._make_filter(None, filter_host, filter_hosts)
        params = {
            "ids": self.counter_id,
            "metrics": "ym:s:users",
            "date1": since,
            "date2": today,
            "accuracy": "full",
        }
        if flt:
            params["filters"] = flt
        try:
            data = self._get("/stat/v1/data", params)
            return round(data.get("totals", [0])[0])
        except Exception as e:
            log.warning("get_cumulative_users: %s", e)
            return 0

    def get_active_users(
        self,
        d1: str,
        d2: str,
        filter_hosts: list[str] | None = None,
        filter_host: str | None = None,
        min_visits: int = 3,
    ) -> int:
        """
        Активные пользователи = те кто совершил min_visits+ визитов за период.
        Использует фильтр ym:s:visitNumber>={min_visits}.
        """
        assert self.counter_id
        host_filter = self._make_filter(None, filter_host, filter_hosts)
        visit_filter = f"ym:s:visitNumber>={min_visits}"

        if host_filter:
            combined = f"({host_filter}) AND {visit_filter}"
        else:
            combined = visit_filter

        params = {
            "ids": self.counter_id,
            "metrics": "ym:s:users",
            "date1": d1,
            "date2": d2,
            "accuracy": "full",
            "filters": combined,
        }
        try:
            data = self._get("/stat/v1/data", params)
            return round(data.get("totals", [0])[0])
        except Exception as e:
            log.warning("get_active_users: %s", e)
            return 0

    def get_new_users(
        self,
        d1: str,
        d2: str,
        filter_hosts: list[str] | None = None,
        filter_host: str | None = None,
    ) -> int:
        """
        Новые пользователи = те кто пришёл впервые (ym:s:newUsers).
        """
        assert self.counter_id
        flt = self._make_filter(None, filter_host, filter_hosts)
        params = {
            "ids": self.counter_id,
            "metrics": "ym:s:newUsers",
            "date1": d1,
            "date2": d2,
            "accuracy": "full",
        }
        if flt:
            params["filters"] = flt
        try:
            data = self._get("/stat/v1/data", params)
            return round(data.get("totals", [0])[0])
        except Exception as e:
            log.warning("get_new_users: %s", e)
            return 0

    def get_users_by_product_monthly(
        self,
        filter_hosts: list[str],
        months: int = 3,
    ) -> dict[str, list[dict]]:
        """
        Уникальные пользователи по каждому продукту (первый сегмент пути) помесячно.
        Возвращает {product_prefix: [{"month": ..., "users": ...}]}.
        """
        assert self.counter_id
        from datetime import date
        import calendar
        today = date.today()
        result: dict[str, list[dict]] = {}

        for i in range(months - 1, -1, -1):
            if today.month - i <= 0:
                year = today.year - 1
                month = today.month - i + 12
            else:
                year = today.year
                month = today.month - i
            last_day = calendar.monthrange(year, month)[1]
            d1 = f"{year:04d}-{month:02d}-01"
            d2 = f"{year:04d}-{month:02d}-{last_day:02d}"
            label = f"{year:04d}-{month:02d}"

            flt = self._make_filter(None, None, filter_hosts)
            params = {
                "ids": self.counter_id,
                "metrics": "ym:s:users",
                "dimensions": "ym:s:startURL",
                "date1": d1,
                "date2": d2,
                "sort": "-ym:s:users",
                "limit": 100,
                "accuracy": "full",
            }
            if flt:
                params["filters"] = flt
            try:
                data = self._get("/stat/v1/data", params)
            except Exception as e:
                log.warning("get_users_by_product_monthly %s: %s", label, e)
                continue

            # Группируем по первому сегменту пути
            from collections import defaultdict
            prefix_users: dict[str, int] = defaultdict(int)
            for row in data.get("data", []):
                url = row["dimensions"][0].get("name", "")
                parsed = _safe_parse(url)
                if not parsed:
                    continue
                _, parts = parsed
                prefix = "/" + parts[0] if parts else "/"
                prefix_users[prefix] += round(row["metrics"][0])

            for prefix, users in prefix_users.items():
                if prefix not in result:
                    result[prefix] = []
                result[prefix].append({"month": label, "users": users})

        return result


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
