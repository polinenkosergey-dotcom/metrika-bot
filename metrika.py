"""
Клиент Яндекс Метрики.
Все запросы к API сосредоточены здесь.
"""

import time
import logging
from datetime import datetime, timedelta

import requests

log = logging.getLogger("metrika")

BASE_URL = "https://api-metrika.yandex.net"


class MetrikaClient:
    def __init__(self, token: str, counter_id: int):
        self.token = token
        self.counter_id = counter_id
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"OAuth {token}"

    def _get(self, path: str, params: dict) -> dict:
        url = f"{BASE_URL}{path}"
        resp = self.session.get(url, params=params, timeout=20)
        resp.raise_for_status()
        time.sleep(0.25)  # уважаем лимиты API
        return resp.json()

    @staticmethod
    def week_range(offset: int = 0) -> tuple[str, str]:
        """
        offset=0 → прошлая завершённая неделя (пн–вс)
        offset=1 → позапрошлая неделя
        """
        today = datetime.now().date()
        last_monday = today - timedelta(days=today.weekday() + 7 * (1 + offset))
        return str(last_monday), str(last_monday + timedelta(days=6))

    # ── Публичные методы ──────────────────────────────────────────────────────

    def get_summary(self, url_prefix: str | None = None) -> dict:
        """Сводные метрики за текущую и прошлую неделю."""
        metrics = "ym:s:visits,ym:s:users,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds"
        result = {}

        for label, offset in [("this_week", 0), ("last_week", 1)]:
            d1, d2 = self.week_range(offset)
            params = {
                "ids": self.counter_id,
                "metrics": metrics,
                "date1": d1,
                "date2": d2,
                "accuracy": "full",
            }
            if url_prefix:
                params["filters"] = f"ym:s:URLPath=~'^{url_prefix}'"

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

        # WoW дельта
        tw = result.get("this_week", {})
        lw = result.get("last_week", {})
        deltas = {}
        for key in ["visits", "users"]:
            v_new, v_old = tw.get(key, 0), lw.get(key, 0)
            if v_old and v_old > 0:
                deltas[f"{key}_delta_pct"] = round((v_new - v_old) / v_old * 100, 1)
        result["wow_delta"] = deltas
        return result

    def get_traffic_sources(self, url_prefix: str | None = None) -> list[dict]:
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
        if url_prefix:
            params["filters"] = f"ym:s:URLPath=~'^{url_prefix}'"
        try:
            data = self._get("/stat/v1/data", params)
            return [
                {
                    "source": row["dimensions"][0].get("name", "unknown"),
                    "visits": round(row["metrics"][0]),
                }
                for row in data.get("data", [])
            ]
        except Exception as e:
            log.warning("get_traffic_sources error: %s", e)
            return []

    def get_top_pages(self, url_prefix: str | None = None, limit: int = 10) -> list[dict]:
        d1, d2 = self.week_range(0)
        params = {
            "ids": self.counter_id,
            "metrics": "ym:s:visits,ym:s:bounceRate",
            "dimensions": "ym:s:URLPath",
            "date1": d1,
            "date2": d2,
            "sort": "-ym:s:visits",
            "limit": limit,
        }
        if url_prefix:
            params["filters"] = f"ym:s:URLPath=~'^{url_prefix}'"
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

    def get_devices(self, url_prefix: str | None = None) -> list[dict]:
        d1, d2 = self.week_range(0)
        params = {
            "ids": self.counter_id,
            "metrics": "ym:s:visits",
            "dimensions": "ym:s:deviceCategory",
            "date1": d1,
            "date2": d2,
        }
        if url_prefix:
            params["filters"] = f"ym:s:URLPath=~'^{url_prefix}'"
        try:
            data = self._get("/stat/v1/data", params)
            return [
                {
                    "device": row["dimensions"][0].get("name", ""),
                    "visits": round(row["metrics"][0]),
                }
                for row in data.get("data", [])
            ]
        except Exception as e:
            log.warning("get_devices error: %s", e)
            return []
