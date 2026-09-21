"""Генератор синтетических данных e-commerce.

Зачем свой генератор, а не готовый датасет: Free Edition ограничивает исходящий
интернет, а нам нужны управляемые «грязные» случаи — дубли, NULL-и,
отрицательные количества, опоздавшие события, изменения атрибутов клиента
(для SCD2). Всё это здесь параметризовано.

Функции ничего не знают про Spark: возвращают списки словарей.
Запись в Volume — в `writer.py` / ноутбуке Part 0.
"""

from __future__ import annotations

import hashlib
import random
from datetime import date, datetime, timedelta

CATEGORIES: dict[str, list[str]] = {
    "Electronics": ["Phones", "Laptops", "Audio", "Wearables"],
    "Home": ["Kitchen", "Furniture", "Lighting", "Textile"],
    "Sports": ["Running", "Cycling", "Fitness", "Outdoor"],
    "Beauty": ["Skincare", "Haircare", "Fragrance"],
    "Books": ["Fiction", "Tech", "Children"],
}
BRANDS = ["Aurora", "Nordkap", "Vela", "Kite", "Orbit", "Muren", "Sable", "Lumen"]
COUNTRIES = ["DE", "FR", "NL", "PL", "ES", "IT", "SE", "CZ"]
CITIES = {
    "DE": ["Berlin", "Munich", "Hamburg"],
    "FR": ["Paris", "Lyon", "Nice"],
    "NL": ["Amsterdam", "Rotterdam"],
    "PL": ["Warsaw", "Krakow"],
    "ES": ["Madrid", "Barcelona"],
    "IT": ["Rome", "Milan"],
    "SE": ["Stockholm", "Malmo"],
    "CZ": ["Prague", "Brno"],
}
SEGMENTS = ["consumer", "pro", "vip"]
CHANNELS = ["web", "ios", "android", "partner"]
STATUSES = ["created", "paid", "shipped", "delivered", "cancelled", "returned"]
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "checkout"]
DEVICES = ["desktop", "mobile", "tablet"]

FIRST_NAMES = ["Anna", "Piotr", "Marek", "Lena", "Sofia", "Jonas", "Elin", "Tomas", "Ivan", "Nora"]
LAST_NAMES = ["Kowal", "Nowak", "Berg", "Fischer", "Rossi", "Moreau", "Silva", "Novak", "Lind"]


def _rng(seed: int) -> random.Random:
    return random.Random(seed)


def _stable_id(prefix: str, *parts: object) -> str:
    raw = "|".join(str(p) for p in parts)
    return f"{prefix}_{hashlib.md5(raw.encode()).hexdigest()[:12]}"


# --------------------------------------------------------------------------
# Справочники
# --------------------------------------------------------------------------


def generate_products(n: int = 300, seed: int = 42, as_of: date | None = None) -> list[dict]:
    """Каталог товаров. `updated_at` нужен для SCD/merge-сценариев."""
    rnd = _rng(seed)
    as_of = as_of or date(2026, 1, 1)
    products = []
    for i in range(n):
        category = rnd.choice(list(CATEGORIES))
        products.append(
            {
                "product_id": f"P{i + 1000:05d}",
                "product_name": f"{rnd.choice(BRANDS)} {rnd.choice(CATEGORIES[category])} {i % 97}",
                "category": category,
                "subcategory": rnd.choice(CATEGORIES[category]),
                "brand": rnd.choice(BRANDS),
                "list_price": round(rnd.uniform(5, 1800), 2),
                "is_active": rnd.random() > 0.05,
                "updated_at": f"{as_of.isoformat()}T00:00:00",
            }
        )
    return products


def generate_customers(
    n: int = 2000,
    seed: int = 7,
    as_of: date | None = None,
    dirty_ratio: float = 0.02,
) -> list[dict]:
    """Клиенты на дату `as_of`."""
    rnd = _rng(seed)
    as_of = as_of or date(2026, 1, 1)
    customers = []
    for i in range(n):
        country = rnd.choice(COUNTRIES)
        first, last = rnd.choice(FIRST_NAMES), rnd.choice(LAST_NAMES)
        email = f"{first.lower()}.{last.lower()}{i}@example.com"
        row = {
            "customer_id": f"C{i + 10000:06d}",
            "full_name": f"{first} {last}",
            "email": email,
            "phone": f"+49{rnd.randint(1000000, 9999999)}",
            "country": country,
            "city": rnd.choice(CITIES[country]),
            "segment": rnd.choices(SEGMENTS, weights=[0.7, 0.22, 0.08])[0],
            "signup_date": (as_of - timedelta(days=rnd.randint(1, 900))).isoformat(),
            "updated_at": f"{as_of.isoformat()}T00:00:00",
        }
        if rnd.random() < dirty_ratio:  # битый email — поймаем в DQ
            row["email"] = email.replace("@", "#")
        customers.append(row)
    return customers


def evolve_customers(
    customers: list[dict],
    as_of: date,
    change_ratio: float = 0.05,
    seed: int = 11,
) -> list[dict]:
    """Снимок справочника на следующую дату: часть клиентов сменила атрибуты.

    Возвращает ТОЛЬКО изменившиеся записи — именно так приходит CDC-фид,
    и именно из него в Part 1/2 строится SCD Type 2.
    """
    rnd = _rng(seed + as_of.toordinal())
    changed = []
    for row in customers:
        if rnd.random() >= change_ratio:
            continue
        new = dict(row)
        choice = rnd.random()
        if choice < 0.4:
            new["segment"] = rnd.choice(SEGMENTS)
        elif choice < 0.7:
            country = rnd.choice(COUNTRIES)
            new["country"] = country
            new["city"] = rnd.choice(CITIES[country])
        else:
            new["city"] = rnd.choice(CITIES[new["country"]])
        new["updated_at"] = f"{as_of.isoformat()}T0{rnd.randint(1, 9)}:00:00"
        changed.append(new)
    return changed


# --------------------------------------------------------------------------
# Транзакции
# --------------------------------------------------------------------------


def generate_day(
    day: date,
    customers: list[dict],
    products: list[dict],
    orders_per_day: int = 800,
    seed: int = 0,
    dup_ratio: float = 0.01,
    dirty_ratio: float = 0.02,
    late_ratio: float = 0.03,
) -> tuple[list[dict], list[dict]]:
    """Заказы и позиции заказов за один день.

    Специально закладываем:
    * `dup_ratio`  — точные дубли строк (ретрай продюсера) → дедуп в Silver;
    * `dirty_ratio`— NULL customer_id, отрицательное количество, битая цена → DQ;
    * `late_ratio` — события «вчерашним числом» → watermark и late-arriving data.
    """
    rnd = _rng(seed + day.toordinal())
    weekday_boost = 1.25 if day.weekday() >= 5 else 1.0
    n_orders = int(orders_per_day * weekday_boost)

    orders: list[dict] = []
    items: list[dict] = []

    for i in range(n_orders):
        cust = rnd.choice(customers)
        ts = datetime(day.year, day.month, day.day, rnd.randint(0, 23), rnd.randint(0, 59), rnd.randint(0, 59))
        if rnd.random() < late_ratio:  # опоздавшее событие
            ts -= timedelta(days=rnd.randint(1, 3))

        order_id = _stable_id("O", day.isoformat(), i)
        order = {
            "order_id": order_id,
            "customer_id": cust["customer_id"],
            "order_ts": ts.isoformat(timespec="seconds"),
            "status": rnd.choices(STATUSES, weights=[0.05, 0.3, 0.2, 0.3, 0.1, 0.05])[0],
            "channel": rnd.choices(CHANNELS, weights=[0.45, 0.25, 0.25, 0.05])[0],
            "currency": "EUR",
            "shipping_country": cust["country"],
            "discount_pct": round(rnd.choice([0, 0, 0, 5, 10, 15, 20]) / 100, 2),
        }

        if rnd.random() < dirty_ratio:
            broken = rnd.random()
            if broken < 0.34:
                order["customer_id"] = None
            elif broken < 0.67:
                order["order_ts"] = (ts + timedelta(days=400)).isoformat(timespec="seconds")
            else:
                order["status"] = "UNKNOWN_STATUS"

        orders.append(order)

        for j in range(rnd.randint(1, 5)):
            prod = rnd.choice(products)
            qty = rnd.randint(1, 4)
            price = round(prod["list_price"] * rnd.uniform(0.85, 1.05), 2)
            if rnd.random() < dirty_ratio:
                qty = -qty if rnd.random() < 0.5 else 0
                price = -price
            items.append(
                {
                    "order_item_id": _stable_id("OI", order_id, j),
                    "order_id": order_id,
                    "product_id": prod["product_id"],
                    "quantity": qty,
                    "unit_price": price,
                }
            )

    # дубли — ретраи продюсера
    n_dups = int(len(orders) * dup_ratio)
    for _ in range(n_dups):
        orders.append(dict(rnd.choice(orders)))

    return orders, items


def generate_clickstream(
    day: date,
    customers: list[dict],
    products: list[dict],
    n_events: int = 5000,
    seed: int = 3,
) -> list[dict]:
    """Клик-стрим: сессии по 3–15 событий, event-time вперемешку."""
    rnd = _rng(seed + day.toordinal())
    events: list[dict] = []
    n_sessions = max(1, n_events // 8)

    for s in range(n_sessions):
        cust = rnd.choice(customers) if rnd.random() > 0.25 else None
        session_id = _stable_id("S", day.isoformat(), s)
        start = datetime(day.year, day.month, day.day, rnd.randint(0, 23), rnd.randint(0, 59))
        device = rnd.choice(DEVICES)
        for e in range(rnd.randint(3, 15)):
            ts = start + timedelta(seconds=e * rnd.randint(5, 180))
            event_type = rnd.choices(EVENT_TYPES, weights=[0.4, 0.3, 0.15, 0.05, 0.1])[0]
            product = rnd.choice(products) if event_type != "page_view" else None
            events.append(
                {
                    "event_id": _stable_id("E", session_id, e),
                    "session_id": session_id,
                    "customer_id": cust["customer_id"] if cust else None,
                    "event_ts": ts.isoformat(timespec="seconds"),
                    "event_type": event_type,
                    "product_id": product["product_id"] if product else None,
                    "page_url": f"/{event_type}/{s % 50}",
                    "device": device,
                }
            )
    return events
