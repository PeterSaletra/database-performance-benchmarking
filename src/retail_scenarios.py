from __future__ import annotations

import datetime as dt
import random
import string
from dataclasses import dataclass
from typing import Any, Callable

from retail_client import RetailDBClient


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    operation: str
    description: str
    executor: Callable[[RetailDBClient, dict[str, Any]], int]


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _random_text(prefix: str = "value", length: int = 8) -> str:
    alphabet = string.ascii_letters + string.digits
    suffix = "".join(random.choice(alphabet) for _ in range(length))
    return f"{prefix}_{suffix}"


def _random_city() -> str:
    return random.choice(["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan", "Lodz"])


def _random_price() -> str:
    return f"{random.uniform(10.0, 500.0):.2f}"


def _create_customer(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    customer_id = client.create_customer({"city": _random_city(), "signup_date": _now()})
    ctx["last_customer_id"] = customer_id
    return 1


def _create_product(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    product_id = client.create_product(
        {
            "category_id": random.randint(1, 20),
            "supplier_id": random.randint(1, 20),
            "price": _random_price(),
        }
    )
    ctx["last_product_id"] = product_id
    return 1


def _create_order(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    order_id = client.create_order(
        {
            "customer_id": ctx["customer_id"],
            "store_id": random.randint(1, 10),
            "order_date": _now(),
            "promotion_id": None,
        }
    )
    ctx["last_order_id"] = order_id
    return 1


def _create_order_item(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    item_id = client.create_order_item(
        {
            "order_id": ctx["order_id"],
            "product_id": ctx["product_id"],
            "qty": random.randint(1, 5),
            "price": _random_price(),
        }
    )
    ctx["last_order_item_id"] = item_id
    return 1 if item_id is not None else 0


def _read_customer(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    return client.read_customer(ctx["customer_id"])


def _read_product(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    return client.read_product(ctx["product_id"])


def _read_order(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    return client.read_order(ctx["order_id"])


def _read_order_items(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    return client.read_order_items_for_order(ctx["order_id"])


def _update_customer_city(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    return client.update_customer_city(ctx["customer_id"], _random_city())


def _update_product_price(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    return client.update_product_price(ctx["product_id"], _random_price())


def _update_order_promotion(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    return client.update_order_promotion(ctx["order_id"], random.randint(1, 50))


def _update_shipment_status(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    return client.update_shipment_status(ctx["shipment_id"], _random_text("status"))


def _delete_order_item(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    order_id = ctx["order_id"]
    item_id = client.create_order_item(
        {
            "order_id": order_id,
            "product_id": ctx["product_id"],
            "qty": random.randint(1, 5),
            "price": _random_price(),
        }
    )
    return client.delete_order_item(item_id)


def _delete_payment(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    order_id = ctx["order_id"]
    payment_id = client.create_payment({"order_id": order_id, "amount": _random_price()})
    return client.delete_payment(payment_id if payment_id is not None else order_id)


def _delete_shipment(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    order_id = ctx["order_id"]
    shipment_id = client.create_shipment({"order_id": order_id, "status": _random_text("ship")})
    return client.delete_shipment(shipment_id if shipment_id is not None else order_id)


def _delete_order_cascade(client: RetailDBClient, ctx: dict[str, Any]) -> int:
    temp_order_id = client.create_order(
        {
            "customer_id": ctx["customer_id"],
            "store_id": random.randint(1, 10),
            "order_date": _now(),
            "promotion_id": random.randint(1, 50),
        }
    )
    client.create_order_item(
        {
            "order_id": temp_order_id,
            "product_id": ctx["product_id"],
            "qty": 1,
            "price": _random_price(),
        }
    )
    client.create_payment({"order_id": temp_order_id, "amount": _random_price()})
    client.create_shipment({"order_id": temp_order_id, "status": "pending"})
    return client.delete_order_cascade(temp_order_id)


def build_all_scenarios() -> list[Scenario]:
    return [
        Scenario("C1", "create", "Create customer", _create_customer),
        Scenario("C2", "create", "Create product", _create_product),
        Scenario("C3", "create", "Create order", _create_order),
        Scenario("C4", "create", "Create order item", _create_order_item),
        Scenario("R1", "read", "Read customer", _read_customer),
        Scenario("R2", "read", "Read product", _read_product),
        Scenario("R3", "read", "Read order", _read_order),
        Scenario("R4", "read", "Read order items for order", _read_order_items),
        Scenario("U1", "update", "Update customer city", _update_customer_city),
        Scenario("U2", "update", "Update product price", _update_product_price),
        Scenario("U3", "update", "Update order promotion", _update_order_promotion),
        Scenario("U4", "update", "Update shipment status", _update_shipment_status),
        Scenario("D1", "delete", "Delete order item", _delete_order_item),
        Scenario("D2", "delete", "Delete payment", _delete_payment),
        Scenario("D3", "delete", "Delete shipment", _delete_shipment),
        Scenario("D4", "delete", "Delete order cascade", _delete_order_cascade),
    ]
