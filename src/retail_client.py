from __future__ import annotations

import datetime as dt
import json
import os
import random
from abc import ABC, abstractmethod
from typing import Any

import mysql.connector
import psycopg
from pymongo import MongoClient


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _utc_date() -> dt.datetime:
    return _utc_now()


def _random_string(prefix: str = "x") -> str:
    return f"{prefix}_{random.randint(100000, 999999)}"


def _random_city() -> str:
    return random.choice(["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan", "Lodz"])


def _random_status() -> str:
    return random.choice(["new", "packed", "shipped", "delivered", "returned"])


def _random_price() -> str:
    return f"{random.uniform(5.0, 999.0):.2f}"


def _json_loads(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _json_dumps(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False)


def _first_value(row: Any) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    if isinstance(row, (list, tuple)):
        return row[0] if row else None
    return row


class RetailDBClient(ABC):
    name: str

    def setup(self) -> None:
        return None

    def configure_mode(self, mode: str) -> None:
        _ = mode

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def sample_customer_id(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sample_product_id(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sample_order_id(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sample_order_customer_id(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sample_order_store_id(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sample_order_promotion_id(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sample_order_date(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sample_order_item_id(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sample_payment_id(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sample_shipment_id(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def create_customer(self, data: dict[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def create_product(self, data: dict[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def create_order(self, data: dict[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def create_order_item(self, data: dict[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def create_payment(self, data: dict[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def create_shipment(self, data: dict[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def read_customer(self, customer_id: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def read_product(self, product_id: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def read_order(self, order_id: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def read_order_items_for_order(self, order_id: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def read_orders_by_customer_id(self, customer_id: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def read_orders_by_store_id(self, store_id: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def read_orders_by_promotion_id(self, promotion_id: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def update_customer_city(self, customer_id: Any, city: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def update_product_price(self, product_id: Any, price: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def update_order_promotion(self, order_id: Any, promotion_id: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def update_order_customer(self, order_id: Any, customer_id: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def update_order_store(self, order_id: Any, store_id: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def update_order_date(self, order_id: Any, order_date: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def update_shipment_status(self, shipment_id_or_order_id: Any, status: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def delete_order_item(self, order_item_id: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def delete_payment(self, payment_id_or_order_id: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def delete_shipment(self, shipment_id_or_order_id: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def delete_order_cascade(self, order_id: Any) -> int:
        raise NotImplementedError

    def explain_samples(self) -> dict[str, str]:
        return {}


class _SQLRetailClient(RetailDBClient):
    customer_table = "customers"
    product_table = "products"
    order_table = "orders"
    order_item_table = "order_items"
    payment_table = "payments"
    shipment_table = "shipments"

    def setup(self) -> None:
        return None

    def _mode_index_specs(self) -> list[tuple[str, str, str]]:
        return [
            ("idx_orders_customer_id", self.order_table, "customer_id"),
            ("idx_orders_store_id", self.order_table, "store_id"),
            ("idx_orders_order_date", self.order_table, "order_date"),
            ("idx_orders_promotion_id", self.order_table, "promotion_id"),
            ("idx_order_items_order_id", self.order_item_table, "order_id"),
            ("idx_payments_order_id", self.payment_table, "order_id"),
            ("idx_shipments_order_id", self.shipment_table, "order_id"),
        ]

    def _create_mode_indexes(self) -> None:
        return None

    def _drop_mode_indexes(self) -> None:
        return None

    def configure_mode(self, mode: str) -> None:
        if mode == "after-index":
            self._create_mode_indexes()
        else:
            self._drop_mode_indexes()

    def _scalar(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            row = cur.fetchone()
            return _first_value(row)
        finally:
            cur.close()

    def _count(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            row = cur.fetchone()
            return int(_first_value(row) or 0)
        finally:
            cur.close()

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            affected = cur.rowcount or 0
            self.conn.commit()
            return int(affected)
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def _next_id(self, table: str, column: str) -> int:
        next_value = self._scalar(f"SELECT COALESCE(MAX({column}), 0) + 1 FROM {table}")
        return int(next_value or 1)

    def sample_customer_id(self) -> Any:
        return self._scalar(f"SELECT customer_id FROM {self.customer_table} ORDER BY customer_id LIMIT 1")

    def sample_product_id(self) -> Any:
        return self._scalar(f"SELECT product_id FROM {self.product_table} ORDER BY product_id LIMIT 1")

    def sample_order_id(self) -> Any:
        return self._scalar(f"SELECT order_id FROM {self.order_table} ORDER BY order_id LIMIT 1")

    def sample_order_customer_id(self) -> Any:
        return self._scalar(f"SELECT customer_id FROM {self.order_table} ORDER BY order_id LIMIT 1")

    def sample_order_store_id(self) -> Any:
        return self._scalar(f"SELECT store_id FROM {self.order_table} ORDER BY order_id LIMIT 1")

    def sample_order_promotion_id(self) -> Any:
        return self._scalar(f"SELECT promotion_id FROM {self.order_table} WHERE promotion_id IS NOT NULL ORDER BY order_id LIMIT 1")

    def sample_order_date(self) -> Any:
        return self._scalar(f"SELECT order_date FROM {self.order_table} ORDER BY order_id LIMIT 1")

    def sample_order_item_id(self) -> Any:
        return self._scalar(f"SELECT order_item_id FROM {self.order_item_table} ORDER BY order_item_id LIMIT 1")

    def sample_payment_id(self) -> Any:
        return self._scalar(f"SELECT payment_id FROM {self.payment_table} ORDER BY payment_id LIMIT 1")

    def sample_shipment_id(self) -> Any:
        return self._scalar(f"SELECT shipment_id FROM {self.shipment_table} ORDER BY shipment_id LIMIT 1")

    def create_customer(self, data: dict[str, Any]) -> Any:
        new_id = self._next_id(self.customer_table, "customer_id")
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"INSERT INTO {self.customer_table} (customer_id, city, signup_date) VALUES (%s, %s, %s)",
                (new_id, data["city"], data["signup_date"]),
            )
            self.conn.commit()
            return new_id
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def create_product(self, data: dict[str, Any]) -> Any:
        new_id = self._next_id(self.product_table, "product_id")
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"INSERT INTO {self.product_table} (product_id, category_id, supplier_id, price) VALUES (%s, %s, %s, %s)",
                (new_id, data["category_id"], data["supplier_id"], data["price"]),
            )
            self.conn.commit()
            return new_id
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def create_order(self, data: dict[str, Any]) -> Any:
        new_id = self._next_id(self.order_table, "order_id")
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"INSERT INTO {self.order_table} (order_id, customer_id, store_id, order_date, promotion_id) VALUES (%s, %s, %s, %s, %s)",
                (new_id, data["customer_id"], data["store_id"], data["order_date"], data.get("promotion_id")),
            )
            self.conn.commit()
            return new_id
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def create_order_item(self, data: dict[str, Any]) -> Any:
        new_id = self._next_id(self.order_item_table, "order_item_id")
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"INSERT INTO {self.order_item_table} (order_item_id, order_id, product_id, qty, price) VALUES (%s, %s, %s, %s, %s)",
                (new_id, data["order_id"], data["product_id"], data["qty"], data["price"]),
            )
            self.conn.commit()
            return new_id
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def create_payment(self, data: dict[str, Any]) -> Any:
        new_id = self._next_id(self.payment_table, "payment_id")
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"INSERT INTO {self.payment_table} (payment_id, order_id, amount) VALUES (%s, %s, %s)",
                (new_id, data["order_id"], data["amount"]),
            )
            self.conn.commit()
            return new_id
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def create_shipment(self, data: dict[str, Any]) -> Any:
        new_id = self._next_id(self.shipment_table, "shipment_id")
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"INSERT INTO {self.shipment_table} (shipment_id, order_id, status) VALUES (%s, %s, %s)",
                (new_id, data["order_id"], data["status"]),
            )
            self.conn.commit()
            return new_id
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def read_customer(self, customer_id: Any) -> int:
        return self._count(f"SELECT COUNT(*) FROM {self.customer_table} WHERE customer_id = %s", (customer_id,))

    def read_product(self, product_id: Any) -> int:
        return self._count(f"SELECT COUNT(*) FROM {self.product_table} WHERE product_id = %s", (product_id,))

    def read_order(self, order_id: Any) -> int:
        return self._count(f"SELECT COUNT(*) FROM {self.order_table} WHERE order_id = %s", (order_id,))

    def read_order_items_for_order(self, order_id: Any) -> int:
        return self._count(f"SELECT COUNT(*) FROM {self.order_item_table} WHERE order_id = %s", (order_id,))

    def read_orders_by_customer_id(self, customer_id: Any) -> int:
        return self._count(f"SELECT COUNT(*) FROM {self.order_table} WHERE customer_id = %s", (customer_id,))

    def read_orders_by_store_id(self, store_id: Any) -> int:
        return self._count(f"SELECT COUNT(*) FROM {self.order_table} WHERE store_id = %s", (store_id,))

    def read_orders_by_promotion_id(self, promotion_id: Any) -> int:
        return self._count(f"SELECT COUNT(*) FROM {self.order_table} WHERE promotion_id = %s", (promotion_id,))

    def update_customer_city(self, customer_id: Any, city: str) -> int:
        return self._execute(f"UPDATE {self.customer_table} SET city = %s WHERE customer_id = %s", (city, customer_id))

    def update_product_price(self, product_id: Any, price: str) -> int:
        return self._execute(f"UPDATE {self.product_table} SET price = %s WHERE product_id = %s", (price, product_id))

    def update_order_promotion(self, order_id: Any, promotion_id: Any) -> int:
        return self._execute(
            f"UPDATE {self.order_table} SET promotion_id = %s WHERE order_id = %s",
            (promotion_id, order_id),
        )

    def update_order_customer(self, order_id: Any, customer_id: Any) -> int:
        return self._execute(f"UPDATE {self.order_table} SET customer_id = %s WHERE order_id = %s", (customer_id, order_id))

    def update_order_store(self, order_id: Any, store_id: Any) -> int:
        return self._execute(f"UPDATE {self.order_table} SET store_id = %s WHERE order_id = %s", (store_id, order_id))

    def update_order_date(self, order_id: Any, order_date: Any) -> int:
        return self._execute(f"UPDATE {self.order_table} SET order_date = %s WHERE order_id = %s", (order_date, order_id))

    def update_shipment_status(self, shipment_id_or_order_id: Any, status: str) -> int:
        return self._execute(
            f"UPDATE {self.shipment_table} SET status = %s WHERE shipment_id = %s OR order_id = %s",
            (status, shipment_id_or_order_id, shipment_id_or_order_id),
        )

    def delete_order_item(self, order_item_id: Any) -> int:
        return self._execute(f"DELETE FROM {self.order_item_table} WHERE order_item_id = %s", (order_item_id,))

    def delete_payment(self, payment_id_or_order_id: Any) -> int:
        return self._execute(
            f"DELETE FROM {self.payment_table} WHERE payment_id = %s OR order_id = %s",
            (payment_id_or_order_id, payment_id_or_order_id),
        )

    def delete_shipment(self, shipment_id_or_order_id: Any) -> int:
        return self._execute(
            f"DELETE FROM {self.shipment_table} WHERE shipment_id = %s OR order_id = %s",
            (shipment_id_or_order_id, shipment_id_or_order_id),
        )

    def delete_order_cascade(self, order_id: Any) -> int:
        cur = self.conn.cursor()
        try:
            cur.execute(f"DELETE FROM {self.order_item_table} WHERE order_id = %s", (order_id,))
            cur.execute(f"DELETE FROM {self.payment_table} WHERE order_id = %s", (order_id,))
            cur.execute(f"DELETE FROM {self.shipment_table} WHERE order_id = %s", (order_id,))
            cur.execute(f"DELETE FROM {self.order_table} WHERE order_id = %s", (order_id,))
            affected = cur.rowcount or 0
            self.conn.commit()
            return int(affected)
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def explain_samples(self) -> dict[str, str]:
        out: dict[str, str] = {}
        cur = self.conn.cursor()
        try:
            cur.execute(f"EXPLAIN SELECT customer_id FROM {self.customer_table} WHERE customer_id = %s", (self.sample_customer_id(),))
            out["customer_read"] = "\n".join(str(row[0]) for row in cur.fetchall())
            cur.execute(f"EXPLAIN SELECT order_id FROM {self.order_table} WHERE order_id = %s", (self.sample_order_id(),))
            out["order_read"] = "\n".join(str(row[0]) for row in cur.fetchall())
        finally:
            cur.close()
        return out


class PostgresRetailClient(_SQLRetailClient):
    name = "postgres"

    def __init__(self) -> None:
        self.conn = psycopg.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "benchmark_db"),
            user=os.getenv("POSTGRES_USER", "benchmark_user"),
            password=os.getenv("POSTGRES_PASSWORD", "benchmark_pass"),
        )
        self.conn.autocommit = False

    def _create_mode_indexes(self) -> None:
        cur = self.conn.cursor()
        try:
            for index_name, table_name, column_name in self._mode_index_specs():
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_name})"
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def _drop_mode_indexes(self) -> None:
        cur = self.conn.cursor()
        try:
            for index_name, _, _ in self._mode_index_specs():
                cur.execute(f"DROP INDEX IF EXISTS {index_name}")
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def close(self) -> None:
        self.conn.close()


class MySQLRetailClient(_SQLRetailClient):
    name = "mysql"

    def __init__(self) -> None:
        self.conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("MYSQL_DATABASE", "benchmark_db"),
            user=os.getenv("MYSQL_USER", "benchmark_user"),
            password=os.getenv("MYSQL_PASSWORD", "benchmark_pass"),
            autocommit=False,
        )

    def _mysql_index_exists(self, table_name: str, index_name: str) -> bool:
        cur = self.conn.cursor()
        try:
            cur.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name = %s", (index_name,))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def _create_mode_indexes(self) -> None:
        cur = self.conn.cursor()
        try:
            for index_name, table_name, column_name in self._mode_index_specs():
                if self._mysql_index_exists(table_name, index_name):
                    continue
                cur.execute(f"CREATE INDEX {index_name} ON {table_name} ({column_name})")
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def _drop_mode_indexes(self) -> None:
        cur = self.conn.cursor()
        try:
            for index_name, table_name, _ in self._mode_index_specs():
                try:
                    cur.execute(f"DROP INDEX {index_name} ON {table_name}")
                except Exception:
                    self.conn.rollback()
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def close(self) -> None:
        self.conn.close()


class MongoRetailClient(RetailDBClient):
    name = "mongo"

    def __init__(self) -> None:
        user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "benchmark_user")
        password = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "benchmark_pass")
        host = os.getenv("MONGO_HOST", "localhost")
        port = int(os.getenv("MONGO_PORT", "27017"))
        db_name = os.getenv("MONGO_DB", "benchmark_db")
        self.client = MongoClient(f"mongodb://{user}:{password}@{host}:{port}/?authSource=admin", serverSelectionTimeoutMS=5000)
        self.db = self.client[db_name]
        self.customers = self.db["customers"]
        self.products = self.db["products"]
        self.orders = self.db["orders"]

    def configure_mode(self, mode: str) -> None:
        if mode == "after-index":
            try:
                self.orders.create_index([("customer_id", 1)], name="idx_orders_customer_id")
            except Exception:
                pass
            try:
                self.orders.create_index([("store_id", 1)], name="idx_orders_store_id")
            except Exception:
                pass
            try:
                self.orders.create_index([("order_date", 1)], name="idx_orders_order_date")
            except Exception:
                pass
            try:
                self.orders.create_index([("promotion_id", 1)], name="idx_orders_promotion_id")
            except Exception:
                pass
        else:
            for collection, index_name in [
                (self.orders, "idx_orders_customer_id"),
                (self.orders, "idx_orders_store_id"),
                (self.orders, "idx_orders_order_date"),
                (self.orders, "idx_orders_promotion_id"),
            ]:
                try:
                    collection.drop_index(index_name)
                except Exception:
                    pass

    def close(self) -> None:
        self.client.close()

    def _sample_id(self, collection_name: str, dotted_field: str | None = None) -> Any:
        projection = {"_id": 1}
        if dotted_field:
            projection[dotted_field] = 1
        doc = self.db[collection_name].find_one({}, projection)
        if not doc:
            return None
        if dotted_field:
            current: Any = doc
            for part in dotted_field.split("."):
                if isinstance(current, dict):
                    current = current.get(part)
                else:
                    current = None
                if current is None:
                    break
            if isinstance(current, dict):
                return current.get("_id") or current.get("id")
            return current
        return doc.get("_id")

    def sample_customer_id(self) -> Any:
        return self._sample_id("customers")

    def sample_product_id(self) -> Any:
        return self._sample_id("products")

    def sample_order_id(self) -> Any:
        return self._sample_id("orders")

    def sample_order_customer_id(self) -> Any:
        return self._sample_id("orders", "customer_id")

    def sample_order_store_id(self) -> Any:
        return self._sample_id("orders", "store_id")

    def sample_order_promotion_id(self) -> Any:
        doc = self.orders.find_one({"promotion_id": {"$ne": None}}, {"promotion_id": 1})
        if not doc:
            return self.sample_order_id()
        return doc.get("promotion_id") or doc.get("_id")

    def sample_order_date(self) -> Any:
        doc = self.orders.find_one({}, {"order_date": 1})
        if not doc:
            return None
        return doc.get("order_date")

    def sample_order_item_id(self) -> Any:
        doc = self.orders.find_one({"items.0": {"$exists": True}}, {"items": 1})
        if not doc:
            return self.sample_order_id()
        item = doc.get("items", [])[0]
        if isinstance(item, dict):
            return item.get("order_item_id") or doc.get("_id")
        return doc.get("_id")

    def sample_payment_id(self) -> Any:
        doc = self.orders.find_one({"payment.payment_id": {"$exists": True}}, {"payment": 1})
        if not doc:
            return self.sample_order_id()
        payment = doc.get("payment") or {}
        if isinstance(payment, dict):
            return payment.get("payment_id") or doc.get("_id")
        return doc.get("_id")

    def sample_shipment_id(self) -> Any:
        doc = self.orders.find_one({"shipment.shipment_id": {"$exists": True}}, {"shipment": 1})
        if not doc:
            return self.sample_order_id()
        shipment = doc.get("shipment") or {}
        if isinstance(shipment, dict):
            return shipment.get("shipment_id") or doc.get("_id")
        return doc.get("_id")

    def create_customer(self, data: dict[str, Any]) -> Any:
        doc = {"_id": _random_string("customer"), "city": data["city"], "signup_date": data["signup_date"].isoformat()}
        self.customers.insert_one(doc)
        return doc["_id"]

    def create_product(self, data: dict[str, Any]) -> Any:
        doc = {
            "_id": _random_string("product"),
            "category_id": data["category_id"],
            "supplier_id": data["supplier_id"],
            "price": data["price"],
        }
        self.products.insert_one(doc)
        return doc["_id"]

    def create_order(self, data: dict[str, Any]) -> Any:
        doc = {
            "_id": _random_string("order"),
            "customer_id": data["customer_id"],
            "store_id": data["store_id"],
            "order_date": data["order_date"].isoformat(),
            "promotion_id": data.get("promotion_id"),
            "items": [],
        }
        self.orders.insert_one(doc)
        return doc["_id"]

    def create_order_item(self, data: dict[str, Any]) -> Any:
        item = {
            "order_item_id": _random_string("item"),
            "product_id": data["product_id"],
            "qty": data["qty"],
            "price": data["price"],
        }
        out = self.orders.update_one({"_id": data["order_id"]}, {"$push": {"items": item}})
        if out.matched_count == 0:
            return None
        return item["order_item_id"]

    def create_payment(self, data: dict[str, Any]) -> Any:
        payment = {"payment_id": _random_string("payment"), "amount": data["amount"]}
        out = self.orders.update_one({"_id": data["order_id"]}, {"$set": {"payment": payment}})
        if out.matched_count == 0:
            return None
        return payment["payment_id"]

    def create_shipment(self, data: dict[str, Any]) -> Any:
        shipment = {"shipment_id": _random_string("shipment"), "status": data["status"]}
        out = self.orders.update_one({"_id": data["order_id"]}, {"$set": {"shipment": shipment}})
        if out.matched_count == 0:
            return None
        return shipment["shipment_id"]

    def read_customer(self, customer_id: Any) -> int:
        return 1 if self.customers.find_one({"_id": customer_id}, {"_id": 1}) else 0

    def read_product(self, product_id: Any) -> int:
        return 1 if self.products.find_one({"_id": product_id}, {"_id": 1}) else 0

    def read_order(self, order_id: Any) -> int:
        return 1 if self.orders.find_one({"_id": order_id}, {"_id": 1}) else 0

    def read_order_items_for_order(self, order_id: Any) -> int:
        doc = self.orders.find_one({"_id": order_id}, {"items": 1})
        return len(doc.get("items", [])) if doc else 0

    def read_orders_by_customer_id(self, customer_id: Any) -> int:
        return int(self.orders.count_documents({"customer_id": customer_id}))

    def read_orders_by_store_id(self, store_id: Any) -> int:
        return int(self.orders.count_documents({"store_id": store_id}))

    def read_orders_by_promotion_id(self, promotion_id: Any) -> int:
        return int(self.orders.count_documents({"promotion_id": promotion_id}))

    def update_customer_city(self, customer_id: Any, city: str) -> int:
        return int(self.customers.update_one({"_id": customer_id}, {"$set": {"city": city}}).modified_count)

    def update_product_price(self, product_id: Any, price: str) -> int:
        return int(self.products.update_one({"_id": product_id}, {"$set": {"price": price}}).modified_count)

    def update_order_promotion(self, order_id: Any, promotion_id: Any) -> int:
        return int(self.orders.update_one({"_id": order_id}, {"$set": {"promotion_id": promotion_id}}).modified_count)

    def update_order_customer(self, order_id: Any, customer_id: Any) -> int:
        return int(self.orders.update_one({"_id": order_id}, {"$set": {"customer_id": customer_id}}).modified_count)

    def update_order_store(self, order_id: Any, store_id: Any) -> int:
        return int(self.orders.update_one({"_id": order_id}, {"$set": {"store_id": store_id}}).modified_count)

    def update_order_date(self, order_id: Any, order_date: Any) -> int:
        value = order_date.isoformat() if hasattr(order_date, "isoformat") else order_date
        return int(self.orders.update_one({"_id": order_id}, {"$set": {"order_date": value}}).modified_count)

    def update_shipment_status(self, shipment_id_or_order_id: Any, status: str) -> int:
        out = self.orders.update_one(
            {"$or": [{"_id": shipment_id_or_order_id}, {"shipment.shipment_id": shipment_id_or_order_id}]},
            {"$set": {"shipment.status": status}},
        )
        return int(out.modified_count)

    def delete_order_item(self, order_item_id: Any) -> int:
        out = self.orders.update_many({"items.order_item_id": order_item_id}, {"$pull": {"items": {"order_item_id": order_item_id}}})
        return int(out.modified_count)

    def delete_payment(self, payment_id_or_order_id: Any) -> int:
        out = self.orders.update_many(
            {"$or": [{"_id": payment_id_or_order_id}, {"payment.payment_id": payment_id_or_order_id}]},
            {"$unset": {"payment": ""}},
        )
        return int(out.modified_count)

    def delete_shipment(self, shipment_id_or_order_id: Any) -> int:
        out = self.orders.update_many(
            {"$or": [{"_id": shipment_id_or_order_id}, {"shipment.shipment_id": shipment_id_or_order_id}]},
            {"$unset": {"shipment": ""}},
        )
        return int(out.modified_count)

    def delete_order_cascade(self, order_id: Any) -> int:
        return int(self.orders.delete_one({"_id": order_id}).deleted_count)


class ScyllaRetailClient(RetailDBClient):
    name = "scylla"

    def __init__(self) -> None:
        try:
            from cassandra.cluster import Cluster
        except ModuleNotFoundError as exc:
            raise RuntimeError("Missing dependency 'cassandra-driver'. Run: pip install -r requirements.txt") from exc

        host = os.getenv("SCYLLA_HOST", "localhost")
        port = int(os.getenv("SCYLLA_PORT", "9042"))
        self.keyspace = os.getenv("SCYLLA_KEYSPACE", "benchmark_db")
        self.cluster = Cluster([host], port=port)
        self.session = self.cluster.connect()
        self.session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {self.keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        )
        self.session.set_keyspace(self.keyspace)
        self._ensure_table("retail_customers")
        self._ensure_table("retail_products")
        self._ensure_table("retail_orders")

    def _ensure_table(self, table: str) -> None:
        self.session.execute(f"CREATE TABLE IF NOT EXISTS {table} (id text PRIMARY KEY, data text)")

    def configure_mode(self, mode: str) -> None:
        if mode == "after-index":
            for table_name, column_name in [
                ("scylla_orders_customer_id", "customer_id"),
                ("scylla_orders_store_id", "store_id"),
                ("scylla_orders_order_date", "order_date"),
                ("scylla_orders_promotion_id", "promotion_id"),
            ]:
                try:
                    self.session.execute(
                        f"CREATE TABLE IF NOT EXISTS {table_name} (order_id text PRIMARY KEY, {column_name} text)"
                    )
                    self.session.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{column_name} ON {table_name} ({column_name})"
                    )
                except Exception:
                    pass
        else:
            # Secondary indexes are optional here; baseline just keeps the default schema.
            return None

    def close(self) -> None:
        self.cluster.shutdown()

    def _sample_id(self, table: str, dotted_field: str | None = None) -> Any:
        row = self.session.execute(f"SELECT id, data FROM {table} LIMIT 1").one()
        if not row:
            return None
        if not dotted_field:
            return getattr(row, "id", None)
        doc = _json_loads(getattr(row, "data", None))
        current: Any = doc
        for part in dotted_field.split("."):
            if isinstance(current, dict):
                current = current.get(part)
            else:
                current = None
            if current is None:
                break
        if isinstance(current, dict):
            return current.get("_id") or current.get("id")
        return current

    def sample_customer_id(self) -> Any:
        return self._sample_id("retail_customers")

    def sample_product_id(self) -> Any:
        return self._sample_id("retail_products")

    def sample_order_id(self) -> Any:
        return self._sample_id("retail_orders")

    def sample_order_customer_id(self) -> Any:
        return self._sample_id("retail_orders", "customer_id")

    def sample_order_store_id(self) -> Any:
        return self._sample_id("retail_orders", "store_id")

    def sample_order_promotion_id(self) -> Any:
        value = self._sample_id("retail_orders", "promotion_id")
        return value or self.sample_order_id()

    def sample_order_date(self) -> Any:
        return self._sample_id("retail_orders", "order_date")

    def sample_order_item_id(self) -> Any:
        item_id = self._sample_id("retail_orders", "items")
        return item_id or self.sample_order_id()

    def sample_payment_id(self) -> Any:
        payment_id = self._sample_id("retail_orders", "payment")
        return payment_id or self.sample_order_id()

    def sample_shipment_id(self) -> Any:
        shipment_id = self._sample_id("retail_orders", "shipment")
        return shipment_id or self.sample_order_id()

    def _insert_json_row(self, table: str, row_id: str, data: dict[str, Any]) -> None:
        self.session.execute(f"INSERT INTO {table} (id, data) VALUES (%s, %s)", (row_id, _json_dumps(data)))

    def _sync_order_sidecars(self, order_id: Any, doc: dict[str, Any]) -> None:
        if order_id is None:
            return
        for table_name, column_name, value in [
            ("scylla_orders_customer_id", "customer_id", doc.get("customer_id")),
            ("scylla_orders_store_id", "store_id", doc.get("store_id")),
            ("scylla_orders_order_date", "order_date", doc.get("order_date")),
            ("scylla_orders_promotion_id", "promotion_id", doc.get("promotion_id")),
        ]:
            if value is None:
                continue
            try:
                self.session.execute(
                    f"INSERT INTO {table_name} (order_id, {column_name}) VALUES (%s, %s)",
                    (order_id, value if not hasattr(value, "isoformat") else value.isoformat()),
                )
            except Exception:
                pass

    def create_customer(self, data: dict[str, Any]) -> Any:
        row_id = _random_string("customer")
        self._insert_json_row("retail_customers", row_id, {"city": data["city"], "signup_date": data["signup_date"].isoformat()})
        return row_id

    def create_product(self, data: dict[str, Any]) -> Any:
        row_id = _random_string("product")
        self._insert_json_row(
            "retail_products",
            row_id,
            {"category_id": data["category_id"], "supplier_id": data["supplier_id"], "price": data["price"]},
        )
        return row_id

    def create_order(self, data: dict[str, Any]) -> Any:
        row_id = _random_string("order")
        doc = {
            "customer_id": data["customer_id"],
            "store_id": data["store_id"],
            "order_date": data["order_date"].isoformat(),
            "promotion_id": data.get("promotion_id"),
            "items": [],
        }
        self._insert_json_row("retail_orders", row_id, doc)
        self._sync_order_sidecars(row_id, doc)
        return row_id

    def _load_order_doc(self, order_id: Any) -> dict[str, Any] | None:
        row = self.session.execute("SELECT data FROM retail_orders WHERE id = %s", (order_id,)).one()
        if not row:
            return None
        return _json_loads(getattr(row, "data", None))

    def _find_order_doc_by_substring(self, needle: str) -> tuple[Any, dict[str, Any]] | None:
        for row in self.session.execute("SELECT id, data FROM retail_orders"):
            doc = _json_loads(getattr(row, "data", None))
            if needle in json.dumps(doc, ensure_ascii=False):
                return getattr(row, "id", None), doc
        return None

    def _save_order_doc(self, order_id: Any, doc: dict[str, Any]) -> int:
        self.session.execute("UPDATE retail_orders SET data = %s WHERE id = %s", (_json_dumps(doc), order_id))
        self._sync_order_sidecars(order_id, doc)
        return 1

    def create_order_item(self, data: dict[str, Any]) -> Any:
        doc = self._load_order_doc(data["order_id"])
        if doc is None:
            return None
        item_id = _random_string("item")
        items = list(doc.get("items", []))
        items.append({"order_item_id": item_id, "product_id": data["product_id"], "qty": data["qty"], "price": data["price"]})
        doc["items"] = items
        self._save_order_doc(data["order_id"], doc)
        return item_id

    def create_payment(self, data: dict[str, Any]) -> Any:
        doc = self._load_order_doc(data["order_id"])
        if doc is None:
            return None
        payment = {"payment_id": _random_string("payment"), "amount": data["amount"]}
        doc["payment"] = payment
        self._save_order_doc(data["order_id"], doc)
        return payment["payment_id"]

    def create_shipment(self, data: dict[str, Any]) -> Any:
        doc = self._load_order_doc(data["order_id"])
        if doc is None:
            return None
        shipment = {"shipment_id": _random_string("shipment"), "status": data["status"]}
        doc["shipment"] = shipment
        self._save_order_doc(data["order_id"], doc)
        return shipment["shipment_id"]

    def read_customer(self, customer_id: Any) -> int:
        return 1 if self.session.execute("SELECT id FROM retail_customers WHERE id = %s", (customer_id,)).one() else 0

    def read_product(self, product_id: Any) -> int:
        return 1 if self.session.execute("SELECT id FROM retail_products WHERE id = %s", (product_id,)).one() else 0

    def read_order(self, order_id: Any) -> int:
        return 1 if self.session.execute("SELECT id FROM retail_orders WHERE id = %s", (order_id,)).one() else 0

    def read_order_items_for_order(self, order_id: Any) -> int:
        doc = self._load_order_doc(order_id)
        return len(doc.get("items", [])) if doc else 0

    def read_orders_by_customer_id(self, customer_id: Any) -> int:
        try:
            row = self.session.execute("SELECT order_id FROM scylla_orders_customer_id WHERE customer_id = %s ALLOW FILTERING", (customer_id,)).one()
            return 1 if row else 0
        except Exception:
            total = 0
            for row in self.session.execute("SELECT data FROM retail_orders"):
                doc = _json_loads(getattr(row, "data", None))
                if doc.get("customer_id") == customer_id:
                    total += 1
            return total

    def read_orders_by_store_id(self, store_id: Any) -> int:
        try:
            row = self.session.execute("SELECT order_id FROM scylla_orders_store_id WHERE store_id = %s ALLOW FILTERING", (store_id,)).one()
            return 1 if row else 0
        except Exception:
            total = 0
            for row in self.session.execute("SELECT data FROM retail_orders"):
                doc = _json_loads(getattr(row, "data", None))
                if doc.get("store_id") == store_id:
                    total += 1
            return total

    def read_orders_by_promotion_id(self, promotion_id: Any) -> int:
        try:
            row = self.session.execute("SELECT order_id FROM scylla_orders_promotion_id WHERE promotion_id = %s ALLOW FILTERING", (promotion_id,)).one()
            return 1 if row else 0
        except Exception:
            total = 0
            for row in self.session.execute("SELECT data FROM retail_orders"):
                doc = _json_loads(getattr(row, "data", None))
                if doc.get("promotion_id") == promotion_id:
                    total += 1
            return total

    def update_customer_city(self, customer_id: Any, city: str) -> int:
        row = self.session.execute("SELECT data FROM retail_customers WHERE id = %s", (customer_id,)).one()
        if not row:
            return 0
        doc = _json_loads(getattr(row, "data", None))
        doc["city"] = city
        self.session.execute("UPDATE retail_customers SET data = %s WHERE id = %s", (_json_dumps(doc), customer_id))
        return 1

    def update_product_price(self, product_id: Any, price: str) -> int:
        row = self.session.execute("SELECT data FROM retail_products WHERE id = %s", (product_id,)).one()
        if not row:
            return 0
        doc = _json_loads(getattr(row, "data", None))
        doc["price"] = price
        self.session.execute("UPDATE retail_products SET data = %s WHERE id = %s", (_json_dumps(doc), product_id))
        return 1

    def update_order_promotion(self, order_id: Any, promotion_id: Any) -> int:
        doc = self._load_order_doc(order_id)
        if doc is None:
            return 0
        doc["promotion_id"] = promotion_id
        self._save_order_doc(order_id, doc)
        return 1

    def update_order_customer(self, order_id: Any, customer_id: Any) -> int:
        doc = self._load_order_doc(order_id)
        if doc is None:
            return 0
        doc["customer_id"] = customer_id
        self._save_order_doc(order_id, doc)
        return 1

    def update_order_store(self, order_id: Any, store_id: Any) -> int:
        doc = self._load_order_doc(order_id)
        if doc is None:
            return 0
        doc["store_id"] = store_id
        self._save_order_doc(order_id, doc)
        return 1

    def update_order_date(self, order_id: Any, order_date: Any) -> int:
        doc = self._load_order_doc(order_id)
        if doc is None:
            return 0
        doc["order_date"] = order_date.isoformat() if hasattr(order_date, "isoformat") else order_date
        self._save_order_doc(order_id, doc)
        return 1

    def update_shipment_status(self, shipment_id_or_order_id: Any, status: str) -> int:
        order_id = shipment_id_or_order_id
        doc = self._load_order_doc(order_id)
        if doc is None:
            found = self._find_order_doc_by_substring(f'"shipment_id": "{shipment_id_or_order_id}"')
            if found is None:
                return 0
            order_id, doc = found
        shipment = doc.get("shipment") or {}
        shipment["status"] = status
        doc["shipment"] = shipment
        self._save_order_doc(order_id, doc)
        return 1

    def delete_order_item(self, order_item_id: Any) -> int:
        found = self._find_order_doc_by_substring(f'"order_item_id": "{order_item_id}"')
        if found is None:
            return 0
        order_id, doc = found
        items = [item for item in doc.get("items", []) if item.get("order_item_id") != order_item_id]
        if len(items) == len(doc.get("items", [])):
            return 0
        doc["items"] = items
        self._save_order_doc(order_id, doc)
        return 1

    def delete_payment(self, payment_id_or_order_id: Any) -> int:
        order_id = payment_id_or_order_id
        doc = self._load_order_doc(order_id)
        if doc is None:
            found = self._find_order_doc_by_substring(f'"payment_id": "{payment_id_or_order_id}"')
            if found is None:
                return 0
            order_id, doc = found
        if "payment" not in doc:
            return 0
        doc.pop("payment", None)
        self._save_order_doc(order_id, doc)
        return 1

    def delete_shipment(self, shipment_id_or_order_id: Any) -> int:
        order_id = shipment_id_or_order_id
        doc = self._load_order_doc(order_id)
        if doc is None:
            found = self._find_order_doc_by_substring(f'"shipment_id": "{shipment_id_or_order_id}"')
            if found is None:
                return 0
            order_id, doc = found
        if "shipment" not in doc:
            return 0
        doc.pop("shipment", None)
        self._save_order_doc(order_id, doc)
        return 1

    def delete_order_cascade(self, order_id: Any) -> int:
        existing = self.session.execute("SELECT id FROM retail_orders WHERE id = %s", (order_id,)).one()
        if not existing:
            return 0
        self.session.execute("DELETE FROM retail_orders WHERE id = %s", (order_id,))
        return 1



def retail_client_factory(name: str) -> RetailDBClient:
    if name == "postgres":
        return PostgresRetailClient()
    if name == "mysql":
        return MySQLRetailClient()
    if name == "mongo":
        return MongoRetailClient()
    if name == "scylla":
        return ScyllaRetailClient()
    raise ValueError(f"Unsupported engine: {name}")
