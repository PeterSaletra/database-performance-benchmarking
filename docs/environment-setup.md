# Test environment setup

Recommended Python version: 3.11 or 3.12 (some dependencies may not provide wheels for Python 3.14 yet).

## 1. Copy environment variables

Create `.env` in the repository root based on `.env.example`.

## 2. Start database services

```bash
docker compose up -d
```

## 3. Install Python dependencies

```bash
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 4. Verify connectivity

```bash
python src/check_connections.py
```

Expected result: all 4 services return `OK`.

## 5. Run DB data setup script

```bash
python src/import_data.py --reset --batch-size 10000 --orders-target-rows 9000000 --nosql-mode denormalized
```

Flags:

- `--reset` - drops existing data before importing (Postgres/MySQL tables, Mongo collections, Redis keys).
- `--batch-size N` - CSV chunk size for MySQL/Mongo/Redis imports (default: `10000`).
- `--orders-target-rows N` - target size for `orders` (default: `9000000`). Relational DBs expand `orders` inside the database; Mongo/Redis expand during import.
- `--nosql-mode tables|denormalized` - for MongoDB and Redis:
	- `tables`: 1 CSV -> 1 collection/namespace (row-oriented)
	- `denormalized`: ERD-like documents/JSON (embedded customer/items/product/category/supplier, etc.)
- `--dataset-id OWNER/DATASET` - overrides the Kaggle dataset id.

Notes:

- In `--nosql-mode denormalized`, MongoDB/Redis import builds a local SQLite cache for `order_items` joins. This cache can be many GB.
- By default, the cache is created under `%TEMP%\retail_dwh_cache` on Windows.
- You can change the location with env `RETAIL_CACHE_DIR` (recommended if your C: drive is small).
- The cache is deleted automatically after a successful import; set env `RETAIL_KEEP_CACHE=1` to keep it.

Examples:

```bash
# import without wiping existing data
python src/import_data.py --batch-size 10000 --orders-target-rows 9000000 --nosql-mode denormalized

# import with "tables" mode for Mongo/Redis
python src/import_data.py --reset --batch-size 10000 --orders-target-rows 9000000 --nosql-mode tables
```


## 6. Stop services

```bash
docker compose down
```

To remove persisted data volumes:

```bash
docker compose down -v
```
