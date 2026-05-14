# database-performance-benchmarking
A comparative performance analysis of relational and non-relational database management systems for a university project

## Test environment quickstart

Recommended Python version: 3.11 or 3.12 (ScyllaDB requires `cassandra-driver`, which may fail to install on Python 3.13+ on Windows).

1. Copy `.env.example` to `.env`.
2. Start services:

```bash
docker compose up -d
```

3. Install Python dependencies:

```bash
py -3.12 -m venv .venv
. .venv/Scripts/activate
python --version
pip install -r requirements.txt
```

4. Verify all database connections:

```bash
python src/check_connections.py
```

5. Import benchmark dataset (Retail DWH):

```bash
python src/import_data.py --reset --batch-size 10000 --orders-target-rows 9000000 --nosql-mode denormalized
```

## Import Kaggle Retail DWH dataset (12 tables)

This repo also contains import scripts for the Kaggle dataset:
`datarspectrum/retail-data-warehouse-12-table-1m-rows-dataset`.

Each database has a separate importer (run from the repository root):

```bash
python src/import_retail_postgres.py --reset
python src/import_retail_mysql.py --reset --batch-size 5000
python src/import_retail_mongo.py --reset --batch-size 10000
python src/import_retail_scylla.py --reset --batch-size 10000
```

The importers will also expand the `orders` table/collection to 9,000,000 rows by duplicating records.
You can change this with `--orders-target-rows` (or env `ORDERS_TARGET_ROWS`).

Note: depending on the dataset access settings, `kagglehub` may require Kaggle credentials configured on your machine.

Detailed setup instructions are available in `docs/environment-setup.md`.

## Run benchmark MVP (16 CRUD scenarios)

1. Ensure services are up and data was imported.
2. Run baseline benchmark (without payload indexes):

```bash
python src/run_benchmarks.py --db all --trials 3 --size-label mvp --mode baseline
```

3. Run after-index benchmark (creates payload-prefix indexes where supported):

```bash
python src/run_benchmarks.py --db all --trials 3 --size-label mvp --mode after-index
```

In `after-index` mode the benchmark creates supporting indexes before the run. For SQL databases these are indexes on `order_items.order_id`, `payments.order_id`, and `shipments.order_id`. For MongoDB it also adds indexes on the embedded order fields used by the scenarios, and ScyllaDB keeps the default schema with optional secondary indexes where supported.

4. Generate charts for a single run CSV:

```bash
python src/plot_results.py --input data/results/benchmark_<run_id>.csv --output-dir plots
```

5. Generate baseline vs after-index comparison artifacts:

```bash
python src/plot_results.py --baseline-input data/results/benchmark_<baseline_run_id>.csv --after-input data/results/benchmark_<after_run_id>.csv --output-dir plots
```

Each run also saves EXPLAIN samples to `data/results/explain_<run_id>.json`.

Scenario definitions are documented in `docs/benchmark-scenarios.md`.
