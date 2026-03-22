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

5. Import synthetic benchmark dataset:

```bash
python src/import_data.py --rows 10000 --batch-size 2000 --seed 42 --reset
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
