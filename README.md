# database-performance-benchmarking
A comparative performance analysis of relational and non-relational database management systems for a university project

## Test environment quickstart

1. Copy `.env.example` to `.env`.
2. Start services:

```bash
docker compose up -d
```

3. Install Python dependencies:

```bash
python -m venv .venv
. .venv/Scripts/activate
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

Detailed setup instructions are available in `docs/environment-setup.md`.
