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

## 5. Import synthetic benchmark data

```bash
python src/import_data.py --rows 10000 --batch-size 2000 --seed 42 --reset
```

You can prepare larger datasets by increasing `--rows`, for example:

```bash
python src/import_data.py --rows 100000 --batch-size 5000 --seed 42 --reset
python src/import_data.py --rows 1000000 --batch-size 10000 --seed 42 --reset
```

## 6. Stop services

```bash
docker compose down
```

To remove persisted data volumes:

```bash
docker compose down -v
```
