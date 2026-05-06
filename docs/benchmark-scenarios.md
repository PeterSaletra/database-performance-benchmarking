# Benchmark Scenarios (Layer 1)

Ten dokument opisuje MVP benchmarku opartego o realne encje retail.
Scenariusze działają na tabelach PostgreSQL/MySQL oraz na odpowiednikach dokumentowych dla MongoDB i ScyllaDB.

## Parametry MVP

- Liczba scenariuszy: 16
- Struktura: 4x Create, 4x Read, 4x Update, 4x Delete
- Liczba prób: domyślnie 3 na scenariusz
- Tryby uruchomienia: `baseline` i `after-index`
- Obsługiwane silniki: PostgreSQL, MySQL, MongoDB, ScyllaDB
- Skrypt uruchomieniowy: `src/run_benchmarks.py`

## Scenariusze

| ID | CRUD | Opis | Oczekiwany efekt |
|---|---|---|---|
| C1 | Create | Create customer | `rows_affected=1` |
| C2 | Create | Create product | `rows_affected=1` |
| C3 | Create | Create order | `rows_affected=1` |
| C4 | Create | Create order item | `rows_affected=1` |
| R1 | Read | Read customer | `rows_affected=1` |
| R2 | Read | Read product | `rows_affected=1` |
| R3 | Read | Read order | `rows_affected=1` |
| R4 | Read | Read order items for order | `rows_affected>=0` |
| U1 | Update | Update customer city | `rows_affected=1` |
| U2 | Update | Update product price | `rows_affected=1` |
| U3 | Update | Update order promotion | `rows_affected=1` |
| U4 | Update | Update shipment status | `rows_affected=1` |
| D1 | Delete | Delete order item | `rows_affected=1` |
| D2 | Delete | Delete payment | `rows_affected=1` |
| D3 | Delete | Delete shipment | `rows_affected=1` |
| D4 | Delete | Delete order cascade | `rows_affected=1` |

## Format wyników

Wyniki są zapisywane do `data/results/benchmark_<run_id>.csv` i `data/results/benchmark_<run_id>.json`.
Plik EXPLAIN jest zapisywany do `data/results/explain_<run_id>.json`.

Kluczowe pola:

- `run_id`
- `db_engine`
- `scenario_id`
- `operation`
- `trial_no`
- `latency_ms`
- `rows_affected`
- `data_size_label`
- `mode`
- `timestamp_utc`

## Kolejne kroki

1. Rozszerzyć scenariusze do Layer 2 i Layer 3.
2. Dodać porównanie baseline vs after-index do raportu.
3. Dodać formalną analizę hipotezy badawczej na podstawie wyników.
