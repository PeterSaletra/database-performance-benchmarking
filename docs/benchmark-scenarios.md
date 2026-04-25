# Benchmark Scenarios (MVP)

Ten dokument definiuje pierwszy zestaw scenariuszy CRUD dla benchmarku MVP.
Scenariusze są wykonywane na pomocniczej tabeli/kolekcji `benchmark_ops`, aby nie modyfikować danych retail.

## Parametry MVP

- Liczba scenariuszy: 24
- Struktura: 6x Create, 6x Read, 6x Update, 6x Delete
- Liczba prób: domyślnie 3 na scenariusz
- Tryby uruchomienia: `baseline` (bez indeksów payload) i `after-index` (z indeksami payload, jeśli wspierane)
- Obsługiwane silniki: PostgreSQL, MySQL, MongoDB, ScyllaDB
- Skrypt uruchomieniowy: `src/run_benchmarks.py`

## Scenariusze

| ID | CRUD | Opis | Oczekiwany efekt |
|---|---|---|---|
| C1 | Create | Insert pojedynczego rekordu | `rows_affected=1` |
| C2 | Create | Insert 10 rekordów | `rows_affected=10` |
| C3 | Create | Insert 100 rekordów | `rows_affected=100` |
| C4 | Create | Insert 250 rekordów | `rows_affected=250` |
| C5 | Create | Insert pojedynczego rekordu z markerem | `rows_affected=1` |
| C6 | Create | Insert 50 rekordów z markerem | `rows_affected=50` |
| R1 | Read | Odczyt rekordu po istniejącym ID | `rows_affected=1` lub `0/1` |
| R2 | Read | Odczyt 10 najnowszych rekordów | `rows_affected<=10` |
| R3 | Read | Odczyt po wzorcu payload | `rows_affected<=20` |
| R4 | Read | Odczyt 100 najnowszych rekordów | `rows_affected<=100` |
| R5 | Read | Odczyt po wzorcu `seed_payload_anchor` | `rows_affected<=10` |
| R6 | Read | Odczyt po ID ostatniego markera | `rows_affected=1` lub `0/1` |
| U1 | Update | Aktualizacja rekordu po ID | `rows_affected>=0` |
| U2 | Update | Aktualizacja 10 rekordów (latest) | `rows_affected<=10` |
| U3 | Update | Aktualizacja rekordów po wzorcu payload | `rows_affected>=0` |
| U4 | Update | Aktualizacja drugiego rekordu seed po ID | `rows_affected>=0` |
| U5 | Update | Aktualizacja 50 rekordów (latest) | `rows_affected<=50` |
| U6 | Update | Aktualizacja po wzorcu `seed_payload_anchor` | `rows_affected>=0` |
| D1 | Delete | Usunięcie pojedynczego rekordu tymczasowego | `rows_affected=1` |
| D2 | Delete | Usunięcie 5 rekordów tymczasowych (latest) | `rows_affected<=5` |
| D3 | Delete | Usunięcie rekordów z markerem payload | `rows_affected<=5` |
| D4 | Delete | Usunięcie 20 rekordów tymczasowych (latest) | `rows_affected<=20` |
| D5 | Delete | Usunięcie pojedynczego rekordu markerowego po ID | `rows_affected=1` |
| D6 | Delete | Usunięcie 20 rekordów markerowych po wzorcu | `rows_affected<=20` |

## Format wyników

Wyniki są zapisywane do `data/results/benchmark_<run_id>.csv` i `data/results/benchmark_<run_id>.json`.
Plany zapytań EXPLAIN są zapisywane do `data/results/explain_<run_id>.json`.

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

## Kolejne kroki po MVP

1. Dodać tryby `before-index` i `after-index` oraz porównanie wyników.
2. Dodać pomiary równoległe (skalowalność) i p95.
3. Dodać formalną walidację hipotezy badawczej na podstawie wyników.
