# Opis aplikacji testowej (database-performance-benchmarking)

## Cel aplikacji
Aplikacja testowa służy do porównania wydajności operacji CRUD pomiędzy czterema systemami baz danych:
- relacyjnymi: PostgreSQL, MySQL,
- nierelacyjnymi: MongoDB (dokumentowa), ScyllaDB (Cassandra-kompatybilna).

Wspólną bazą danych do importu jest zestaw CSV „Retail Data Warehouse” (12 tabel) pobierany automatycznie z Kaggle. Właściwe testy CRUD są wykonywane na pomocniczej tabeli/kolekcji `benchmark_ops`, aby nie modyfikować danych retail.

## Technologie i narzędzia
- Python 3.11/3.12 + biblioteki z [requirements.txt](../requirements.txt) (m.in. `psycopg`, `mysql-connector-python`, `pymongo`, `cassandra-driver`, `pandas`, `matplotlib`).
- Docker Compose: uruchomienie usług bazodanowych z [docker-compose.yml](../docker-compose.yml) i konfiguracją z [.env](../.env) / [.env.example](../.env.example).
- KaggleHub: automatyczny download datasetu (wymaga skonfigurowanych poświadczeń Kaggle, jeśli dataset tego wymaga).

## Jak działa aplikacja (przepływ end-to-end)
1) Uruchomienie środowiska baz danych (kontenery) przez Docker Compose.
2) Weryfikacja łączności do wszystkich silników przez skrypt kontrolny.
3) Import danych retail do każdego silnika (osobne importery, uruchamiane sekwencyjnie).
4) Uruchomienie benchmarku CRUD (24 scenariusze, minimum 3 próby na scenariusz) w trybie `baseline` lub `after-index`.
5) Zapis wyników do CSV/JSON + zapis próbek planów zapytań EXPLAIN.
6) Generowanie wykresów oraz porównań baseline vs after-index.

## Automatyzacje zastosowane w projekcie
- Automatyczne uruchomienie 4 silników DB: [docker-compose.yml](../docker-compose.yml) (kontenery, porty, wolumeny, healthchecki).
- Automatyczne pobieranie datasetu: funkcje w [src/retail_import_common.py](../src/retail_import_common.py) używają `kagglehub` do pobrania CSV.
- Automatyczne „odkrywanie” schematu z CSV:
  - normalizacja nazw tabel/kolumn,
  - zgadywanie klucza głównego i kluczy obcych na podstawie nazw (`*_id`),
  - zgadywanie typów kolumn (heurystyki) – w [src/retail_import_common.py](../src/retail_import_common.py).
- Automatyczne zwiększanie wolumenu danych: importery potrafią rozbudować `orders` do zadanego celu (domyślnie 9 000 000 wierszy/dokumentów) przez duplikację rekordów.
- Automatyczna denormalizacja (relacyjne → dokumenty/JSON):
  - budowa dokumentów encji bazowych (customers/stores/products),
  - materializacja relacji `orders`–`order_items` (i opcjonalnie payments/shipments) do zagnieżdżonych struktur,
  - użycie tymczasowego cache SQLite do wydajnego „joinowania” `order_items` po `order_id`,
  - automatyczne sprzątanie cache po imporcie (z opcją zachowania) – w [src/retail_denormalize.py](../src/retail_denormalize.py) oraz importerach NoSQL.
- Automatyzacja testów CRUD:
  - jednolita lista scenariuszy (Create/Read/Update/Delete),
  - wielokrotne próby, pomiar czasu, zapis wyników,
  - przełączanie trybu `baseline`/`after-index` (tworzenie/usuwanie indeksów „prefix/payload” tam, gdzie jest to wspierane) – w [src/run_benchmarks.py](../src/run_benchmarks.py).
- Automatyczne generowanie artefaktów wynikowych: wykresy per-scenariusz i per-operacja oraz porównanie baseline vs after-index – w [src/plot_results.py](../src/plot_results.py).

## Krótki opis plików i katalogów

### Pliki w katalogu głównym
- [README.md](../README.md) – szybki start: Docker, import, benchmark, wykresy.
- [WYMAGANIA.md](../WYMAGANIA.md) – wymagania do sprawozdania (kryteria ocen).
- [docker-compose.yml](../docker-compose.yml) – definicje usług: Postgres, MySQL, Mongo, Scylla.
- [.env.example](../.env.example) / [.env](../.env) – konfiguracja portów, użytkowników i haseł.
- [requirements.txt](../requirements.txt) – zależności Pythona.

### Skrypty w src/
- [src/check_connections.py](../src/check_connections.py) – szybki test połączeń do wszystkich 4 baz.
- [src/import_data.py](../src/import_data.py) – „pipeline” importu: uruchamia po kolei importery Postgres/MySQL/Mongo/Scylla z tymi samymi parametrami.
- [src/import_retail_postgres.py](../src/import_retail_postgres.py) – import CSV do PostgreSQL (DDL z nagłówków, szybki COPY, FK best-effort, ekspansja `orders`).
- [src/import_retail_mysql.py](../src/import_retail_mysql.py) – import CSV do MySQL (DDL, chunk insert przez pandas, FK best-effort, ekspansja `orders`).
- [src/import_retail_mongo.py](../src/import_retail_mongo.py) – import do MongoDB w trybie `tables` (1 CSV → 1 kolekcja) lub `denormalized` (dokumenty z embedowaniem).
- [src/import_retail_scylla.py](../src/import_retail_scylla.py) – import do ScyllaDB w trybie `tables` (1 CSV → tabela `retail_<name>` z `(id,data JSON)`) lub `denormalized` (tabele `retail_customers|stores|products|inventory|orders`).
- [src/retail_import_common.py](../src/retail_import_common.py) – wspólne utilsy: download datasetu, odkrywanie tabel, heurystyki PK/FK i typów, normalizacja nazw.
- [src/retail_denormalize.py](../src/retail_denormalize.py) – logika denormalizacji: budowa dokumentów, cache SQLite dla „joinów”, stabilne identyfikatory.
- [src/run_benchmarks.py](../src/run_benchmarks.py) – uruchomienie benchmarku CRUD (24 scenariusze), zapis wyników CSV/JSON i próbek EXPLAIN.
- [src/plot_results.py](../src/plot_results.py) – generowanie wykresów i porównań na podstawie CSV.

### Dokumentacja i artefakty
- [docs/environment-setup.md](environment-setup.md) – szczegółowa instrukcja uruchomienia środowiska.
- [docs/benchmark-scenarios.md](benchmark-scenarios.md) – definicje scenariuszy CRUD i format wyników.
- [docs/uml-relational.mmd](uml-relational.mmd), [docs/uml-mongodb.mmd](uml-mongodb.mmd), [docs/uml-scylladb.mmd](uml-scylladb.mmd) – diagramy UML (Mermaid) dla modeli danych.
- Katalog [data/](../data/) – wyniki i dane pomocnicze (np. CSV/JSON z benchmarku).
- Katalog [plots/](../plots/) – wygenerowane wykresy.
