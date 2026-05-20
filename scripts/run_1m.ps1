$env:PG_DSN = 'dbname=benchmark_db user=benchmark_user password=benchmark_pass host=127.0.0.1'
$env:MYSQL_HOST = '127.0.0.1'
$env:MYSQL_USER = 'benchmark_user'
$env:MYSQL_PASSWORD = 'benchmark_pass'
$env:MYSQL_DB = 'benchmark_db'
$env:MONGO_URI = 'mongodb://benchmark_user:benchmark_pass@127.0.0.1:27017'
$env:SCYLLA_HOST = '127.0.0.1'

Write-Host 'Starting 1,000,000-row bulk insert benchmark. This may take a long time.'
python .\src\run_bulk_inserts.py --engines postgres mysql mongo scylla --rows 1000000 --batch-size 1000 --output-dir data/results --modes no_index with_index
Write-Host 'Benchmark finished (or exited early). Check data/results for JSON output.'
