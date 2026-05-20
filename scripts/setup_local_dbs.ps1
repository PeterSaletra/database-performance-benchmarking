<#
Create .env with default credentials and start docker-compose services.

Usage (PowerShell):
  .\scripts\setup_local_dbs.ps1

This will create a `.env` file in the repo root (if not present) with sensible defaults
and run `docker compose up -d` to start Postgres, MySQL, Mongo and Scylla containers.

Edit the generated .env if you need different passwords or ports.
#>

# Determine repository root (scripts folder's parent)
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = Split-Path -Parent $scriptDir
Set-Location $repo

$envPath = Join-Path $repo '.env'
if (-Not (Test-Path $envPath)) {
    Write-Host "Creating .env with default credentials at $envPath"
    @"
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_PORT=5432

MYSQL_DATABASE=test
MYSQL_USER=root
MYSQL_PASSWORD=root
MYSQL_ROOT_PASSWORD=root
MYSQL_PORT=3306

MONGO_INITDB_ROOT_USERNAME=root
MONGO_INITDB_ROOT_PASSWORD=root
MONGO_PORT=27017

SCYLLA_PORT=9042
"@ | Out-File -FilePath $envPath -Encoding utf8
} else {
  Write-Host ".env already exists at $envPath - leaving it unchanged"
}

Write-Host 'Starting containers with docker compose...'
docker compose up -d

Write-Host 'Containers started. Use ''docker compose ps'' to check status and ''docker logs <service>'' for logs.'
Write-Host 'Recommended next steps: set environment variables in this PowerShell session for the benchmark script:'
Write-Host '  $env:PG_DSN = ''dbname=postgres user=postgres password=postgres host=127.0.0.1'''
Write-Host '  $env:MYSQL_HOST = ''127.0.0.1'''
Write-Host '  $env:MYSQL_USER = ''root'''
Write-Host '  $env:MYSQL_PASSWORD = ''root'''
Write-Host '  $env:MYSQL_DB = ''test'''
Write-Host '  $env:MONGO_URI = ''mongodb://root:root@127.0.0.1:27017'''
Write-Host '  $env:SCYLLA_HOST = ''127.0.0.1'''
