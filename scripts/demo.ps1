# Runs a short, self-contained demo: producer then consumer, bounded message counts.
$ErrorActionPreference = "Stop"
Push-Location (Split-Path $PSScriptRoot -Parent)
$py = ".\.venv\Scripts\python.exe"

Write-Host "Publishing 60 orders (with deliberate bad data)..." -ForegroundColor Cyan
& $py -m src.producer.main --count 60 --interval 0.05

Write-Host "`nConsuming and aggregating..." -ForegroundColor Cyan
& $py -m src.consumer.main --max-messages 60 --plain

Write-Host "`nDead letter queue contents:" -ForegroundColor Yellow
& $py scripts\inspect_dlq.py

Pop-Location
