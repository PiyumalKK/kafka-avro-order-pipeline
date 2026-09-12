# Wipe all Kafka data and start clean. Destroys every message in every topic.
$ErrorActionPreference = "Stop"
Push-Location (Split-Path $PSScriptRoot -Parent)

Write-Host "Tearing down the stack and deleting volumes..." -ForegroundColor Yellow
docker compose down -v

Write-Host "Bringing it back up..." -ForegroundColor Cyan
& $PSScriptRoot\up.ps1

Pop-Location
