# Bring the whole stack up and create topics. Idempotent.
$ErrorActionPreference = "Stop"
Push-Location (Split-Path $PSScriptRoot -Parent)

Write-Host "Starting Kafka, Schema Registry and Kafka UI..." -ForegroundColor Cyan
docker compose up -d

Write-Host "Waiting for the broker to report healthy..." -ForegroundColor Cyan
$deadline = (Get-Date).AddMinutes(3)
while ((Get-Date) -lt $deadline) {
    $state = docker inspect -f '{{.State.Health.Status}}' kafka 2>$null
    if ($state -eq "healthy") { break }
    Start-Sleep -Seconds 3
}
if ($state -ne "healthy") { throw "Kafka did not become healthy in time." }

Write-Host "Creating topics..." -ForegroundColor Cyan
& .\.venv\Scripts\python.exe scripts\create_topics.py

Write-Host "`nReady. Kafka UI: http://localhost:8080" -ForegroundColor Green
Pop-Location
