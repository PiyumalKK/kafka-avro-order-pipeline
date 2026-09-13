# Runs the whole demonstration automatically, with on-screen captions.
#
#   1. Press Win + Shift + S, choose the video camera icon, select the screen, press Start.
#   2. Run this script.
#   3. Do nothing. It drives itself for about 6 minutes.
#
#   .\scripts\auto-demo.ps1              normal speed
#   .\scripts\auto-demo.ps1 -Fast        roughly half the pauses (for rehearsing)
#   .\scripts\auto-demo.ps1 -SkipReset   keep existing messages instead of wiping

param(
    [switch]$Fast,
    [switch]$SkipReset
)

# Deliberately NOT "Stop". Windows PowerShell 5.1 turns anything a native
# program writes to stderr into a terminating NativeCommandError, and docker
# reports ordinary progress ("Container kafka Stopping") on stderr. With
# "Stop" the demo dies on its own setup step. Failures are raised explicitly
# with throw instead.
$ErrorActionPreference = "Continue"
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root

$py = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { throw "Virtual environment not found. Run: python -m venv .venv" }

$scale = if ($Fast) { 0.5 } else { 1.0 }
function Wait-Beat([double]$seconds) { Start-Sleep -Milliseconds ([int]($seconds * $scale * 1000)) }

# ---------------------------------------------------------------- window layout
Add-Type -AssemblyName System.Windows.Forms
Add-Type @"
using System;
using System.Runtime.InteropServices;
public class Win {
    [DllImport("user32.dll")] public static extern bool MoveWindow(IntPtr h, int x, int y, int w, int t, bool r);
    [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr h);
}
"@

$screen = [System.Windows.Forms.Screen]::PrimaryScreen.WorkingArea
$halfWidth = [int]($screen.Width / 2)

function Move-ToLeft([IntPtr]$handle) {
    [void][Win]::MoveWindow($handle, $screen.X, $screen.Y, $halfWidth, $screen.Height, $true)
}
function Move-ToRight([IntPtr]$handle) {
    [void][Win]::MoveWindow($handle, $screen.X + $halfWidth, $screen.Y, $halfWidth, $screen.Height, $true)
}

# This window drives the demo and sits on the right.
$self = (Get-Process -Id $PID).MainWindowHandle
Move-ToRight $self

# ---------------------------------------------------------------- captions
function Show-Caption {
    param([string]$Number, [string]$Title, [string]$Detail)

    Write-Host ""
    Write-Host ("=" * 64) -ForegroundColor DarkCyan
    Write-Host "  $Number  $Title" -ForegroundColor Cyan
    if ($Detail) { Write-Host "  $Detail" -ForegroundColor Gray }
    Write-Host ("=" * 64) -ForegroundColor DarkCyan
    Write-Host ""
}

function Show-Command([string]$text) {
    Write-Host "  PS> " -ForegroundColor DarkGray -NoNewline
    Write-Host $text -ForegroundColor Yellow
    Write-Host ""
}

# ---------------------------------------------------------------- title card
Clear-Host
Write-Host ""
Write-Host "   ____________________________________________________" -ForegroundColor DarkCyan
Write-Host ""
Write-Host "     EC8202  BIG DATA ANALYTICS" -ForegroundColor White
Write-Host "     Kafka + Avro Order Pipeline" -ForegroundColor Cyan
Write-Host ""
Write-Host "     Real-time aggregation  |  Retry logic  |  Dead Letter Queue" -ForegroundColor Gray
Write-Host ""
Write-Host "     Piyumal Ranasinghe" -ForegroundColor White
Write-Host "   ____________________________________________________" -ForegroundColor DarkCyan
Write-Host ""
Wait-Beat 6

# ---------------------------------------------------------------- scene 1
Show-Caption "SCENE 1" "The infrastructure" "Kafka, Schema Registry and Kafka UI, running in Docker"
Show-Command "docker compose ps"
docker compose ps --format "table {{.Service}}`t{{.State}}`t{{.Status}}" 2>$null
Wait-Beat 6

# ---------------------------------------------------------------- reset
if (-not $SkipReset) {
    Show-Caption "SETUP" "Starting from a clean slate" "Deleting old test messages so the demo begins at zero"
    Show-Command "docker compose down -v ; docker compose up -d"
    docker compose down -v *>$null
    docker compose up -d *>$null

    # Kafka reports healthy well before the Schema Registry finishes starting,
    # and before the transaction coordinator has finished loading. Producing
    # too early fails with "Coordinator load in progress" and a Schema Registry
    # connection error, so wait for all three signals, not just the broker.
    Write-Host "  Waiting for Kafka to become healthy..." -ForegroundColor Gray
    $deadline = (Get-Date).AddMinutes(4)
    while ((Get-Date) -lt $deadline) {
        $state = docker inspect -f '{{.State.Health.Status}}' kafka 2>$null
        if ($state -eq "healthy") { break }
        Start-Sleep -Seconds 3
    }
    if ($state -ne "healthy") { throw "Kafka did not become healthy. Check Docker Desktop." }
    Write-Host "  Kafka is healthy." -ForegroundColor Green

    Write-Host "  Waiting for the Schema Registry..." -ForegroundColor Gray
    $registryReady = $false
    while ((Get-Date) -lt $deadline -and -not $registryReady) {
        try {
            $response = Invoke-WebRequest -Uri "http://localhost:8081/subjects" -UseBasicParsing -TimeoutSec 5
            if ($response.StatusCode -eq 200) { $registryReady = $true; break }
        } catch {
            Start-Sleep -Seconds 3
        }
    }
    if (-not $registryReady) { throw "Schema Registry never came up on http://localhost:8081" }
    Write-Host "  Schema Registry is ready." -ForegroundColor Green

    # Let the transaction coordinator settle before the idempotent producer
    # asks it for a producer id.
    Write-Host "  Letting the cluster settle..." -ForegroundColor Gray
    Start-Sleep -Seconds 8
    Write-Host ""

    Show-Command "python scripts\create_topics.py"
    & $py scripts\create_topics.py
    Wait-Beat 5
}

# ---------------------------------------------------------------- scene 2
Show-Caption "SCENE 2" "The message contract" "order.avsc - the Avro schema the assignment specified"
Show-Command "type schemas\order.avsc"
Get-Content schemas\order.avsc | Write-Host -ForegroundColor White
Write-Host ""
Write-Host "  Every message is encoded with this schema. The schema itself never" -ForegroundColor Gray
Write-Host "  travels - only a 4-byte id pointing at the Schema Registry." -ForegroundColor Gray
Wait-Beat 9

# ---------------------------------------------------------------- scene 3
Show-Caption "SCENE 3" "Starting the consumer" "It opens on the left. Watch the RUNNING AVERAGE."

$consumerArgs = "-NoExit -Command `"Set-Location '$root'; & '$py' -m src.consumer.main --max-messages 90 --transient-fault-rate 0.35`""
$consumer = Start-Process powershell -ArgumentList $consumerArgs -PassThru

# Give the window time to exist, then park it on the left.
$tries = 0
while ($consumer.MainWindowHandle -eq 0 -and $tries -lt 40) {
    Start-Sleep -Milliseconds 250
    $consumer.Refresh()
    $tries++
}
if ($consumer.MainWindowHandle -ne 0) { Move-ToLeft $consumer.MainWindowHandle }
[void][Win]::SetForegroundWindow($self)

Write-Host "  The consumer is waiting. Nothing has arrived yet, so the average is 0.00." -ForegroundColor Gray
Wait-Beat 7

# ---------------------------------------------------------------- scene 4
Show-Caption "SCENE 4" "Sending orders" "REQUIREMENT 1 of 3 - real-time aggregation"
Write-Host "  The running average on the left recalculates on EVERY message." -ForegroundColor Green
Write-Host "  It is never computed at the end - there is no end to a stream." -ForegroundColor Gray
Write-Host ""
Show-Command "python -m src.producer.main --count 90 --interval 0.35"
& $py -m src.producer.main --count 90 --interval 0.35 --permanent-fault-rate 0.15
Wait-Beat 4

# ---------------------------------------------------------------- scene 5
Show-Caption "SCENE 5" "Retry logic" "REQUIREMENT 2 of 3 - look at the YELLOW lines on the left"
Write-Host "  The inventory service fails sometimes. That is a TEMPORARY fault," -ForegroundColor Gray
Write-Host "  so the consumer waits and tries again - up to 3 attempts." -ForegroundColor Gray
Write-Host ""
Write-Host "  Each wait is longer than the last:  0.4s  ->  0.8s  ->  1.6s" -ForegroundColor White
Write-Host "  Plus a small random amount, so that many consumers do not all" -ForegroundColor Gray
Write-Host "  retry at the same instant and overwhelm a recovering service." -ForegroundColor Gray
Write-Host ""
Write-Host "  Look for 'recovered by retry' in the table - those succeeded" -ForegroundColor Green
Write-Host "  on a later attempt and were counted normally." -ForegroundColor Green
Wait-Beat 13

# ---------------------------------------------------------------- scene 6
Show-Caption "SCENE 6" "Dead Letter Queue" "REQUIREMENT 3 of 3 - look at the RED lines on the left"
Write-Host "  Some orders are permanently broken - a negative price, or an" -ForegroundColor Gray
Write-Host "  empty product name. Retrying can NEVER fix those." -ForegroundColor Gray
Write-Host ""
Write-Host "  Retrying them forever would block every message behind them." -ForegroundColor White
Write-Host "  That is called a poison pill." -ForegroundColor White
Write-Host ""
Write-Host "  So they are moved aside into the Dead Letter Queue, and the" -ForegroundColor Green
Write-Host "  pipeline keeps running." -ForegroundColor Green
Wait-Beat 13

# ---------------------------------------------------------------- scene 7
Show-Caption "SCENE 7" "Reading the Dead Letter Queue back" "Every failure kept its reason"
Show-Command "python scripts\inspect_dlq.py"
& $py scripts\inspect_dlq.py --timeout 6
Write-Host ""
Write-Host "  Each dead letter carries its original topic, partition and offset," -ForegroundColor Gray
Write-Host "  the error, and how many attempts were made - so it can be" -ForegroundColor Gray
Write-Host "  diagnosed and replayed later." -ForegroundColor Gray
Wait-Beat 11

# ---------------------------------------------------------------- scene 8
Show-Caption "SCENE 8" "The tests" "32 tests covering all three requirements"
Show-Command "python -m pytest"
& $py -m pytest -q
Wait-Beat 8

# ---------------------------------------------------------------- closing
Show-Caption "SUMMARY" "All three requirements demonstrated" ""
Write-Host "   [1]  Real-time aggregation" -ForegroundColor Green
Write-Host "        Running average recalculated on every message (Welford's algorithm)" -ForegroundColor Gray
Write-Host ""
Write-Host "   [2]  Retry logic" -ForegroundColor Green
Write-Host "        Exponential backoff with jitter, for temporary failures only" -ForegroundColor Gray
Write-Host ""
Write-Host "   [3]  Dead Letter Queue" -ForegroundColor Green
Write-Host "        Permanent failures parked with full diagnostic headers" -ForegroundColor Gray
Write-Host ""
Write-Host "   Messages are Avro-encoded against a registered schema." -ForegroundColor White
Write-Host "   Offsets are committed only after a message is fully handled," -ForegroundColor White
Write-Host "   giving at-least-once delivery with no silent data loss." -ForegroundColor White
Write-Host ""
Write-Host "   github.com/PiyumalKK/kafka-avro-order-pipeline" -ForegroundColor Cyan
Write-Host ""
Write-Host ("=" * 64) -ForegroundColor DarkCyan
Write-Host ""
Write-Host "   Demo complete. You can stop the recording now." -ForegroundColor Yellow
Write-Host ""
