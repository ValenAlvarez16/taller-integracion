# =============================================================================
# Persona 3 - CP-10 ELASTIC (RabbitMQ) - PowerShell
# Escalado de consumidores y reparto de carga: escala el worker a 2 instancias,
# envia 10 pedidos y muestra como RabbitMQ reparte los mensajes (round-robin por
# prefetch=1) entre las dos instancias.
# Requisitos: entorno RabbitMQ levantado y Kafka apagado.
# Uso (desde la carpeta rabbitmq):
#   powershell -ExecutionPolicy Bypass -File .\scripts\cp10-elastic.ps1
# =============================================================================
param([string]$Url = "http://localhost:3000/orders")
$ErrorActionPreference = "Continue"; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location (Split-Path -Parent $PSScriptRoot)
New-Item -ItemType Directory -Force -Path ".\evidencias" | Out-Null
$logFile = ".\evidencias\cp10-elastic-rabbitmq-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"
Start-Transcript -Path $logFile | Out-Null
function Run($c) { Write-Host "> $c" -ForegroundColor Cyan; cmd /c "$c 2>&1" | ForEach-Object { Write-Host $_ } }
function Cola { Run "docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged" }
function Post($n) {
  $b = @{ customerName = $n; product = "Libro de arquitectura"; quantity = 1 } | ConvertTo-Json -Compress
  try { $r = Invoke-WebRequest -Method Post -Uri $Url -ContentType "application/json" -Body ([Text.Encoding]::UTF8.GetBytes($b)) -UseBasicParsing -TimeoutSec 60
        Write-Host ("HTTP {0}" -f [int]$r.StatusCode) } catch { Write-Host ("HTTP {0}" -f [int]$_.Exception.Response.StatusCode) }
}

Write-Host "########## CP-10 ELASTIC (RabbitMQ) - $(Get-Date -Format 'dd/MM/yyyy HH:mm:ss') ##########"
Write-Host "=== PASO 1: ESCALAR notification-worker a 2 instancias ==="
Run "docker compose up -d --scale notification-worker=2"; Start-Sleep 6; Run "docker compose ps"
Write-Host "=== PASO 2: identificar las 2 instancias (WORKER_ID = hostname) ==="
Run "docker compose logs --no-color notification-worker | findstr Escuchando"
Write-Host "=== PASO 3: enviar 10 pedidos para repartir entre las 2 instancias ==="
for ($i = 1; $i -le 10; $i++) { Write-Host "Pedido $i ->" -NoNewline; Post "CP10 Cliente $i" }
Write-Host "=== PASO 4: esperar procesamiento (10 / 2 workers x 3s + margen) ==="; Start-Sleep 22; Cola
Write-Host "=== PASO 5: REPARTO DE CARGA - pedidos CP10 por instancia ==="
Run "docker compose logs --no-color notification-worker | findstr Simulando | findstr CP10"
$ids = (cmd /c "docker compose logs --no-color notification-worker 2>&1" |
        Select-String -Pattern "notification-worker ([0-9a-f]{12})" -AllMatches |
        ForEach-Object { $_.Matches } | ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique)
foreach ($id in $ids) {
  $n = (cmd /c "docker compose logs --no-color notification-worker 2>&1" |
        Select-String -SimpleMatch $id | Select-String -SimpleMatch "CP10" | Select-String -SimpleMatch "Simulando").Count
  Write-Host "Instancia $id -> procesó $n pedidos CP10"
}
Write-Host "=== PASO 6: volver a 1 instancia (entorno limpio) ==="
Run "docker compose up -d --scale notification-worker=1"; Start-Sleep 3; Run "docker compose ps"
Write-Host "=== FIN CP-10 RabbitMQ.  Log: $logFile ==="
Stop-Transcript | Out-Null
