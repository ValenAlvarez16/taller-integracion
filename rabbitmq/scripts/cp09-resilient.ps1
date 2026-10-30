# =============================================================================
# Persona 3 - CP-09 RESILIENT (RabbitMQ) - PowerShell
# Recuperacion ante caida del consumidor: apaga el worker, envia 5 pedidos (la
# API sigue en 202), verifica la acumulacion en cola y el procesamiento sin
# perdida al reiniciar el worker.
# Requisitos: entorno RabbitMQ levantado y Kafka apagado (ambos usan el puerto 3000).
# Uso (desde la carpeta rabbitmq):
#   powershell -ExecutionPolicy Bypass -File .\scripts\cp09-resilient.ps1
# =============================================================================
param([string]$Url = "http://localhost:3000/orders")
$ErrorActionPreference = "Continue"; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location (Split-Path -Parent $PSScriptRoot)
New-Item -ItemType Directory -Force -Path ".\evidencias" | Out-Null
$logFile = ".\evidencias\cp09-resilient-rabbitmq-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"
Start-Transcript -Path $logFile | Out-Null
function Run($c) { Write-Host "> $c" -ForegroundColor Cyan; cmd /c "$c 2>&1" | ForEach-Object { Write-Host $_ } }
function Cola { Run "docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged" }
function Post($n) {
  $b = @{ customerName = $n; product = "Libro de arquitectura"; quantity = 1 } | ConvertTo-Json -Compress
  try { $r = Invoke-WebRequest -Method Post -Uri $Url -ContentType "application/json" -Body ([Text.Encoding]::UTF8.GetBytes($b)) -UseBasicParsing -TimeoutSec 60
        Write-Host ("HTTP {0}  {1}" -f [int]$r.StatusCode, $r.Content) }
  catch { Write-Host ("HTTP {0}  {1}" -f [int]$_.Exception.Response.StatusCode, $_.ErrorDetails.Message) }
}

Write-Host "########## CP-09 RESILIENT (RabbitMQ) - $(Get-Date -Format 'dd/MM/yyyy HH:mm:ss') ##########"
Write-Host "=== PASO 0: estado inicial ==="; Run "docker compose ps"; Cola
Write-Host "=== PASO 1: APAGAR el consumidor (simular caida) ==="; Run "docker compose stop notification-worker"; Run "docker compose ps"
Write-Host "=== PASO 2: enviar 5 pedidos con el worker CAIDO (la API debe responder 202) ==="
for ($i = 1; $i -le 5; $i++) { Write-Host "Pedido $i ->" -NoNewline; Post "CP09 Cliente $i" }
Write-Host "=== PASO 3: cola con worker caido (READY=5, UNACKED=0) ==="; Start-Sleep 2; Cola
Write-Host "=== PASO 4: REINICIAR el consumidor (recuperacion) ==="; Run "docker compose start notification-worker"
Write-Host "=== PASO 5: esperar procesamiento (5 x 3s + margen) ==="; Start-Sleep 20
Write-Host "=== PASO 6: cola final (debe quedar 0 -> 0 perdida) ==="; Cola
Write-Host "=== PASO 7: el worker procesó los pedidos CP09 (notificacion + ACK) ==="
Run "docker compose logs --no-color notification-worker | findstr Simulando | findstr CP09"
Run "docker compose logs --no-color notification-worker | findstr ACK"
Write-Host "=== FIN CP-09 RabbitMQ.  Log: $logFile ==="
Stop-Transcript | Out-Null
