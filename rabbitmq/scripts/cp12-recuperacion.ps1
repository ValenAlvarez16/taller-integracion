# =============================================================================
# Persona 3 - CP-12 RECUPERACION / 0 PERDIDA (RabbitMQ) - PowerShell
#   12.1 Reinicio del BROKER con mensajes persistentes: sobreviven (cola durable
#        + persistent=true) y se procesan al volver el worker.
#   12.2 Mensaje UNACKED: si el worker muere a mitad del procesamiento, RabbitMQ
#        reencola y REENTREGA (redelivered=true) al reiniciar.
# Requisitos: entorno RabbitMQ levantado y Kafka apagado.
# Uso (desde la carpeta rabbitmq):
#   powershell -ExecutionPolicy Bypass -File .\scripts\cp12-recuperacion.ps1
# =============================================================================
param([string]$Url = "http://localhost:3000/orders")
$ErrorActionPreference = "Continue"; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location (Split-Path -Parent $PSScriptRoot)
New-Item -ItemType Directory -Force -Path ".\evidencias" | Out-Null
$logFile = ".\evidencias\cp12-recuperacion-rabbitmq-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"
Start-Transcript -Path $logFile | Out-Null
function Run($c) { Write-Host "> $c" -ForegroundColor Cyan; cmd /c "$c 2>&1" | ForEach-Object { Write-Host $_ } }
function Cola { Run "docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged" }
function Post($n) {
  $b = @{ customerName = $n; product = "Libro de arquitectura"; quantity = 1 } | ConvertTo-Json -Compress
  try { $r = Invoke-WebRequest -Method Post -Uri $Url -ContentType "application/json" -Body ([Text.Encoding]::UTF8.GetBytes($b)) -UseBasicParsing -TimeoutSec 60; return ($r.Content | ConvertFrom-Json) }
  catch { Write-Host ("HTTP {0}" -f [int]$_.Exception.Response.StatusCode); return $null }
}

Write-Host "########## CP-12 RECUPERACION / 0 PERDIDA (RabbitMQ) - $(Get-Date -Format 'dd/MM/yyyy HH:mm:ss') ##########"
Write-Host "=== 12.1 REINICIO DEL BROKER con mensajes persistentes retenidos ==="
Run "docker compose stop notification-worker"
Write-Host "--- Publicar 4 pedidos (persistent=true, cola durable=true) ---"
for ($i = 1; $i -le 4; $i++) { Write-Host "Pedido $i ->" -NoNewline; Post "CP12 Cliente $i" | Out-Null; Write-Host " enviado" }
Start-Sleep 2
Write-Host "--- Cola ANTES del reinicio del broker ---"
Run "docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready durable"
Write-Host "--- REINICIAR el broker RabbitMQ ---"; Run "docker compose restart rabbitmq"; Start-Sleep 25; Run "docker compose ps"
Write-Host "--- Cola DESPUES del reinicio (los mensajes persistentes deben seguir ahi) ---"
Run "docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready durable"
Write-Host "--- Iniciar worker: procesa los 4 mensajes recuperados ---"; Run "docker compose start notification-worker"; Start-Sleep 18
Write-Host "--- Cola final (0 -> 0 perdida pese al reinicio del broker) ---"; Cola

Write-Host ""
Write-Host "=== 12.2 REENTREGA de mensaje UNACKED (worker muere a mitad de procesamiento) ==="
$r = Post "CP12 Unacked"; $oid = $r.orderId
Write-Host "    Pedido publicado orderId=$oid"; Start-Sleep 1
Write-Host "    Estado: el mensaje deberia estar UNACKED:"; Cola
Write-Host "--- MATAR el worker (kill) antes del ACK ---"; Run "docker compose kill notification-worker"; Start-Sleep 3
Write-Host "    Tras matar el worker, el mensaje UNACKED vuelve a READY:"; Cola
Write-Host "--- Reiniciar worker: REENTREGA (redelivered=true) y procesa ---"; Run "docker compose up -d notification-worker"; Start-Sleep 8
Write-Host "    Cola final (0 -> el mensaje no se perdio):"; Cola
Write-Host "    Evidencia de reentrega (redelivered=true) del pedido $oid:"
Run "docker compose logs --no-color notification-worker | findstr $oid"
Write-Host "=== FIN CP-12 RabbitMQ.  Log: $logFile ==="
Stop-Transcript | Out-Null
