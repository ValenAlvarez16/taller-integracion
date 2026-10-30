# =============================================================================
# Persona 3 - CP-11 DESACOPLAMIENTO TEMPORAL (RabbitMQ) - PowerShell
# Los mensajes publicados con el consumidor apagado se procesan al iniciarlo:
# publica 3 pedidos con el worker apagado, espera, lo inicia y compara la hora de
# publicacion (createdAt) con la de procesamiento (timestamp del log).
# Requisitos: entorno RabbitMQ levantado y Kafka apagado.
# Uso (desde la carpeta rabbitmq):
#   powershell -ExecutionPolicy Bypass -File .\scripts\cp11-desacople.ps1
# =============================================================================
param([string]$Url = "http://localhost:3000/orders")
$ErrorActionPreference = "Continue"; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location (Split-Path -Parent $PSScriptRoot)
New-Item -ItemType Directory -Force -Path ".\evidencias" | Out-Null
$logFile = ".\evidencias\cp11-desacople-rabbitmq-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"
Start-Transcript -Path $logFile | Out-Null
function Run($c) { Write-Host "> $c" -ForegroundColor Cyan; cmd /c "$c 2>&1" | ForEach-Object { Write-Host $_ } }
function Cola { Run "docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged" }
function Post($n) {
  $b = @{ customerName = $n; product = "Libro de arquitectura"; quantity = 1 } | ConvertTo-Json -Compress
  try { $r = Invoke-WebRequest -Method Post -Uri $Url -ContentType "application/json" -Body ([Text.Encoding]::UTF8.GetBytes($b)) -UseBasicParsing -TimeoutSec 60
        Write-Host ("HTTP {0}  {1}" -f [int]$r.StatusCode, $r.Content) } catch { Write-Host ("HTTP {0}" -f [int]$_.Exception.Response.StatusCode) }
}

Write-Host "########## CP-11 DESACOPLAMIENTO TEMPORAL (RabbitMQ) - $(Get-Date -Format 'dd/MM/yyyy HH:mm:ss') ##########"
Write-Host "=== PASO 1: APAGAR el consumidor ==="; Run "docker compose stop notification-worker"; Run "docker compose ps"
Write-Host ("=== PASO 2: publicar 3 pedidos. HORA DE PUBLICACION: {0} ===" -f (Get-Date -Format "HH:mm:ss"))
for ($i = 1; $i -le 3; $i++) { Write-Host "Pedido $i ->" -NoNewline; Post "CP11 Cliente $i" }
Write-Host "=== PASO 3: cola con consumidor apagado (READY=3) ==="; Start-Sleep 2; Cola
Write-Host "=== PASO 4: ESPERA DELIBERADA de 10s con los mensajes retenidos ==="; Start-Sleep 10; Cola
Write-Host ("=== PASO 5: INICIAR el consumidor. HORA DE INICIO: {0} ===" -f (Get-Date -Format "HH:mm:ss"))
Run "docker compose start notification-worker"; Start-Sleep 15
Write-Host "=== PASO 6: cola final (debe quedar 0) ==="; Cola
Write-Host "=== PASO 7: comparar createdAt (publicacion) vs timestamp del log (procesamiento) ==="
Run "docker compose logs --no-color notification-worker | findstr CP11 | findstr Contenido"
Write-Host "=== FIN CP-11 RabbitMQ.  Log: $logFile ==="
Stop-Transcript | Out-Null
