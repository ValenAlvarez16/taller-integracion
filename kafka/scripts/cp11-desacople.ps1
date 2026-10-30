# =============================================================================
# Persona 3 - CP-11 DESACOPLAMIENTO TEMPORAL (Kafka) - PowerShell
# Los eventos publicados con el consumidor apagado se procesan al iniciarlo:
# apaga inventory-consumer, publica 3 eventos (el log de Kafka los conserva),
# espera, inicia el consumidor y compara occurredAt vs timestamp de procesamiento.
# Requisitos: entorno Kafka levantado y RabbitMQ apagado.
# Uso (desde la carpeta kafka):
#   powershell -ExecutionPolicy Bypass -File .\scripts\cp11-desacople.ps1
# =============================================================================
param([string]$Url = "http://localhost:3000/orders")
$ErrorActionPreference = "Continue"; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location (Split-Path -Parent $PSScriptRoot)
New-Item -ItemType Directory -Force -Path ".\evidencias" | Out-Null
$logFile = ".\evidencias\cp11-desacople-kafka-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"
Start-Transcript -Path $logFile | Out-Null
$Topic = "orders.events"
function Run($c) { Write-Host "> $c" -ForegroundColor Cyan; cmd /c "$c 2>&1" | ForEach-Object { Write-Host $_ } }
function Dg($g) { Run "docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $g" }
function Offsets { Run "docker compose exec -T kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $Topic" }
function Post($n) {
  $b = @{ customerName = $n; product = "Libro de arquitectura"; quantity = 1; unitPrice = 85000 } | ConvertTo-Json -Compress
  try { $r = Invoke-WebRequest -Method Post -Uri $Url -ContentType "application/json" -Body ([Text.Encoding]::UTF8.GetBytes($b)) -UseBasicParsing -TimeoutSec 60
        Write-Host ("HTTP {0}  {1}" -f [int]$r.StatusCode, $r.Content) } catch { Write-Host ("HTTP {0}" -f [int]$_.Exception.Response.StatusCode) }
}

Write-Host "########## CP-11 DESACOPLAMIENTO TEMPORAL (Kafka) - $(Get-Date -Format 'dd/MM/yyyy HH:mm:ss') ##########"
Write-Host "=== PASO 1: APAGAR inventory-consumer ==="; Run "docker compose stop inventory-consumer"
Write-Host "=== PASO 2: offset final ANTES de publicar ==="; Offsets
Write-Host ("=== PASO 3: publicar 3 eventos. HORA DE PUBLICACION: {0} ===" -f (Get-Date -Format "HH:mm:ss"))
for ($i = 1; $i -le 3; $i++) { Write-Host "Pedido $i ->" -NoNewline; Post "CP11K Cliente $i" }
Start-Sleep 2
Write-Host "=== PASO 4: offset DESPUES de publicar (el log crecio; eventos persistidos) ==="; Offsets; Dg "inventory-service-group"
Write-Host "=== PASO 5: ESPERA DELIBERADA de 12s con el consumidor apagado ==="; Start-Sleep 12
Write-Host ("=== PASO 6: INICIAR inventory-consumer. HORA DE INICIO: {0} ===" -f (Get-Date -Format "HH:mm:ss"))
Run "docker compose start inventory-consumer"; Start-Sleep 20
Write-Host "=== PASO 7: lag final (LAG 0) y procesamiento diferido (occurredAt vs log) ==="; Dg "inventory-service-group"
Run "docker compose logs --no-color inventory-consumer | findstr Contenido | findstr CP11K"
Write-Host "=== FIN CP-11 Kafka.  Log: $logFile ==="
Stop-Transcript | Out-Null
