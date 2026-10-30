# =============================================================================
# Persona 3 - CP-09 RESILIENT (Kafka) - PowerShell
# Recuperacion ante caida de un consumidor: apaga inventory-consumer, envia 5
# pedidos (la API sigue en 202; billing y notification siguen), verifica que
# inventory acumula LAG y que al reiniciarlo consume desde el log (LAG 0).
# Requisitos: entorno Kafka levantado y RabbitMQ apagado (ambos usan el puerto 3000).
# Uso (desde la carpeta kafka):
#   powershell -ExecutionPolicy Bypass -File .\scripts\cp09-resilient.ps1
# =============================================================================
param([string]$Url = "http://localhost:3000/orders")
$ErrorActionPreference = "Continue"; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location (Split-Path -Parent $PSScriptRoot)
New-Item -ItemType Directory -Force -Path ".\evidencias" | Out-Null
$logFile = ".\evidencias\cp09-resilient-kafka-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"
Start-Transcript -Path $logFile | Out-Null
function Run($c) { Write-Host "> $c" -ForegroundColor Cyan; cmd /c "$c 2>&1" | ForEach-Object { Write-Host $_ } }
function Dg($g) { Run "docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $g" }
function Post($n) {
  $b = @{ customerName = $n; product = "Libro de arquitectura"; quantity = 1; unitPrice = 85000 } | ConvertTo-Json -Compress
  try { $r = Invoke-WebRequest -Method Post -Uri $Url -ContentType "application/json" -Body ([Text.Encoding]::UTF8.GetBytes($b)) -UseBasicParsing -TimeoutSec 60
        Write-Host ("HTTP {0}" -f [int]$r.StatusCode) } catch { Write-Host ("HTTP {0}" -f [int]$_.Exception.Response.StatusCode) }
}

Write-Host "########## CP-09 RESILIENT (Kafka) - $(Get-Date -Format 'dd/MM/yyyy HH:mm:ss') ##########"
Write-Host "=== PASO 0: lag inicial de inventory-service-group ==="; Dg "inventory-service-group"
Write-Host "=== PASO 1: APAGAR inventory-consumer (simular caida) ==="; Run "docker compose stop inventory-consumer"; Run "docker compose ps"
Write-Host "=== PASO 2: enviar 5 pedidos con inventory CAIDO (la API debe responder 202) ==="
for ($i = 1; $i -le 5; $i++) { Write-Host "Pedido $i ->" -NoNewline; Post "CP09K Cliente $i" }
Start-Sleep 3
Write-Host "=== PASO 3: inventory acumula LAG; billing sigue activo (LAG 0) ==="
Dg "inventory-service-group"; Dg "billing-service-group"
Write-Host "=== PASO 4: REINICIAR inventory-consumer (recuperacion) ==="; Run "docker compose start inventory-consumer"; Start-Sleep 22
Write-Host "=== PASO 5: lag de inventory tras recuperacion (debe ser LAG 0) ==="; Dg "inventory-service-group"
Write-Host "=== PASO 6: inventory consumió los 5 eventos CP09K desde el log ==="
Run "docker compose logs --no-color inventory-consumer | findstr Contenido | findstr CP09K"
Write-Host "=== PASO 7: GROUP_JOIN tras reconexion (reasignacion de particiones) ==="
Run "docker compose logs --no-color inventory-consumer | findstr ""Unido al grupo"""
Write-Host "=== FIN CP-09 Kafka.  Log: $logFile ==="
Stop-Transcript | Out-Null
