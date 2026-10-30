# =============================================================================
# Persona 3 - CP-10 ELASTIC (Kafka) - PowerShell
# Escalado de consumidores del MISMO grupo: escala inventory-consumer a 2
# instancias (Kafka asigna una particion a cada una, CONSUMER-ID distinto), envia
# 6 pedidos y evidencia el reparto por particion con la vista del broker.
# Requisitos: entorno Kafka levantado y RabbitMQ apagado.
# Uso (desde la carpeta kafka):
#   powershell -ExecutionPolicy Bypass -File .\scripts\cp10-elastic.ps1
# =============================================================================
param([string]$Url = "http://localhost:3000/orders")
$ErrorActionPreference = "Continue"; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location (Split-Path -Parent $PSScriptRoot)
New-Item -ItemType Directory -Force -Path ".\evidencias" | Out-Null
$logFile = ".\evidencias\cp10-elastic-kafka-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"
Start-Transcript -Path $logFile | Out-Null
function Run($c) { Write-Host "> $c" -ForegroundColor Cyan; cmd /c "$c 2>&1" | ForEach-Object { Write-Host $_ } }
function Dg($g) { Run "docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $g" }
function Post($n) {
  $b = @{ customerName = $n; product = "Libro de arquitectura"; quantity = 1; unitPrice = 85000 } | ConvertTo-Json -Compress
  try { $r = Invoke-WebRequest -Method Post -Uri $Url -ContentType "application/json" -Body ([Text.Encoding]::UTF8.GetBytes($b)) -UseBasicParsing -TimeoutSec 60
        Write-Host ("HTTP {0}" -f [int]$r.StatusCode) } catch { Write-Host ("HTTP {0}" -f [int]$_.Exception.Response.StatusCode) }
}

Write-Host "########## CP-10 ELASTIC (Kafka) - $(Get-Date -Format 'dd/MM/yyyy HH:mm:ss') ##########"
Write-Host "=== PASO 1: ESCALAR inventory-consumer a 2 instancias (mismo grupo) ==="
Run "docker compose up -d --scale inventory-consumer=2"; Start-Sleep 15; Run "docker compose ps"
Write-Host "=== PASO 2: reparto de PARTICIONES (cada una a un CONSUMER-ID distinto) ==="; Dg "inventory-service-group"
Write-Host "=== PASO 3: enviar 6 pedidos (key=orderId -> se distribuyen entre las 2 particiones) ==="
for ($i = 1; $i -le 6; $i++) { Write-Host "Pedido $i ->" -NoNewline; Post "CP10K Cliente $i" }
Start-Sleep 18
Write-Host "=== PASO 4: total de eventos CP10K consumidos por el grupo (debe ser 6) ==="
Run "docker compose logs --no-color inventory-consumer | findstr Contenido | findstr CP10K | find /c ""CP10K"""
Write-Host "    El reparto por particion se evidencia en la vista del broker (PASO 2 y PASO 5)."
Write-Host "=== PASO 5: lag final del grupo y offsets por particion (LAG 0) ==="; Dg "inventory-service-group"
Write-Host "=== PASO 6: volver a 1 instancia (entorno limpio) ==="
Run "docker compose up -d --scale inventory-consumer=1"; Start-Sleep 5; Run "docker compose ps"
Write-Host "=== FIN CP-10 Kafka.  Log: $logFile ==="
Stop-Transcript | Out-Null
