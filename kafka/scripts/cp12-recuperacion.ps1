# =============================================================================
# Persona 3 - CP-12 RECUPERACION / 0 PERDIDA (Kafka) - PowerShell
#   12.1 Reinicio del BROKER Kafka: offsets del log y commits de los 3 grupos se
#        conservan (volumen kafka_data) -> 0 perdida.
#   12.2 Consumidor POSTERIOR: un consumer group NUEVO lee TODO el historial del
#        log desde --from-beginning -> 0 perdida por diseño.
# Requisitos: entorno Kafka levantado y RabbitMQ apagado.
# Uso (desde la carpeta kafka):
#   powershell -ExecutionPolicy Bypass -File .\scripts\cp12-recuperacion.ps1
# =============================================================================
$ErrorActionPreference = "Continue"; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location (Split-Path -Parent $PSScriptRoot)
New-Item -ItemType Directory -Force -Path ".\evidencias" | Out-Null
$logFile = ".\evidencias\cp12-recuperacion-kafka-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"
Start-Transcript -Path $logFile | Out-Null
$Topic = "orders.events"
function Run($c) { Write-Host "> $c" -ForegroundColor Cyan; cmd /c "$c 2>&1" | ForEach-Object { Write-Host $_ } }
function Dg($g) { Run "docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $g" }

Write-Host "########## CP-12 RECUPERACION / 0 PERDIDA (Kafka) - $(Get-Date -Format 'dd/MM/yyyy HH:mm:ss') ##########"
Write-Host "=== 12.1 REINICIO DEL BROKER KAFKA con eventos en el log ==="
Write-Host "--- offsets y commits de los 3 grupos ANTES del reinicio ---"
Run "docker compose exec -T kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $Topic"
Run "docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups"
Write-Host "--- REINICIAR el broker Kafka ---"; Run "docker compose restart kafka"; Start-Sleep 32; Run "docker compose ps"
Write-Host "--- offsets DESPUES del reinicio (los eventos persisten en el volumen kafka_data) ---"
Run "docker compose exec -T kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $Topic"
Write-Host "--- commits de los grupos DESPUES del reinicio (se conservan -> 0 perdida) ---"
Run "docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups"

Write-Host ""
Write-Host "=== 12.2 CONSUMIDOR POSTERIOR (grupo nuevo lee TODO el historial del log) ==="
$offLines = cmd /c "docker compose exec -T kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $Topic 2>&1"
$total = ($offLines | ForEach-Object { if ($_ -match ":(\d+)$") { [int]$Matches[1] } } | Measure-Object -Sum).Sum
Write-Host "    Eventos historicos en $Topic = $total"
$newGroup = "qa-auditor-group-$(Get-Date -Format 'HHmmss')"; $tout = $total * 700 + 10000
Write-Host "--- Crear consumer group NUEVO ($newGroup) que lee desde --from-beginning (timeout ${tout}ms) ---"
Run "docker compose exec -T kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $Topic --group $newGroup --from-beginning --timeout-ms $tout"
Write-Host "--- estado del nuevo grupo (CURRENT-OFFSET = LOG-END-OFFSET, LAG 0 -> 0 perdida) ---"; Dg $newGroup
Write-Host "    La suma de CURRENT-OFFSET del grupo debe ser $total (todo el log)."
Write-Host "=== FIN CP-12 Kafka.  Log: $logFile ==="
Stop-Transcript | Out-Null
