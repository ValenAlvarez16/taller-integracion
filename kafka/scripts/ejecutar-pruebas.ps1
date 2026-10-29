# Ejecuta de forma automatica las pruebas CP-01, CP-04, CP-05, CP-06, CP-07 y CP-08 (Kafka)
# y guarda TODO lo que sale por consola en kafka\evidencias\ejecucion-<fecha>.log
#
# Uso (desde la carpeta kafka, con el entorno ya levantado con docker compose up -d --build):
#   powershell -ExecutionPolicy Bypass -File .\scripts\ejecutar-pruebas.ps1
#   (opcional) agregar -IncluirKafkaDetenido para la prueba extra de CP-08 con el broker apagado
param([switch]$IncluirKafkaDetenido, [string]$Url = "http://localhost:3000/orders")

$ErrorActionPreference = "Continue"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location (Split-Path -Parent $PSScriptRoot)
New-Item -ItemType Directory -Force -Path ".\evidencias" | Out-Null
$logFile = ".\evidencias\ejecucion-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"
Start-Transcript -Path $logFile | Out-Null

$Topic = "orders.events"
$Consumers = @("inventory-consumer", "billing-consumer", "notification-consumer")
$All = @("orders-api") + $Consumers

function Banner($t) {
  Write-Host ""
  Write-Host ("=" * 90) -ForegroundColor Yellow
  Write-Host ("{0}   [fecha de ejecucion: {1}]" -f $t, (Get-Date -Format "dd/MM/yyyy HH:mm:ss")) -ForegroundColor Yellow
  Write-Host ("=" * 90) -ForegroundColor Yellow
}

# Ejecuta un comando (cmd) mostrando comando y salida; devuelve las lineas de salida.
function Run([string]$Cmd) {
  Write-Host ""
  Write-Host "> $Cmd" -ForegroundColor Cyan
  $o = cmd /c "$Cmd 2>&1"
  $o | ForEach-Object { Write-Host $_ }
  return $o
}

function Post-Order($obj) {
  $json = $obj | ConvertTo-Json -Compress
  $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  $status = 0; $body = ""
  try {
    $r = Invoke-WebRequest -Method Post -Uri $Url -ContentType "application/json" -Body $bytes -UseBasicParsing -TimeoutSec 60
    $status = [int]$r.StatusCode; $body = $r.Content
  } catch {
    $resp = $_.Exception.Response
    if ($resp) { $status = [int]$resp.StatusCode }
    if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $body = $_.ErrorDetails.Message }
    elseif ($resp -and $resp.GetResponseStream) {
      try { $body = (New-Object System.IO.StreamReader($resp.GetResponseStream())).ReadToEnd() } catch { $body = $_.Exception.Message }
    } else { $body = $_.Exception.Message }
  }
  $sw.Stop()
  Write-Host ""
  Write-Host "POST $Url" -ForegroundColor Cyan
  Write-Host "Body enviado : $json"
  Write-Host ("HTTP {0}  ({1} ms medidos por el cliente)" -f $status, $sw.ElapsedMilliseconds)
  Write-Host "Respuesta    : $body"
  $parsed = $null
  try { $parsed = $body | ConvertFrom-Json } catch {}
  return @{ Status = $status; Ms = $sw.ElapsedMilliseconds; Body = $body; Json = $parsed }
}

function Get-Offsets {
  $o = cmd /c "docker compose exec -T kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $Topic 2>&1"
  $h = @{}
  foreach ($l in $o) { if ($l -match "^$([regex]::Escape($Topic)):(\d+):(\d+)") { $h[[int]$Matches[1]] = [int]$Matches[2] } }
  if ($h.Count -eq 0) {
    $o = cmd /c "docker compose exec -T kafka kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic $Topic 2>&1"
    foreach ($l in $o) { if ($l -match "^$([regex]::Escape($Topic)):(\d+):(\d+)") { $h[[int]$Matches[1]] = [int]$Matches[2] } }
  }
  return $h
}

function Show-Offsets($h) {
  ($h.Keys | Sort-Object | ForEach-Object { "particion $_ -> offset final $($h[$_])" }) -join "   |   "
}

# Muestra el ultimo evento publicado en la particion cuyo offset aumento respecto a $before.
function Show-NewEvent($before) {
  $after = Get-Offsets
  Write-Host ""
  Write-Host "Offsets del topico ANTES : $(Show-Offsets $before)"
  Write-Host "Offsets del topico DESPUES: $(Show-Offsets $after)"
  foreach ($p in ($after.Keys | Sort-Object)) {
    if ($after[$p] -gt $before[$p]) {
      $off = $after[$p] - 1
      Write-Host "El evento nuevo quedo en particion=$p offset=$off. Contenido leido directamente del broker:" -ForegroundColor Green
      Run "docker compose exec -T kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $Topic --partition $p --offset $off --max-messages 1 --timeout-ms 10000 --property print.partition=true --property print.offset=true --property print.key=true --property print.headers=true" | Out-Null
    }
  }
}

function Show-Groups {
  Run "docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups" | Out-Null
}

function Show-Logs($services, [int]$tail = 12) {
  foreach ($s in $services) { Run "docker compose logs --no-color --tail $tail $s" | Out-Null }
}

Write-Host "Log completo se guarda en: $logFile" -ForegroundColor Green

# ---------------------------------------------------------------------------------------------
Banner "0. MONTAJE DEL ENTORNO (estado actual)"
Run "docker compose ps" | Out-Null
Run "docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic $Topic" | Out-Null
Run "docker compose logs --no-color --tail 15 orders-api" | Out-Null
Run "docker compose logs --no-color --tail 6 inventory-consumer" | Out-Null
Write-Host "Health de la API:" ; Run "curl.exe -s http://localhost:3000/health" | Out-Null

# ---------------------------------------------------------------------------------------------
Banner "CP-01  Creacion de pedido via API"
$r1 = Post-Order @{ customerName = "Pedro"; product = "Libro de arquitectura"; quantity = 1; unitPrice = 85000 }
$r2 = Post-Order @{ customerName = "Pedro"; product = "Libro de arquitectura"; quantity = 0; unitPrice = 85000 }
Start-Sleep -Seconds 2
Run "docker compose logs --no-color --tail 6 orders-api" | Out-Null

# ---------------------------------------------------------------------------------------------
Banner "CP-04  Publicacion de evento en Kafka"
$before = Get-Offsets
$r4 = Post-Order @{ customerName = "Pedro CP04"; product = "Libro de arquitectura"; quantity = 2; unitPrice = 85000 }
Start-Sleep -Seconds 2
Run "docker compose logs --no-color --tail 3 orders-api" | Out-Null
Show-NewEvent $before

# ---------------------------------------------------------------------------------------------
Banner "CP-05  Consumo de evento (Kafka) por los 3 consumers"
Write-Host "Esperando 15 s a que los 3 consumers terminen de procesar (cada evento tarda ~3 s)..."
Start-Sleep -Seconds 15
$r5 = Post-Order @{ customerName = "Pedro CP05"; product = "Libro de arquitectura"; quantity = 1; unitPrice = 85000 }
Start-Sleep -Seconds 8
Show-Logs $Consumers 8
Show-Groups

# ---------------------------------------------------------------------------------------------
Banner "CP-06  Trazabilidad del flujo (mismo orderId / correlationId)"
$r6 = Post-Order @{ customerName = "Traza"; product = "Libro de arquitectura"; quantity = 1; unitPrice = 85000 }
$oid = $r6.Json.orderId; $cid = $r6.Json.correlationId
Write-Host "orderId=$oid   correlationId=$cid" -ForegroundColor Green
Start-Sleep -Seconds 8
Write-Host ""
Write-Host "> docker compose logs (orders-api + 3 consumers) filtrado por $oid" -ForegroundColor Cyan
$allLogs = cmd /c "docker compose logs --no-color $($All -join ' ') 2>&1"
$allLogs | Select-String -SimpleMatch $oid | ForEach-Object { Write-Host $_.Line }
Write-Host ""
Write-Host "Lineas que contienen el correlationId $cid :" -ForegroundColor Cyan
($allLogs | Select-String -SimpleMatch $cid | Measure-Object).Count | ForEach-Object { Write-Host "$_ lineas (API + inventory + billing + notification)" }
foreach ($p in 0, 1) {
  Write-Host ""
  Write-Host "Buscando el evento de $oid en la particion $p del topico (lectura directa del broker):" -ForegroundColor Cyan
  $ev = cmd /c "docker compose exec -T kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $Topic --partition $p --offset earliest --timeout-ms 8000 --property print.partition=true --property print.offset=true --property print.key=true --property print.headers=true 2>&1"
  $hit = $ev | Select-String -SimpleMatch $oid
  if ($hit) { $hit | ForEach-Object { Write-Host $_.Line } } else { Write-Host "(no esta en la particion $p)" }
}

# ---------------------------------------------------------------------------------------------
Banner "CP-07  Responsive (la API responde sin esperar a los consumers)"
Write-Host "Esperando 8 s a que se vacie el trabajo pendiente..."
Start-Sleep -Seconds 8
Write-Host "--- 7.1  5 pedidos seguidos con los consumers activos (cada uno tarda ~3 s en procesar) ---" -ForegroundColor Green
$t0 = Get-Date
for ($i = 1; $i -le 5; $i++) { Post-Order @{ customerName = "Cliente $i"; product = "Libro de arquitectura"; quantity = 1; unitPrice = 85000 } | Out-Null }
Write-Host ("Los 5 pedidos fueron aceptados en {0:N0} ms en total." -f ((Get-Date) - $t0).TotalMilliseconds) -ForegroundColor Green
Show-Groups
Write-Host "--- 7.2  Con los 3 consumers APAGADOS ---" -ForegroundColor Green
Run "docker compose stop $($Consumers -join ' ')" | Out-Null
$r7 = Post-Order @{ customerName = "Consumers apagados"; product = "Libro de arquitectura"; quantity = 1; unitPrice = 85000 }
Start-Sleep -Seconds 2
Run "docker compose logs --no-color --tail 8 orders-api" | Out-Null
Show-Groups
Write-Host "--- 7.3  Se vuelven a encender los consumers (dejan el entorno limpio y procesan lo pendiente) ---" -ForegroundColor Green
Run "docker compose start $($Consumers -join ' ')" | Out-Null
Write-Host "Esperando 45 s a que los consumers se unan al grupo y procesen lo pendiente..."
Start-Sleep -Seconds 45
Show-Groups

# ---------------------------------------------------------------------------------------------
Banner "CP-08  Message-driven (sin comunicacion directa entre servicios)"
Run "docker compose ps" | Out-Null
Write-Host ""
Write-Host "> Puertos publicados en docker compose config (solo kafka, kafka-ui y orders-api deben tener 'ports')" -ForegroundColor Cyan
$cfg = cmd /c "docker compose config 2>&1"
$cfg | Select-String -Pattern '^  [a-z-]+:$|ports:|published:|target:|container_name:' | ForEach-Object { Write-Host $_.Line }
Write-Host ""
Write-Host "> Codigo fuente: donde aparece HTTP (express / listen / http / axios / fetch)" -ForegroundColor Cyan
Get-ChildItem -Path . -Recurse -Include index.js | Where-Object { $_.FullName -notmatch "node_modules" } | ForEach-Object {
  $m = Select-String -Path $_.FullName -Pattern "require\('express'\)|app\.listen|require\('http|axios|fetch\(|node-fetch" -ErrorAction SilentlyContinue
  $rel = Resolve-Path -Relative $_.FullName
  if ($m) { $m | ForEach-Object { Write-Host ("{0}:{1}: {2}" -f $rel, $_.LineNumber, $_.Line.Trim()) } }
  else { Write-Host "$rel : sin ninguna referencia HTTP" }
}
Write-Host ""
Write-Host "> Intento de llamada directa de orders-api a cada consumer por HTTP (debe fallar)" -ForegroundColor Cyan
foreach ($c in $Consumers) { Run "docker compose exec -T orders-api wget -qO- -T 3 http://${c}:3000" | Out-Null }

if ($IncluirKafkaDetenido) {
  Write-Host ""
  Write-Host "--- CP-08 extra: con Kafka detenido la API no puede publicar (el unico vinculo es el broker) ---" -ForegroundColor Green
  Run "docker compose stop kafka" | Out-Null
  Post-Order @{ customerName = "Kafka apagado"; product = "Libro de arquitectura"; quantity = 1; unitPrice = 85000 } | Out-Null
  Run "docker compose start kafka" | Out-Null
  Write-Host "Esperando 45 s a que Kafka y los servicios se recuperen..."
  Start-Sleep -Seconds 45
  Run "docker compose ps" | Out-Null
}

# ---------------------------------------------------------------------------------------------
Banner "FIN - estado final"
Run "docker compose ps" | Out-Null
Write-Host ""
Write-Host "Log completo guardado en: $logFile" -ForegroundColor Green
Stop-Transcript | Out-Null
