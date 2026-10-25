# Envía N pedidos a la API (por defecto 5) desde Windows PowerShell.
# Uso: .\scripts\enviar-pedidos.ps1 -N 10
param([int]$N = 5, [string]$Url = "http://localhost:3000/orders")

for ($i = 1; $i -le $N; $i++) {
  $body = @{ customerName = "Cliente $i"; product = "Libro de arquitectura"; quantity = 1 } | ConvertTo-Json
  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  $resp = Invoke-RestMethod -Method Post -Uri $Url -ContentType "application/json" -Body $body
  $sw.Stop()
  Write-Output ("{0}  -> {1} ms" -f ($resp | ConvertTo-Json -Compress), $sw.ElapsedMilliseconds)
}