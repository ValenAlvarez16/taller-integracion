#!/usr/bin/env bash
# =============================================================================
# Persona 3 - CP-09 RESILIENT (Kafka) - Linux/bash
# Recuperacion ante caida de un consumidor.
# Apaga inventory-consumer, envia 5 pedidos (la API sigue en 202; billing y
# notification siguen consumiendo), verifica que inventory acumula LAG y que al
# reiniciarlo consume los eventos pendientes desde el log (LAG vuelve a 0).
#
# Requisitos: entorno Kafka levantado (cd kafka && docker compose up -d --build)
#             y RabbitMQ apagado (ambos usan el puerto 3000).
# Uso (desde la carpeta kafka):  bash scripts/cp09-resilient.sh
# =============================================================================
set -u
cd "$(dirname "$0")/.."
mkdir -p evidencias
LOG="evidencias/cp09-resilient-kafka-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee "$LOG") 2>&1
API="http://localhost:3000/orders"
dg() { docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$1" 2>&1; }

echo "##########################################################################"
echo "# CP-09 RESILIENT (Kafka) - Fecha: $(date '+%d/%m/%Y %H:%M:%S')"
echo "##########################################################################"

echo "=== PASO 0: lag inicial de inventory-service-group ==="
dg inventory-service-group

echo "=== PASO 1: APAGAR inventory-consumer (simular caida de UN consumidor) ==="
docker compose stop inventory-consumer
docker compose ps --format "table {{.Name}}\t{{.Status}}"

echo "=== PASO 2: enviar 5 pedidos con inventory CAIDO (la API debe responder 202) ==="
for i in $(seq 1 5); do
  echo -n "Pedido $i -> "
  curl -s -w " HTTP %{http_code} (%{time_total}s)\n" -X POST "$API" \
    -H "Content-Type: application/json" \
    -d "{\"customerName\":\"CP09K Cliente $i\",\"product\":\"Libro de arquitectura\",\"quantity\":1,\"unitPrice\":85000}"
done
sleep 3

echo "=== PASO 3: inventory acumula LAG; billing sigue activo (LAG 0) ==="
echo "--- inventory-service-group (LAG > 0, sin miembros) ---"; dg inventory-service-group
echo "--- billing-service-group (sigue consumiendo) ---"; dg billing-service-group

echo "=== PASO 4: REINICIAR inventory-consumer (recuperacion) ==="
docker compose start inventory-consumer
sleep 22

echo "=== PASO 5: lag de inventory tras recuperacion (debe ser LAG 0) ==="
dg inventory-service-group

echo "=== PASO 6: inventory consumió los 5 eventos CP09K desde el log ==="
docker compose logs --no-color inventory-consumer 2>&1 | grep "Contenido" | grep "CP09K"
echo "--- total CP09K procesados por inventory (debe ser 5) ---"
docker compose logs --no-color inventory-consumer 2>&1 | grep "Contenido" | grep -c "CP09K"

echo "=== PASO 7: GROUP_JOIN tras reconexion (reasignacion de particiones) ==="
docker compose logs --no-color inventory-consumer 2>&1 | grep "Unido al grupo" | tail -1

echo "=== FIN CP-09 Kafka.  Log: $LOG ==="
