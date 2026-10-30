#!/usr/bin/env bash
# =============================================================================
# Persona 3 - CP-11 DESACOPLAMIENTO TEMPORAL (Kafka) - Linux/bash
# Los eventos publicados con el consumidor apagado se procesan al iniciarlo.
# Apaga inventory-consumer, publica 3 eventos (el log de Kafka los conserva),
# espera, inicia el consumidor y compara la hora de publicacion (occurredAt) con
# la hora de procesamiento (timestamp del log) -> procesamiento diferido.
#
# Requisitos: entorno Kafka levantado y RabbitMQ apagado.
# Uso (desde la carpeta kafka):  bash scripts/cp11-desacople.sh
# =============================================================================
set -u
cd "$(dirname "$0")/.."
mkdir -p evidencias
LOG="evidencias/cp11-desacople-kafka-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee "$LOG") 2>&1
API="http://localhost:3000/orders"
TOPIC="orders.events"
dg() { docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$1" 2>&1; }
offsets() { docker compose exec -T kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC" 2>&1; }

echo "##########################################################################"
echo "# CP-11 DESACOPLAMIENTO TEMPORAL (Kafka) - Fecha: $(date '+%d/%m/%Y %H:%M:%S')"
echo "##########################################################################"

echo "=== PASO 1: APAGAR inventory-consumer ==="
docker compose stop inventory-consumer

echo "=== PASO 2: offset final ANTES de publicar ==="
offsets

echo "=== PASO 3: publicar 3 eventos. HORA DE PUBLICACION: $(date '+%H:%M:%S') ==="
for i in $(seq 1 3); do
  echo -n "Pedido $i -> "
  curl -s -w "\n" -X POST "$API" -H "Content-Type: application/json" \
    -d "{\"customerName\":\"CP11K Cliente $i\",\"product\":\"Libro de arquitectura\",\"quantity\":1,\"unitPrice\":85000}"
done
sleep 2

echo "=== PASO 4: offset final DESPUES de publicar (el log crecio; eventos persistidos) ==="
offsets
echo "--- inventory-service-group acumula LAG (consumidor apagado) ---"
dg inventory-service-group

echo "=== PASO 5: ESPERA DELIBERADA de 12s con el consumidor apagado ==="
sleep 12

echo "=== PASO 6: INICIAR inventory-consumer. HORA DE INICIO: $(date '+%H:%M:%S') ==="
docker compose start inventory-consumer
sleep 20

echo "=== PASO 7: lag final (LAG 0) y procesamiento diferido (occurredAt vs timestamp log) ==="
dg inventory-service-group
docker compose logs --no-color inventory-consumer 2>&1 | grep "Contenido" | grep "CP11K"
echo "--- total CP11K procesados (debe ser 3) ---"
docker compose logs --no-color inventory-consumer 2>&1 | grep "Contenido" | grep -c "CP11K"

echo "=== FIN CP-11 Kafka.  Log: $LOG ==="
