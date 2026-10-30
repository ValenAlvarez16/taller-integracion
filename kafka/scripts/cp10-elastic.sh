#!/usr/bin/env bash
# =============================================================================
# Persona 3 - CP-10 ELASTIC (Kafka) - Linux/bash
# Escalado de consumidores del MISMO grupo.
# Escala inventory-consumer a 2 instancias: Kafka asigna una particion del topico
# a cada instancia (CONSUMER-ID distinto por particion). Envia 6 pedidos y muestra
# el reparto por particion mediante la vista del broker (offsets antes/despues).
#
# Requisitos: entorno Kafka levantado y RabbitMQ apagado.
# Uso (desde la carpeta kafka):  bash scripts/cp10-elastic.sh
# =============================================================================
set -u
cd "$(dirname "$0")/.."
mkdir -p evidencias
LOG="evidencias/cp10-elastic-kafka-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee "$LOG") 2>&1
API="http://localhost:3000/orders"
TOPIC="orders.events"
dg() { docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$1" 2>&1; }

echo "##########################################################################"
echo "# CP-10 ELASTIC (Kafka) - Fecha: $(date '+%d/%m/%Y %H:%M:%S')"
echo "##########################################################################"

echo "=== PASO 1: ESCALAR inventory-consumer a 2 instancias (mismo grupo) ==="
docker compose up -d --scale inventory-consumer=2
sleep 15
docker compose ps | grep -E "NAME|inventory"

echo "=== PASO 2: reparto de PARTICIONES entre las 2 instancias (vista del broker) ==="
echo "    Cada particion queda asignada a un CONSUMER-ID distinto -> Kafka reparte por particion."
dg inventory-service-group

echo "=== PASO 3: enviar 6 pedidos (key=orderId -> se distribuyen entre las 2 particiones) ==="
for i in $(seq 1 6); do
  echo -n "Pedido $i -> "
  curl -s -o /dev/null -w "HTTP %{http_code}\n" -X POST "$API" \
    -H "Content-Type: application/json" \
    -d "{\"customerName\":\"CP10K Cliente $i\",\"product\":\"Libro de arquitectura\",\"quantity\":1,\"unitPrice\":85000}"
done
sleep 18

echo "=== PASO 4: total de eventos CP10K consumidos por el grupo inventory (debe ser 6) ==="
docker compose logs --no-color inventory-consumer 2>&1 | grep "Contenido" | grep -c "CP10K"
echo "    El reparto por particion se evidencia en la vista del broker (PASO 2 y PASO 5):"
echo "    cada particion tiene un CONSUMER-ID distinto y sus offsets avanzan de forma independiente."

echo "=== PASO 5: lag final del grupo y avance de offsets por particion (LAG 0) ==="
dg inventory-service-group

echo "=== PASO 6: volver a 1 instancia (entorno limpio) ==="
docker compose up -d --scale inventory-consumer=1
sleep 5
docker compose ps | grep -E "NAME|inventory"

echo "=== FIN CP-10 Kafka.  Log: $LOG ==="
