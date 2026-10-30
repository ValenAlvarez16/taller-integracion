#!/usr/bin/env bash
# =============================================================================
# Persona 3 - CP-10 ELASTIC (RabbitMQ) - Linux/bash
# Escalado de consumidores y reparto de carga.
# Escala el worker a 2 instancias, envia 10 pedidos y muestra como RabbitMQ
# reparte los mensajes entre las dos instancias (round-robin por prefetch=1).
#
# Requisitos: entorno RabbitMQ levantado y Kafka apagado.
# Uso (desde la carpeta rabbitmq):  bash scripts/cp10-elastic.sh
# =============================================================================
set -u
cd "$(dirname "$0")/.."
mkdir -p evidencias
LOG="evidencias/cp10-elastic-rabbitmq-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee "$LOG") 2>&1
API="http://localhost:3000/orders"
cola() { docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged; }

echo "##########################################################################"
echo "# CP-10 ELASTIC (RabbitMQ) - Fecha: $(date '+%d/%m/%Y %H:%M:%S')"
echo "##########################################################################"

echo "=== PASO 1: ESCALAR notification-worker a 2 instancias ==="
docker compose up -d --scale notification-worker=2
sleep 6
docker compose ps

echo "=== PASO 2: identificar las 2 instancias (WORKER_ID = hostname) ==="
docker compose logs --no-color notification-worker 2>&1 | grep "Escuchando" | tail -2

echo "=== PASO 3: enviar 10 pedidos para repartir entre las 2 instancias ==="
for i in $(seq 1 10); do
  echo -n "Pedido $i -> "
  curl -s -o /dev/null -w "HTTP %{http_code}\n" -X POST "$API" \
    -H "Content-Type: application/json" \
    -d "{\"customerName\":\"CP10 Cliente $i\",\"product\":\"Libro de arquitectura\",\"quantity\":1}"
done

echo "=== PASO 4: esperar procesamiento (10 / 2 workers x 3s + margen) ==="
sleep 22; cola

echo "=== PASO 5: REPARTO DE CARGA - pedidos CP10 procesados por cada instancia ==="
docker compose logs --no-color notification-worker 2>&1 | grep "Simulando" | grep "CP10"
echo "--- conteo por instancia (hostname) ---"
for id in $(docker compose logs --no-color notification-worker 2>&1 \
            | grep -oE "notification-worker [0-9a-f]{12}" | awk '{print $2}' | sort -u); do
  n=$(docker compose logs --no-color notification-worker 2>&1 | grep "$id" | grep "CP10" | grep -c "Simulando")
  echo "Instancia $id -> procesó $n pedidos CP10"
done

echo "=== PASO 6: volver a 1 instancia (entorno limpio) ==="
docker compose up -d --scale notification-worker=1
sleep 3; docker compose ps

echo "=== FIN CP-10 RabbitMQ.  Log: $LOG ==="
