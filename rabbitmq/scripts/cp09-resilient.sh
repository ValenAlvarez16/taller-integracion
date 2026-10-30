#!/usr/bin/env bash
# =============================================================================
# Persona 3 - CP-09 RESILIENT (RabbitMQ) - Linux/bash
# Recuperacion ante caida del consumidor.
# Apaga el worker, envia 5 pedidos (la API sigue respondiendo 202), verifica que
# se acumulan en la cola y que al reiniciar el worker se procesan sin perdida.
#
# Requisitos: entorno RabbitMQ levantado (cd rabbitmq && docker compose up -d --build)
#             y Kafka apagado (ambos usan el puerto 3000).
# Uso (desde la carpeta rabbitmq):  bash scripts/cp09-resilient.sh
# =============================================================================
set -u
cd "$(dirname "$0")/.."
mkdir -p evidencias
LOG="evidencias/cp09-resilient-rabbitmq-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee "$LOG") 2>&1
API="http://localhost:3000/orders"
cola() { docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged; }

echo "##########################################################################"
echo "# CP-09 RESILIENT (RabbitMQ) - Fecha: $(date '+%d/%m/%Y %H:%M:%S')"
echo "##########################################################################"

echo "=== PASO 0: estado inicial (contenedores + cola 'pedidos') ==="
docker compose ps
cola

echo "=== PASO 1: APAGAR el consumidor (simular caida) ==="
docker compose stop notification-worker
docker compose ps

echo "=== PASO 2: enviar 5 pedidos con el worker CAIDO (la API debe responder 202) ==="
for i in $(seq 1 5); do
  echo -n "Pedido $i -> "
  curl -s -w " HTTP %{http_code} (%{time_total}s)\n" -X POST "$API" \
    -H "Content-Type: application/json" \
    -d "{\"customerName\":\"CP09 Cliente $i\",\"product\":\"Libro de arquitectura\",\"quantity\":1}"
done

echo "=== PASO 3: cola con worker caido (deben quedar READY=5, UNACKED=0) ==="
sleep 2; cola

echo "=== PASO 4: REINICIAR el consumidor (recuperacion) ==="
docker compose start notification-worker

echo "=== PASO 5: esperar procesamiento (5 x 3s + margen) ==="
sleep 20

echo "=== PASO 6: cola final (debe quedar 0 -> 0 perdida) ==="
cola

echo "=== PASO 7: el worker procesó los pedidos CP09 (notificacion + ACK) ==="
docker compose logs --no-color notification-worker 2>&1 | grep "Simulando" | grep "CP09"
docker compose logs --no-color notification-worker 2>&1 | grep "ACK" | tail -5

echo "=== FIN CP-09 RabbitMQ.  Log: $LOG ==="
