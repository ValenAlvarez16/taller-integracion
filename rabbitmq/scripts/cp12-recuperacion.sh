#!/usr/bin/env bash
# =============================================================================
# Persona 3 - CP-12 RECUPERACION / 0 PERDIDA (RabbitMQ) - Linux/bash
# Dos sub-pruebas:
#   12.1 Reinicio del BROKER con mensajes persistentes: sobreviven (cola durable
#        + persistent=true) y se procesan al volver el worker.
#   12.2 Mensaje UNACKED: si el worker muere a mitad del procesamiento, RabbitMQ
#        reencola el mensaje y lo REENTREGA (redelivered=true) al reiniciar.
#
# Requisitos: entorno RabbitMQ levantado y Kafka apagado.
# Uso (desde la carpeta rabbitmq):  bash scripts/cp12-recuperacion.sh
# =============================================================================
set -u
cd "$(dirname "$0")/.."
mkdir -p evidencias
LOG="evidencias/cp12-recuperacion-rabbitmq-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee "$LOG") 2>&1
API="http://localhost:3000/orders"
cola() { docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged; }

echo "##########################################################################"
echo "# CP-12 RECUPERACION / 0 PERDIDA (RabbitMQ) - Fecha: $(date '+%d/%m/%Y %H:%M:%S')"
echo "##########################################################################"

echo "=== 12.1 REINICIO DEL BROKER con mensajes persistentes retenidos ==="
echo "--- Apagar worker para que los mensajes queden retenidos ---"
docker compose stop notification-worker
echo "--- Publicar 4 pedidos (persistent=true, cola durable=true) ---"
for i in $(seq 1 4); do
  echo -n "Pedido $i -> "
  curl -s -o /dev/null -w "HTTP %{http_code}\n" -X POST "$API" \
    -H "Content-Type: application/json" \
    -d "{\"customerName\":\"CP12 Cliente $i\",\"product\":\"Libro de arquitectura\",\"quantity\":1}"
done
sleep 2
echo "--- Cola ANTES del reinicio del broker ---"
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready durable
echo "--- REINICIAR el broker RabbitMQ ---"
docker compose restart rabbitmq
sleep 25
docker compose ps --format "table {{.Name}}\t{{.Status}}"
echo "--- Cola DESPUES del reinicio (los mensajes persistentes deben seguir ahi) ---"
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready durable
echo "--- Iniciar worker: procesa los 4 mensajes recuperados ---"
docker compose start notification-worker
sleep 18
echo "--- Cola final (0 -> 0 perdida pese al reinicio del broker) ---"
cola

echo ""
echo "=== 12.2 REENTREGA de mensaje UNACKED (worker muere a mitad de procesamiento) ==="
oid=$(curl -s -X POST "$API" -H "Content-Type: application/json" \
  -d "{\"customerName\":\"CP12 Unacked\",\"product\":\"Libro de arquitectura\",\"quantity\":1}" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['orderId'])")
echo "    Pedido publicado orderId=$oid"
sleep 1
echo "    Estado: el mensaje deberia estar UNACKED (entregado pero sin confirmar):"
cola
echo "--- MATAR el worker (kill -9) antes de que confirme el ACK ---"
docker compose kill notification-worker
sleep 3
echo "    Tras matar el worker, el mensaje UNACKED vuelve a READY (RabbitMQ lo reencola):"
cola
echo "--- Reiniciar worker: REENTREGA (redelivered=true) y procesa ---"
docker compose up -d notification-worker
sleep 8
echo "    Cola final (0 -> el mensaje no se perdio, se reentrego):"
cola
echo "    Evidencia de reentrega (redelivered=true) del pedido $oid:"
docker compose logs --no-color notification-worker 2>&1 | grep "$oid"

echo "=== FIN CP-12 RabbitMQ.  Log: $LOG ==="
