#!/usr/bin/env bash
# =============================================================================
# Persona 3 - CP-11 DESACOPLAMIENTO TEMPORAL (RabbitMQ) - Linux/bash
# Los mensajes publicados con el consumidor apagado se procesan al iniciarlo.
# Publica 3 pedidos con el worker apagado, espera, lo inicia y compara la hora
# de publicacion (createdAt) con la hora de procesamiento (timestamp del log).
#
# Requisitos: entorno RabbitMQ levantado y Kafka apagado.
# Uso (desde la carpeta rabbitmq):  bash scripts/cp11-desacople.sh
# =============================================================================
set -u
cd "$(dirname "$0")/.."
mkdir -p evidencias
LOG="evidencias/cp11-desacople-rabbitmq-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee "$LOG") 2>&1
API="http://localhost:3000/orders"
cola() { docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged; }

echo "##########################################################################"
echo "# CP-11 DESACOPLAMIENTO TEMPORAL (RabbitMQ) - Fecha: $(date '+%d/%m/%Y %H:%M:%S')"
echo "##########################################################################"

echo "=== PASO 1: APAGAR el consumidor ==="
docker compose stop notification-worker
docker compose ps --format "table {{.Name}}\t{{.Status}}"

echo "=== PASO 2: publicar 3 pedidos. HORA DE PUBLICACION: $(date '+%H:%M:%S') ==="
for i in $(seq 1 3); do
  echo -n "Pedido $i -> "
  curl -s -w "\n" -X POST "$API" -H "Content-Type: application/json" \
    -d "{\"customerName\":\"CP11 Cliente $i\",\"product\":\"Libro de arquitectura\",\"quantity\":1}"
done

echo "=== PASO 3: cola con consumidor apagado (READY=3) ==="
sleep 2; cola

echo "=== PASO 4: ESPERA DELIBERADA de 10s con los mensajes retenidos en la cola ==="
sleep 10; cola

echo "=== PASO 5: INICIAR el consumidor. HORA DE INICIO: $(date '+%H:%M:%S') ==="
docker compose start notification-worker
sleep 15

echo "=== PASO 6: cola final (debe quedar 0) ==="
cola

echo "=== PASO 7: comparar createdAt (publicacion) vs timestamp del log (procesamiento) ==="
docker compose logs --no-color notification-worker 2>&1 | grep "CP11" | grep "Contenido"

echo "=== FIN CP-11 RabbitMQ.  Log: $LOG ==="
