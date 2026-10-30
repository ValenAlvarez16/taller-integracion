#!/usr/bin/env bash
# =============================================================================
# Persona 3 - CP-12 RECUPERACION / 0 PERDIDA (Kafka) - Linux/bash
# Dos sub-pruebas:
#   12.1 Reinicio del BROKER Kafka: los offsets del log y los commits de los 3
#        grupos se conservan (volumen kafka_data) -> 0 perdida.
#   12.2 Consumidor POSTERIOR: un consumer group NUEVO (creado despues de publicar)
#        lee TODO el historial del log desde --from-beginning -> 0 perdida por diseño.
#
# Requisitos: entorno Kafka levantado y RabbitMQ apagado.
# Uso (desde la carpeta kafka):  bash scripts/cp12-recuperacion.sh
# =============================================================================
set -u
cd "$(dirname "$0")/.."
mkdir -p evidencias
LOG="evidencias/cp12-recuperacion-kafka-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee "$LOG") 2>&1
TOPIC="orders.events"
offsets() { docker compose exec -T kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC" 2>&1; }
dg() { docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$1" 2>&1; }

echo "##########################################################################"
echo "# CP-12 RECUPERACION / 0 PERDIDA (Kafka) - Fecha: $(date '+%d/%m/%Y %H:%M:%S')"
echo "##########################################################################"

echo "=== 12.1 REINICIO DEL BROKER KAFKA con eventos en el log ==="
echo "--- offsets y commits de los 3 grupos ANTES del reinicio ---"
offsets
docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups 2>&1 | grep -E "GROUP|service-group"
echo "--- REINICIAR el broker Kafka ---"
docker compose restart kafka
sleep 32
docker compose ps | grep -E "NAME|kafka"
echo "--- offsets DESPUES del reinicio (los eventos persisten en el volumen kafka_data) ---"
offsets
echo "--- commits de los grupos DESPUES del reinicio (se conservan -> 0 perdida) ---"
docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups 2>&1 | grep -E "GROUP|service-group"

echo ""
echo "=== 12.2 CONSUMIDOR POSTERIOR (grupo nuevo lee TODO el historial del log) ==="
TOTAL=$(offsets | awk -F: '{s+=$3} END{print s}')
echo "    Eventos historicos en $TOPIC = $TOTAL"
NEWGROUP="qa-auditor-group-$(date +%H%M%S)"
TOUT=$(( TOTAL * 700 + 10000 ))
echo "--- Crear consumer group NUEVO ($NEWGROUP) que lee desde --from-beginning (timeout ${TOUT}ms) ---"
LEIDOS=$(docker compose exec -T kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC" \
  --group "$NEWGROUP" --from-beginning --timeout-ms "$TOUT" 2>/dev/null | grep -c "OrderCreated")
echo "    Eventos leidos por $NEWGROUP = $LEIDOS"
echo "--- estado del nuevo grupo tras leer el historial (CURRENT-OFFSET = LOG-END-OFFSET, LAG 0) ---"
dg "$NEWGROUP"
CURR=$(dg "$NEWGROUP" | awk '/orders.events/ {s+=$4} END{print s}')
if [ "$CURR" -eq "$TOTAL" ]; then
  echo "    RESULTADO: OK (CURRENT-OFFSET del grupo = $CURR = $TOTAL eventos del log) -> 0 perdida"
else
  echo "    RESULTADO: revisar (CURRENT-OFFSET=$CURR vs TOTAL=$TOTAL; lector=$LEIDOS)"
fi

echo "=== FIN CP-12 Kafka.  Log: $LOG ==="
