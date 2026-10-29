// billing-consumer (Kafka)
// Consumidor de eventos: se suscribe al tópico "orders.events" con su propio consumer group
// (billing-service-group) y reacciona al evento OrderCreated: simula la generación de la factura o registro de cobro.
// NO expone endpoints HTTP, NO llama a la API y NO modifica el evento original.

const { Kafka, logLevel } = require('kafkajs');
const os = require('os');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'billing-service-group';
const PROCESSING_MS = Number(process.env.PROCESSING_MS || 3000);
const INSTANCE_ID = os.hostname(); // distingue instancias al escalar (--scale)

let processed = 0;

const log = (...args) => console.log(new Date().toISOString(), `[billing-consumer ${INSTANCE_ID}]`, ...args);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const kafka = new Kafka({
  clientId: 'billing-consumer',
  brokers: [KAFKA_BROKER],
  logLevel: logLevel.NOTHING,
  retry: { initialRetryTime: 300, retries: 8 }
});

// Acción de negocio simulada propia de este consumidor.
function simulateBusinessAction(event) {
  const d = event.data;
  return `Simulando generación de factura para ${d.customerName} por total=${d.total} (orderId=${d.orderId})...`;
}

async function start() {
  let attempt = 0;
  while (true) {
    attempt++;
    const consumer = kafka.consumer({ groupId: GROUP_ID, sessionTimeout: 10000, heartbeatInterval: 3000 });
    try {
      log(`Conectando a Kafka en ${KAFKA_BROKER} (intento ${attempt})...`);

      // Si el proceso falla de forma irrecuperable, termina y Docker lo reinicia (restart: unless-stopped).
      consumer.on(consumer.events.CRASH, ({ payload }) => {
        log(`Consumidor caído: ${payload.error && payload.error.message}. restart=${payload.restart}`);
        if (!payload.restart) process.exit(1);
      });
      // Evidencia del reparto de particiones entre instancias del mismo grupo.
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
        log(`Unido al grupo "${GROUP_ID}". Particiones asignadas: ${JSON.stringify(payload.memberAssignment)}`);
      });

      await consumer.connect();
      await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: true });
      log(`Suscrito al tópico "${KAFKA_TOPIC}" con groupId="${GROUP_ID}" (procesamiento=${PROCESSING_MS} ms)`);

      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          let event;
          try {
            event = JSON.parse(message.value.toString());
          } catch (err) {
            log(`Evento con JSON inválido (partition=${partition} offset=${message.offset}), se omite.`);
            return;
          }

          log(
            `Evento recibido eventType=${event.eventType} eventId=${event.eventId} ` +
              `orderId=${event.data && event.data.orderId} correlationId=${event.correlationId} ` +
              `topic=${topic} partition=${partition} offset=${message.offset}`
          );
          log('Contenido:', JSON.stringify(event));
          log(simulateBusinessAction(event));

          await sleep(PROCESSING_MS); // simula el tiempo de procesamiento

          processed++;
          log(
            `Evento procesado orderId=${event.data.orderId} correlationId=${event.correlationId} ` +
              `partition=${partition} offset=${message.offset} totalProcesados=${processed}`
          );
          // Al terminar esta función kafkajs confirma (commit) el offset.
        }
      });
      return;
    } catch (err) {
      log(`Kafka no disponible: ${err.message}. Nuevo intento en 3 s.`);
      try { await consumer.disconnect(); } catch (_) { /* ya desconectado */ }
      await sleep(3000);
    }
  }
}

process.on('SIGTERM', () => {
  log(`Recibida señal de apagado. Eventos procesados en esta ejecución: ${processed}`);
  process.exit(0);
});

start();
