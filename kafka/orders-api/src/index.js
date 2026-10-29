// orders-api (versión Kafka)
// Productor de eventos: expone POST /orders, construye el evento de dominio
// "OrderCreated" y lo publica en el tópico "orders.events" de Kafka.
// NO llama a los consumers por HTTP y NO espera a que ellos procesen el evento.

const express = require('express');
const { Kafka, logLevel } = require('kafkajs');
const { randomUUID } = require('crypto');

const PORT = Number(process.env.PORT || 3000);
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const KAFKA_PARTITIONS = Number(process.env.KAFKA_PARTITIONS || 2);

const log = (...args) => console.log(new Date().toISOString(), '[orders-api]', ...args);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const kafka = new Kafka({
  clientId: 'orders-api',
  brokers: [KAFKA_BROKER],
  logLevel: logLevel.NOTHING, // los logs propios son más claros para la evidencia
  retry: { initialRetryTime: 300, retries: 3 },
  requestTimeout: 5000
});

let producer = null;
let connecting = false;

// Conexión con reintentos: depends_on no garantiza que Kafka esté listo y,
// si el broker se reinicia, la API debe volver a conectarse sola.
async function connectKafka() {
  if (connecting) return;
  connecting = true;
  let attempt = 0;
  while (true) {
    attempt++;
    try {
      log(`Conectando a Kafka en ${KAFKA_BROKER} (intento ${attempt})...`);

      // Se garantiza el tópico con 2 particiones (necesario para repartir carga entre
      // instancias de un mismo consumer group, CP-10). Si ya existe no hace nada.
      const admin = kafka.admin();
      await admin.connect();
      const created = await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: KAFKA_TOPIC, numPartitions: KAFKA_PARTITIONS, replicationFactor: 1 }]
      });
      await admin.disconnect();
      log(created ? `Tópico "${KAFKA_TOPIC}" creado con ${KAFKA_PARTITIONS} particiones.` : `Tópico "${KAFKA_TOPIC}" ya existía.`);

      const p = kafka.producer({ allowAutoTopicCreation: false });
      p.on(p.events.DISCONNECT, () => {
        if (producer === p) {
          log('Productor desconectado de Kafka. Reintentando...');
          producer = null;
          setTimeout(connectKafka, 2000);
        }
      });
      await p.connect();
      producer = p;
      log(`Conectado a Kafka. Productor listo para publicar en "${KAFKA_TOPIC}".`);
      connecting = false;
      return;
    } catch (err) {
      log(`Kafka no disponible: ${err.message}. Nuevo intento en 3 s.`);
      await sleep(3000);
    }
  }
}

function validateOrder(body) {
  const errors = [];
  if (!body || typeof body !== 'object') return ['El cuerpo debe ser un JSON'];
  if (typeof body.customerName !== 'string' || !body.customerName.trim()) errors.push('customerName es obligatorio');
  if (typeof body.product !== 'string' || !body.product.trim()) errors.push('product es obligatorio');
  if (!Number.isInteger(body.quantity) || body.quantity <= 0) errors.push('quantity debe ser un entero mayor que 0');
  if (typeof body.unitPrice !== 'number' || !(body.unitPrice >= 0)) errors.push('unitPrice debe ser un número mayor o igual que 0');
  return errors;
}

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
  res.json({ status: producer ? 'UP' : 'DEGRADED', kafka: producer ? 'connected' : 'disconnected' });
});

app.post('/orders', async (req, res) => {
  const start = process.hrtime.bigint();

  const errors = validateOrder(req.body);
  if (errors.length) {
    log('Pedido rechazado (400):', errors.join('; '));
    return res.status(400).json({ status: 'rejected', errors });
  }

  if (!producer) {
    log('Pedido rechazado (503): sin conexión con Kafka');
    return res.status(503).json({ status: 'unavailable', message: 'Kafka no disponible, intente de nuevo' });
  }

  const orderId = `ORD-${Date.now()}-${Math.floor(Math.random() * 1000).toString().padStart(3, '0')}`;
  const correlationId = randomUUID();
  const { customerName, product, quantity, unitPrice } = req.body;

  // Evento de dominio: un hecho que YA ocurrió (en pasado), no un comando.
  const event = {
    eventId: randomUUID(),
    eventType: 'OrderCreated',
    eventVersion: '1.0',
    occurredAt: new Date().toISOString(),
    source: 'orders-api',
    correlationId,
    data: {
      orderId,
      customerName: customerName.trim(),
      product: product.trim(),
      quantity,
      unitPrice,
      total: quantity * unitPrice
    }
  };

  let metadata;
  try {
    metadata = await producer.send({
      topic: KAFKA_TOPIC,
      messages: [
        {
          key: orderId, // mismo pedido -> misma partición
          value: JSON.stringify(event),
          headers: { correlationId, eventType: event.eventType }
        }
      ]
    });
  } catch (err) {
    log(`Error publicando orderId=${orderId} correlationId=${correlationId}:`, err.message);
    return res.status(503).json({ status: 'error', message: 'No se pudo publicar el evento en Kafka' });
  }

  const { partition, baseOffset } = metadata[0];
  const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
  log(
    `Evento ${event.eventType} publicado topic=${KAFKA_TOPIC} partition=${partition} offset=${baseOffset} ` +
      `eventId=${event.eventId} orderId=${orderId} correlationId=${correlationId} tiempoRespuestaMs=${elapsedMs.toFixed(2)}`
  );

  res.set('X-Correlation-Id', correlationId);
  res.set('X-Response-Time-Ms', elapsedMs.toFixed(2));
  return res.status(202).json({
    status: 'accepted',
    message: 'Pedido recibido y evento OrderCreated publicado en Kafka',
    orderId,
    correlationId
  });
});

app.listen(PORT, () => log(`API escuchando en el puerto ${PORT} (POST /orders)`));
connectKafka();
