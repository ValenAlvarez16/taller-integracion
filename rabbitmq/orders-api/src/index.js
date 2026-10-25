// orders-api (versión RabbitMQ)
// Productor de mensajes: expone POST /orders, construye el mensaje "ProcessOrder"
// y lo publica en la cola "pedidos" usando AMQP. NO llama al worker por HTTP
// y NO espera a que el worker procese el mensaje.

const express = require('express');
const amqp = require('amqplib');
const { randomUUID } = require('crypto');

const PORT = Number(process.env.PORT || 3000);
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE_NAME = process.env.QUEUE_NAME || 'pedidos';

let connection = null;
let channel = null;

const log = (...args) => console.log(new Date().toISOString(), '[orders-api]', ...args);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Conexión con reintentos: depends_on no garantiza que RabbitMQ esté listo,
// y si el broker se reinicia la API debe volver a conectarse sola.
async function connectRabbit() {
  let attempt = 0;
  while (true) {
    attempt++;
    try {
      log(`Conectando a RabbitMQ (intento ${attempt})...`);
      connection = await amqp.connect(RABBITMQ_URL);
      // Canal con confirmaciones del broker: la API sabe que RabbitMQ
      // recibió y guardó el mensaje (no que el worker lo procesó).
      channel = await connection.createConfirmChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });

      connection.on('error', (err) => log('Error de conexión AMQP:', err.message));
      connection.on('close', () => {
        log('Conexión con RabbitMQ cerrada. Reintentando...');
        channel = null;
        connection = null;
        setTimeout(connectRabbit, 2000);
      });

      log(`Conectado a RabbitMQ. Cola "${QUEUE_NAME}" declarada (durable=true).`);
      return;
    } catch (err) {
      log(`RabbitMQ no disponible: ${err.message}. Nuevo intento en 3 s.`);
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
  return errors;
}

function publish(message) {
  return new Promise((resolve, reject) => {
    channel.sendToQueue(
      QUEUE_NAME,
      Buffer.from(JSON.stringify(message)),
      {
        persistent: true,                     // sobrevive a reinicios del broker
        contentType: 'application/json',
        messageId: message.orderId,
        correlationId: message.correlationId,
        type: message.type,
        appId: 'orders-api',
        timestamp: Math.floor(Date.now() / 1000)
      },
      (err) => (err ? reject(err) : resolve())
    );
  });
}

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
  res.json({ status: channel ? 'UP' : 'DEGRADED', rabbitmq: channel ? 'connected' : 'disconnected' });
});

app.post('/orders', async (req, res) => {
  const start = process.hrtime.bigint();

  const errors = validateOrder(req.body);
  if (errors.length) {
    log('Pedido rechazado (400):', errors.join('; '));
    return res.status(400).json({ status: 'rejected', errors });
  }

  if (!channel) {
    log('Pedido rechazado (503): sin conexión con RabbitMQ');
    return res.status(503).json({ status: 'unavailable', message: 'RabbitMQ no disponible, intente de nuevo' });
  }

  const orderId = `ORD-${Date.now()}-${Math.floor(Math.random() * 1000).toString().padStart(3, '0')}`;
  const correlationId = randomUUID();
  const message = {
    type: 'ProcessOrder',
    orderId,
    correlationId,
    customerName: req.body.customerName.trim(),
    product: req.body.product.trim(),
    quantity: req.body.quantity,
    createdAt: new Date().toISOString()
  };

  try {
    await publish(message);
  } catch (err) {
    log(`Error publicando orderId=${orderId}:`, err.message);
    return res.status(503).json({ status: 'error', message: 'No se pudo publicar el mensaje en RabbitMQ' });
  }

  const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
  log(`Mensaje publicado en cola "${QUEUE_NAME}" orderId=${orderId} correlationId=${correlationId} tiempoRespuestaMs=${elapsedMs.toFixed(2)}`);

  res.set('X-Correlation-Id', correlationId);
  res.set('X-Response-Time-Ms', elapsedMs.toFixed(2));
  return res.status(202).json({
    status: 'accepted',
    message: 'Pedido recibido y enviado a RabbitMQ',
    orderId,
    correlationId
  });
});

app.listen(PORT, () => log(`API escuchando en el puerto ${PORT} (POST /orders)`));
connectRabbit();