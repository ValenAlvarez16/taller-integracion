// notification-worker
// Consumidor de mensajes: escucha la cola "pedidos", procesa cada mensaje
// (simula el envío de una notificación) y confirma con ack manual.
// NO expone endpoints HTTP y NO recibe llamadas directas de la API.

const amqp = require('amqplib');
const os = require('os');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE_NAME = process.env.QUEUE_NAME || 'pedidos';
const PROCESSING_MS = Number(process.env.PROCESSING_MS || 3000);
const WORKER_ID = os.hostname(); // distingue instancias al escalar (--scale)

let processed = 0;

const log = (...args) => console.log(new Date().toISOString(), `[notification-worker ${WORKER_ID}]`, ...args);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function start() {
  let attempt = 0;
  while (true) {
    attempt++;
    try {
      log(`Conectando a RabbitMQ (intento ${attempt})...`);
      const connection = await amqp.connect(RABBITMQ_URL);
      const channel = await connection.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });

      // Un mensaje a la vez por worker: la carga se reparte entre instancias.
      await channel.prefetch(1);

      connection.on('error', (err) => log('Error de conexión AMQP:', err.message));
      connection.on('close', () => {
        log('Conexión con RabbitMQ cerrada. Reintentando...');
        setTimeout(start, 2000);
      });

      log(`Escuchando la cola "${QUEUE_NAME}" (prefetch=1, ack manual, procesamiento=${PROCESSING_MS} ms)`);

      await channel.consume(
        QUEUE_NAME,
        async (msg) => {
          if (!msg) return; // el broker canceló el consumidor
          let data;
          try {
            data = JSON.parse(msg.content.toString());
          } catch (err) {
            log('Mensaje con JSON inválido, se descarta:', msg.content.toString());
            channel.nack(msg, false, false);
            return;
          }

          log(
            `Mensaje recibido orderId=${data.orderId} correlationId=${data.correlationId} ` +
              `redelivered=${msg.fields.redelivered} deliveryTag=${msg.fields.deliveryTag}`
          );
          log('Contenido:', JSON.stringify(data));
          log(`Simulando envío de notificación a ${data.customerName} por "${data.product}" x${data.quantity}...`);

          await sleep(PROCESSING_MS); // durante esta espera el mensaje aparece como Unacked

          try {
            channel.ack(msg);
            processed++;
            log(`Notificación enviada. ACK orderId=${data.orderId} correlationId=${data.correlationId} totalProcesados=${processed}`);
          } catch (err) {
            // Si el canal se cerró antes del ack, RabbitMQ reentregará el mensaje.
            log(`No se pudo confirmar orderId=${data.orderId}: ${err.message}. Será reentregado.`);
          }
        },
        { noAck: false }
      );
      return;
    } catch (err) {
      log(`RabbitMQ no disponible: ${err.message}. Nuevo intento en 3 s.`);
      await sleep(3000);
    }
  }
}

process.on('SIGTERM', () => {
  log(`Recibida señal de apagado. Mensajes procesados en esta ejecución: ${processed}`);
  process.exit(0);
});

start();