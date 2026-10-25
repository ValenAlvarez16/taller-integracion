# Arquitectura basada en mensajes con RabbitMQ

Contenedores: rabbitmq (broker), orders-api (productor) y notification-worker (consumidor).

## Cómo ejecutarlo

cd rabbitmq
docker compose up -d --build
docker compose ps

- API: POST http://localhost:3000/orders
- Consola RabbitMQ: http://localhost:15672 (admin / admin)

Ejemplo de pedido:

json
{ "customerName": "Pedro", "product": "Libro de arquitectura", "quantity": 1 }


Para apagar: `docker compose down`
