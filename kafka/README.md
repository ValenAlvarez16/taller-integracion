# Arquitectura basada en eventos con Apache Kafka

Contenedores: `kafka` (broker + log de eventos), `kafka-ui` (consola), `orders-api` (productor) y tres
consumidores independientes: `inventory-consumer`, `billing-consumer` y `notification-consumer`.

La API publica el evento `OrderCreated` en el tópico `orders.events` (2 particiones). Cada consumidor usa su
propio consumer group, así que los tres reciben el mismo evento:

| Consumidor | Consumer group |
|---|---|
| inventory-consumer | inventory-service-group |
| billing-consumer | billing-service-group |
| notification-consumer | notification-service-group |

## Cómo ejecutarlo

```
cd kafka
docker compose up -d --build
docker compose ps
```

- API: `POST http://localhost:3000/orders`
- Kafka UI: http://localhost:8080
- Kafka desde la máquina anfitriona: `localhost:29092`

Ejemplo de pedido:

```json
{ "customerName": "Pedro", "product": "Libro de arquitectura", "quantity": 1, "unitPrice": 85000 }
```

Enviar varios pedidos desde PowerShell: `.\scripts\enviar-pedidos.ps1 -N 5`

## Pruebas útiles

```
docker compose logs -f orders-api inventory-consumer billing-consumer notification-consumer
docker compose stop inventory-consumer          # consumidor apagado
docker compose up -d --scale inventory-consumer=2   # dos instancias del mismo grupo
docker compose exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

Para apagar: `docker compose down` (con `-v` también borra los datos del broker).
