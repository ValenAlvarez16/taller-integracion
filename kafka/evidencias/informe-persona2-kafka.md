# Persona 2 — Arquitectura Kafka: montaje y pruebas (CP-01, CP-04, CP-05, CP-06, CP-07, CP-08)

Fuente de toda la evidencia: `kafka/evidencias/ejecucion-20261028-213616.log` (transcript real, 28/10/2026).
Las líneas marcadas **[CAPTURA]** son capturas de pantalla que debe tomar el integrante (la rúbrica exige capturas reales).

## 1. Contexto del sistema

```
 POST /orders                 topic orders.events (2 particiones)
 cliente ───────► orders-api ─────────────────► kafka ──┬─► inventory-consumer    (inventory-service-group)
                  (productor)                           ├─► billing-consumer      (billing-service-group)
                                                        └─► notification-consumer (notification-service-group)
                                           kafka-ui (consola, :8080)
```

Cada consumidor usa su propio *consumer group*, por lo que los tres reciben el mismo evento `OrderCreated`.
La única vía de comunicación entre servicios es el broker.

## 2. Montaje del entorno (Kafka)

Comando: `cd kafka && docker compose up -d --build`

| Evidencia | Qué muestra | Dónde |
|---|---|---|
| `docker compose ps` | 6 contenedores activos: kafka (healthy), kafka-ui, orders-api, 3 consumers | log, sección "0. MONTAJE" + **[CAPTURA]** |
| Puertos | API `3000`, Kafka UI `8080`, Kafka host `29092` | log CP-08 (`docker compose config`) |
| Tópico | `orders.events`, 2 particiones, replicación 1 | log sección 0 + **[CAPTURA]** http://localhost:8080 → Topics → orders.events |
| Consumer groups | 3 grupos configurados | log CP-05 + **[CAPTURA]** Kafka UI → Consumers |

## 3. Casos de prueba

### CP-01 — Creación de pedido vía API
1. **ID:** CP-01
2. **Nombre:** Creación de pedido vía API (Kafka)
3. **Objetivo:** Verificar que `POST /orders` acepta un pedido válido y rechaza uno inválido.
4. **Precondiciones:** `docker compose up -d --build` ejecutado; los 6 contenedores activos.
5. **Pasos:** (1) POST `http://localhost:3000/orders` con `{"customerName":"Pedro","product":"Libro de arquitectura","quantity":1,"unitPrice":85000}`. (2) Repetir con `quantity: 0`.
6. **Resultado esperado:** Pedido válido aceptado con orderId y correlationId; pedido inválido rechazado con error de validación.
7. **Resultado obtenido:** Válido → **HTTP 202**, `orderId=ORD-1793241382612-820`, `correlationId=4ccf2ea1-25bb-4308-9931-da7f59a12ed1`, mensaje "evento OrderCreated publicado en Kafka" (816 ms, primera llamada). Inválido → **HTTP 400**, `"quantity debe ser un entero mayor que 0"`.
8. **Estado:** PASS
9. **Evidencia:** log, sección CP-01 + **[CAPTURA]** de la respuesta.
10. **Notas:** La API devuelve 202 Accepted (correcto en un flujo asíncrono), no 201 como menciona la guía; aclararlo en el informe.
11. **Fecha de ejecución:** 28/10/2026

### CP-04 — Publicación de evento en Kafka
1. **ID:** CP-04
2. **Nombre:** Publicación de evento en Kafka
3. **Objetivo:** Comprobar que el pedido genera un evento `OrderCreated` en el tópico `orders.events`.
4. **Precondiciones:** Entorno activo; offsets del tópico medidos antes de la prueba.
5. **Pasos:** (1) Leer offsets finales. (2) POST de un pedido (`Pedro CP04`, quantity 2). (3) Leer offsets de nuevo. (4) Leer el mensaje directamente del broker con `kafka-console-consumer.sh`.
6. **Resultado esperado:** El offset de una partición aumenta en 1 y el mensaje contiene el evento con el orderId.
7. **Resultado obtenido:** Offsets antes: p0=0, p1=1; después: p0=0, p1=2. Evento en **partición 1, offset 1**, key `ORD-1793241389967-854`, headers `correlationId:eb74e226-…,eventType:OrderCreated`, JSON con `eventId`, `eventType`, `eventVersion 1.0`.
8. **Estado:** PASS
9. **Evidencia:** log, sección CP-04 + **[CAPTURA]** Kafka UI → orders.events → Messages.
10. **Notas:** El evento se publicó en 55 ms (HTTP 202).
11. **Fecha de ejecución:** 28/10/2026

### CP-05 — Consumo de evento (Kafka)
1. **ID:** CP-05
2. **Nombre:** Consumo de evento por los 3 consumers
3. **Objetivo:** Verificar que cada consumer group recibe y procesa el mismo evento.
4. **Precondiciones:** Consumers activos y sin trabajo pendiente (espera de 15 s).
5. **Pasos:** (1) POST de un pedido (`Pedro CP05`). (2) Esperar 8 s. (3) Revisar logs de los 3 consumers y `kafka-consumer-groups.sh --describe --all-groups`.
6. **Resultado esperado:** Los 3 consumers registran "Evento recibido" y "Evento procesado" del mismo orderId; lag 0 en los 3 grupos.
7. **Resultado obtenido:** `ORD-1793241413783-916` recibido y procesado por inventory, billing y notification (~3 s de procesamiento simulado, partición 1, offset 2). Grupos con LAG 0 (CURRENT-OFFSET = LOG-END-OFFSET = 3 en p1).
8. **Estado:** PASS
9. **Evidencia:** log, sección CP-05 + **[CAPTURA]** Kafka UI → Consumers.
10. **Notas:** La partición 0 aparece sin offset (`-`) porque en ese momento nunca había recibido mensajes.
11. **Fecha de ejecución:** 28/10/2026

### CP-06 — Trazabilidad del flujo completo
1. **ID:** CP-06
2. **Nombre:** Trazabilidad del flujo (API → tópico → consumers)
3. **Objetivo:** Seguir un pedido de extremo a extremo con el mismo orderId/correlationId.
4. **Precondiciones:** Entorno activo.
5. **Pasos:** (1) POST de un pedido (`Traza`). (2) Filtrar los logs de API y 3 consumers por orderId y correlationId. (3) Buscar el evento en cada partición del tópico.
6. **Resultado esperado:** El mismo orderId/correlationId aparece en la API, en el tópico y en los 3 consumers.
7. **Resultado obtenido:** `orderId=ORD-1793241425635-747`, `correlationId=e00fce5e-5590-4a4d-a766-06a3a90e18ed`. La API publicó en partición 0 offset 0; el evento está en esa partición del broker (no en la 1); los 3 consumers lo recibieron y procesaron. **10 líneas** de log con el correlationId.
8. **Estado:** PASS
9. **Evidencia:** log, sección CP-06.
10. **Notas:** Un solo evento, un solo orderId en todo el flujo.
11. **Fecha de ejecución:** 28/10/2026

### CP-07 — Responsive
1. **ID:** CP-07
2. **Nombre:** Responsive — la API responde sin esperar a los consumers
3. **Objetivo:** Demostrar que la latencia de la API es independiente del tiempo de procesamiento de los consumers.
4. **Precondiciones:** Entorno activo; cada consumer tarda ~3 s por evento.
5. **Pasos:** (1) Enviar 5 pedidos seguidos. (2) Detener los 3 consumers. (3) Enviar un pedido. (4) Revisar lag. (5) Reiniciar los consumers y revisar lag.
6. **Resultado esperado:** La API responde en milisegundos en ambos escenarios; el trabajo acumulado se procesa al volver los consumers.
7. **Resultado obtenido:** 5 pedidos aceptados en **153 ms en total** (17–54 ms c/u, todos HTTP 202), aunque procesarlos toma ~3 s por evento. Con consumers apagados: pedido aceptado en **19 ms**; los 3 grupos "no active members" con LAG 4 (p0) y 2 (p1). Tras reiniciar: CURRENT-OFFSET = LOG-END-OFFSET = 5 en ambas particiones, **LAG 0**.
8. **Estado:** PASS
9. **Evidencia:** log, sección CP-07 + **[CAPTURA]** del lag en Kafka UI → Consumers antes y después.
10. **Notas:** Esta prueba deja los consumers encendidos y el entorno limpio al terminar.
11. **Fecha de ejecución:** 28/10/2026

### CP-08 — Message-driven
1. **ID:** CP-08
2. **Nombre:** Message-driven — sin comunicación directa entre servicios
3. **Objetivo:** Verificar que los servicios solo se comunican a través del broker.
4. **Precondiciones:** Entorno activo.
5. **Pasos:** (1) Revisar puertos publicados en `docker compose config`. (2) Buscar referencias HTTP en el código de cada servicio. (3) Intentar llamar por HTTP desde `orders-api` a cada consumer.
6. **Resultado esperado:** Solo kafka, kafka-ui y orders-api publican puertos; los consumers no tienen código HTTP y rechazan conexiones.
7. **Resultado obtenido:** Puertos publicados: kafka 29092, kafka-ui 8080, orders-api 3000; los 3 consumers sin `ports`. `index.js` de los 3 consumers: "sin ninguna referencia HTTP"; solo orders-api usa express. Las 3 llamadas directas: `wget: can't connect … Connection refused`.
8. **Estado:** PASS
9. **Evidencia:** log, sección CP-08.
10. **Notas:** Existe la opción `-IncluirKafkaDetenido` del script para una prueba extra con el broker apagado; no se ejecutó.
11. **Fecha de ejecución:** 28/10/2026
