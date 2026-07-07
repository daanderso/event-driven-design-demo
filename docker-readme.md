# Local Kafka (Docker)

Docker Compose stack for running Apache Kafka locally while developing the **event-driven-design-demo** Spring Boot application.

The Spring app runs on your **host machine** (not in Docker). Only Kafka infrastructure is containerized.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Ports **9092** (Kafka) and **8081** (Kafka UI) available on your machine

## Architecture

```
Host machine                         Docker Compose
+------------------+                 +---------------------------+
| Spring Boot app  | -- localhost:9092 --> | kafka (broker)      |
| :8080            |                 |   internal: kafka:29092 |
+------------------+                 | kafka-init (one-shot)   |
                                     | kafka-ui :8081            |
                                     +---------------------------+
```

- **Host → Kafka:** Spring Boot connects to `localhost:9092` (see `application.yml`).
- **Containers → Kafka:** Kafka UI and the init job use the internal address `kafka:29092`.
- **Topic:** `application-submitted` (1 partition, replication factor 1).

## Quick start

Start Kafka and Kafka UI in the background:

```bash
docker compose up -d
```

Check service status:

```bash
docker compose ps
```

Tail broker logs:

```bash
docker compose logs -f kafka
```

Stop containers (keep topic data):

```bash
docker compose down
```

Stop and delete broker data:

```bash
docker compose down -v
```

## Verify setup

List topics:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Expected output includes `application-submitted`.

Open Kafka UI in a browser:

[http://localhost:8081](http://localhost:8081)

Run the Spring Boot app:

```bash
./mvnw spring-boot:run
```

On Windows:

```bash
mvnw.cmd spring-boot:run
```

Submit a test application:

```bash
curl -X POST http://localhost:8080/applications \
  -H "Content-Type: application/json" \
  -d '{"firstName":"Jane","lastName":"Doe"}'
```

After the outbox dispatcher runs, you should see messages on the `application-submitted` topic in Kafka UI.

Optional — consume messages from the CLI:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic application-submitted \
  --from-beginning \
  --property print.headers=true
```

Press `Ctrl+C` to stop the consumer.

## docker-compose.yml walkthrough

### `kafka` — single-node KRaft broker

Uses the official [`apache/kafka`](https://hub.docker.com/r/apache/kafka) image in **KRaft** mode. KRaft replaces ZooKeeper, which keeps the local stack simpler and aligns with modern Kafka (Spring Boot 4.x embedded tests also use KRaft-style brokers).

**Dual listeners** (required on Docker Desktop for Windows/macOS):

| Listener | Address | Used by |
|----------|---------|---------|
| `PLAINTEXT_HOST` | `localhost:9092` | Spring Boot on the host |
| `PLAINTEXT` | `kafka:29092` | Other Docker containers |

Kafka returns *advertised* addresses to clients after they connect. If only an internal Docker hostname were advertised, your host JVM could not reach the broker even though port 9092 is mapped.

Other notable settings:

- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` — single broker
- `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1` — supports producer `acks: all` on one node
- `kafka-data` volume — persists topics across `docker compose down` (wiped only with `-v`)

A healthcheck waits until the broker accepts connections before starting dependent services.

### `kafka-init` — one-shot topic creation

Runs once after Kafka is healthy and creates the `application-submitted` topic if it does not exist. This matches the topic name in `application.yml` and integration tests.

`restart: "no"` ensures the container exits after success instead of restarting indefinitely.

### `kafka-ui` — web console

[`provectuslabs/kafka-ui`](https://hub.docker.com/r/provectuslabs/kafka-ui) provides a browser UI to inspect clusters, topics, and messages.

Mapped to **8081** on the host to avoid conflicting with Spring Boot on **8080**.

## App integration

No Docker-specific changes are required in the application:

| Setting | Value |
|---------|-------|
| Bootstrap servers | `localhost:9092` (override with `KAFKA_BOOTSTRAP_SERVERS`) |
| Topic | `application-submitted` |
| Value format | Raw Avro `byte[]` (no Confluent Schema Registry) |
| Tests | Use `@EmbeddedKafka`; they do not require this Docker stack |

## Troubleshooting

**Port 9092 already in use**

Another Kafka instance or service is bound to 9092. Stop it or change the host port mapping in `docker-compose.yml` (and update `KAFKA_BOOTSTRAP_SERVERS` accordingly).

**Spring app cannot connect to Kafka**

1. Confirm Kafka is healthy: `docker compose ps`
2. Wait for the healthcheck to pass (~30s on first start)
3. Ensure bootstrap servers are `localhost:9092`, not `kafka:29092` (the latter only works inside Docker)

**`kafka-init` exited with an error**

Check logs: `docker compose logs kafka-init`. Usually means the broker was not ready; run `docker compose up -d` again.

**No messages in Kafka UI**

The outbox dispatcher runs on a fixed delay (default 5s). Submit an application, wait briefly, then refresh the topic view in Kafka UI.

**`docker compose exec` fails in Git Bash on Windows**

Git Bash may rewrite paths like `/opt/kafka/...`. Prefix the command with `MSYS_NO_PATHCONV=1`:

```bash
MSYS_NO_PATHCONV=1 docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Alternatively, run exec commands from PowerShell or CMD.
