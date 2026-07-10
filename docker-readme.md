# Local Kafka (Docker)

Docker Compose stack for running Apache Kafka locally while developing the **event-driven-design-demo** Spring Boot application.

The Spring app runs on your **host machine** (not in Docker). Only Kafka infrastructure is containerized.

## What is in Docker

| Service | Image | Purpose |
|---------|-------|---------|
| `kafka` | `apache/kafka:4.3.0` | Single-node KRaft broker |
| `kafka-init` | `apache/kafka:4.3.0` | One-shot topic creation (`application-submitted`) |
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | Web UI to browse topics and messages |

**Not included** (not required by this project):

- ZooKeeper (KRaft mode)
- Confluent Schema Registry (app publishes raw Avro `byte[]`)
- Spring Boot application container

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and **running**
- Ports available on your machine (see [Port reference](#port-reference))

## Where to run commands

All `docker compose` commands must be run from the **project root** — the directory that contains `docker-compose.yml`:

```
c:\Users\daand\Software-Development\IdeaProjects\event-driven-design-demo
```

Example:

```bash
cd /c/Users/daand/Software-Development/IdeaProjects/event-driven-design-demo
docker compose up -d
```

Docker Compose automatically reads `docker-compose.yml` in the current directory.

## Windows: Docker not found in Git Bash

On Windows, Git Bash often does not have Docker on `PATH`. Symptoms:

```text
bash: docker: command not found
```

or, when pulling images:

```text
error getting credentials - err: exec: "docker-credential-desktop": executable file not found in %PATH%
```

**Fix for the current session** — add Docker Desktop's bin directory to `PATH`:

```bash
export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
```

Then run `docker compose` normally.

**Alternative** — use the full path to `docker.exe`:

```bash
"C:\Program Files\Docker\Docker\resources\bin\docker.exe" compose up -d
```

**Permanent fix (Git Bash)** — add the `export PATH=...` line above to your `~/.bashrc`.

PowerShell and CMD usually work without this if Docker Desktop added itself to the system `PATH` during installation.

## Port reference

| Port | Used by | Notes |
|------|---------|-------|
| **8080** | Spring Boot app (host) | Default Tomcat port; **not** used by Docker in this project |
| **8081** | Kafka UI (Docker) | Host mapping `8081:8080` — avoids clashing with Spring Boot |
| **9092** | Kafka broker (Docker) | Host mapping for Spring Boot and local CLI tools |
| **29092** | Kafka (internal) | Docker network only — used by `kafka-ui` and `kafka-init` |

If Spring Boot fails with **"Port 8080 was already in use"**, that is almost always a **stale Spring Boot instance** on the host (e.g. from a previous IDE run), not Kafka or Docker. See [Port 8080 already in use](#port-8080-already-in-use).

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

## Daily development workflow

1. Start Docker Desktop (if not already running).
2. From the project root, start Kafka:

   ```bash
   docker compose up -d
   ```

3. Confirm services are up:

   ```bash
   docker compose ps
   ```

   Expect `kafka` to show **healthy** and `kafka-ui` **running**. `kafka-init` exits after creating the topic (that is normal).

4. Start the Spring Boot app **once** — from the IDE **or** Maven, not both at the same time:

   ```bash
   ./mvnw spring-boot:run
   ```

   On Windows:

   ```bash
   mvnw.cmd spring-boot:run
   ```

5. Use the app at [http://localhost:8080](http://localhost:8080). Browse Kafka at [http://localhost:8081](http://localhost:8081).

6. When finished with Kafka for the day:

   ```bash
   docker compose down
   ```

Integration tests use `@EmbeddedKafka` and **do not** require this Docker stack.

## Quick start (commands)

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

View all service logs:

```bash
docker compose logs -f
```

Stop containers (keep topic data in volume):

```bash
docker compose down
```

Stop and delete broker data (fresh Kafka cluster):

```bash
docker compose down -v
```

Pull newer images (after editing image tags in `docker-compose.yml`):

```bash
docker compose pull
```

Recreate containers after config or image changes:

```bash
docker compose up -d
```

**Common mistake:** use `up -d` (with a space), not `up-d`. The latter is not a valid command.

Full restart sequence (e.g. after upgrading Kafka):

```bash
docker compose down -v
docker compose pull
docker compose up -d
```

## Verify setup

List topics:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Expected output includes `application-submitted`.

On **Git Bash (Windows)**, if exec fails with a path error, prefix with `MSYS_NO_PATHCONV=1` (see [Troubleshooting](#troubleshooting)).

Open Kafka UI in a browser:

[http://localhost:8081](http://localhost:8081)

Submit a test application (requires Spring Boot running on 8080):

```bash
curl -X POST http://localhost:8080/applications \
  -H "Content-Type: application/json" \
  -d '{"firstName":"Jane","lastName":"Doe"}'
```

After the outbox dispatcher runs (default delay 5 seconds), messages appear on the `application-submitted` topic in Kafka UI.

Optional — consume messages from the CLI:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic application-submitted \
  --from-beginning \
  --property print.headers=true
```

Press `Ctrl+C` to stop the consumer.

## Maintaining the stack

### Upgrading the Kafka image

The broker and init job must use the **same** image tag. In [`docker-compose.yml`](docker-compose.yml), update both:

```yaml
# kafka service
image: apache/kafka:4.3.0

# kafka-init service
image: apache/kafka:4.3.0
```

Then recreate the stack. For a **major version upgrade**, wipe the data volume so old broker metadata does not block startup:

```bash
docker compose down -v
docker compose pull
docker compose up -d
```

After `up -d`, confirm `kafka-init` completed and the topic exists (see [Verify setup](#verify-setup)).

### Upgrading Kafka UI

Change the `image:` tag under the `kafka-ui` service in `docker-compose.yml`, then:

```bash
docker compose pull kafka-ui
docker compose up -d kafka-ui
```

### Recreating the topic init job

`kafka-init` runs once per `docker compose up`. If you need it to run again (e.g. after removing the init container):

```bash
docker compose rm -f kafka-init
docker compose up -d
```

### Persistent data

Topic and broker data live in the Docker volume `event-driven-design-demo_kafka-data` (or `<project-name>_kafka-data`).

| Command | Effect on data |
|---------|----------------|
| `docker compose down` | Stops containers; **keeps** volume |
| `docker compose down -v` | Stops containers; **deletes** volume and all topics/messages |
| `docker compose restart kafka` | Restarts broker; **keeps** volume |

### Inspecting resources

List Compose volumes:

```bash
docker compose volumes
```

List images used by the stack:

```bash
docker compose images
```

## docker-compose.yml walkthrough

### `kafka` — single-node KRaft broker

Uses the official [`apache/kafka`](https://hub.docker.com/r/apache/kafka) image in **KRaft** mode (no ZooKeeper).

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

A healthcheck waits until the broker accepts connections before starting `kafka-ui` and `kafka-init`.

### `kafka-init` — one-shot topic creation

Runs after `kafka` is healthy and creates `application-submitted` if it does not exist. Matches `application.yml` and `@EmbeddedKafka` integration tests.

`restart: "no"` — container exits after success instead of restarting.

### `kafka-ui` — web console

[`provectuslabs/kafka-ui`](https://hub.docker.com/r/provectuslabs/kafka-ui) connects to `kafka:29092` inside the Docker network.

Mapped to **8081** on the host so it does not conflict with Spring Boot on **8080**.

## App integration

No Docker-specific changes are required in the application:

| Setting | Value |
|---------|-------|
| Bootstrap servers | `localhost:9092` (override with env var `KAFKA_BOOTSTRAP_SERVERS`) |
| Topic | `application-submitted` |
| Value format | Raw Avro `byte[]` (no Confluent Schema Registry) |
| Tests | `@EmbeddedKafka` — no Docker required for `mvn test` |

## Troubleshooting

### `docker: command not found` (Git Bash)

See [Windows: Docker not found in Git Bash](#windows-docker-not-found-in-git-bash).

### `docker compose pull` credential error

Same fix — add Docker Desktop's `bin` directory to `PATH` (see Windows section above).

### Port 8080 already in use

Spring Boot defaults to port **8080**. Kafka UI uses **8081** and does **not** occupy 8080.

1. Find what is listening:

   ```bash
   netstat -ano | findstr :8080
   ```

2. If it is a stale `EventDrivenDesignDemoApplication` or `spring-boot:run`, stop it:

   ```bash
   taskkill //PID <PID> //F
   ```

   In the IDE, click **Stop** on any running debug/run session for the app.

3. Do not run the app from both the IDE and `mvnw spring-boot:run` at the same time.

4. Verify the port is free, then start the app once:

   ```bash
   netstat -ano | findstr :8080
   ./mvnw spring-boot:run
   ```

### Port 9092 already in use

Another Kafka instance or service is bound to 9092. Stop it, or change the host port mapping in `docker-compose.yml` (and set `KAFKA_BOOTSTRAP_SERVERS` to match).

### Spring app cannot connect to Kafka

1. Confirm Kafka is healthy: `docker compose ps`
2. Wait for the healthcheck to pass (~30s on first start)
3. Use `localhost:9092` from the host — not `kafka:29092` (internal Docker hostname only)
4. Ensure Docker stack is running: `docker compose up -d`

### `kafka-init` exited with an error

```bash
docker compose logs kafka-init
```

Usually the broker was not ready. Retry:

```bash
docker compose up -d
```

### No messages in Kafka UI

1. Confirm Spring Boot is running and submitted an application
2. Outbox dispatcher runs on a fixed delay (default 5s in `application.yml`) — wait and refresh Kafka UI
3. Confirm Kafka is healthy: `docker compose ps`

### `docker compose exec` fails in Git Bash on Windows

Git Bash may rewrite paths like `/opt/kafka/...`. Prefix the command:

```bash
MSYS_NO_PATHCONV=1 docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Alternatively, run exec commands from PowerShell or CMD.

### `kafka` container unhealthy

Check broker logs:

```bash
docker compose logs kafka
```

If you recently upgraded Kafka major versions, wipe data and start fresh:

```bash
docker compose down -v
docker compose up -d
```
