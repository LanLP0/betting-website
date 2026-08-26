# System Architecture Specification: Production-Simulation Betting Platform

This document outlines the high-level architecture, event-driven design, infrastructure constraints, and directory layout for the full-scale production simulation of our sports betting platform. The system is designed using **Rust** for maximum performance and memory safety, utilizing an **Event-Driven Architecture (EDA)** over a single-node **Docker Swarm** environment.

---

## 1. System Architecture Overview

The architecture transitions from traditional tight coupling to an asynchronous, decoupled system. Services isolate their domain data through logical database schemas and communicate exclusively via an asynchronous message broker, except for fast read-heavy queries served directly via an in-memory cache.

![](./architechture-graph.png)

```mermaid
flowchart LR
    %% Client & Gateway Layer
    FE[Frontend] --> API[Main Server<br>Bearer JWT Auth<br>Nginx]

    %% Main API Routing to Microservices
    API --> US[User Services<br>CRUD, Auth, State]
    API --> WP[Wallet & Payments<br>Bal, Deposit, Withdraw]
    API --> BET[Betting<br>CRUD, GET bet-metrics]
    API --> EV[Events<br>GET events, realtime metrics, odds]
    API --> MM[Management & Metrics]
    API --> NS[Notification Service]
    API --> MS[Mock Service]

    %% External Mock Service
    WP <--> MS
    EV <--> MS

    %% Async Message Bus Connections
    EB[Event Bus<br>RabbitMQ]
    EB <--> US
    EB <--> WP
    EB <--> BET
    EB <--> EV
    EB <--> NS

    %% Management & Metrics Warehouse
    MM --> MW[Metric Warehouse<br>ClickHouse]
    MM --> EB

    %% Notification Service
    NS -->|publish notification| NWWS[Notification Worker (websocket)]
    NS --> NWWS %% Spawn workers based on demand
    NS <--|client subsribe to notification, e.g. specific odds| NWWS

    %% Mock Service


    %% Data Layer
    DB[(Datastore<br>PostgreSQL)]
    CACHE[(Cache<br>Redis)]

    US <--> DB
    WP <--> DB
    BET <--> DB
    EV <--> DB
    EB <--> DB
    NS <--> DB
    MS <--> DB

    US <--> CACHE
    BET <-- CACHE %% Read odds
    EV <--> CACHE
    CACHE -->|direct odds read from Redis| NWWS
```

For demonstration purposes, we only use a single PostgreSQL with different schemas and a Redis instance. But services cannot cross-read each other's data (DB isolation).

---

## 2. Component Specifications

### 2.1 Edge & Routing Layer

- **Main Server (Nginx):** Acts as the reverse proxy and API Gateway. It is responsible for SSL termination, global rate-limiting, and stateless **Bearer JWT Authentication** verification. Valid requests are proxied internally to the corresponding application service.

### 2.2 Application Services (Rust Backend)

Every service is implemented as an independent Rust binary utilizing the high-performance async framework `actix-web`.

- **User Services:** Manages registration, logins, and profile state.
- **Wallet & Payments:** Coordinates the strict financial ledger. Responsible for balancing deposits, withdrawals, and locking funds. It interacts solely with the database layer using explicit **pessimistic database locks** to prevent race conditions or double-spending (ACID compliance).
- **Betting Service:** Orchestrates the lifecycle of a bet (Pending $\rightarrow$ Confirmed $\rightarrow$ Settled). It acts as a Saga coordinator or workflow controller using event state flags.
- **Events Service:** Exposes fast read-only APIs for match data and current odds. It processes inbound mock odds feeds and constantly pushes updates out via WebSockets. When an event is online/avaiable for betting, its odds must be published to Redis (odds:event_id).
- **Notification Service:** Real-time communication hub between backend domain events and user frontends via WebSocket.
  - **Storage Strategy:** Notifications are split into two categories:
    - _Broadcast (ephemeral):_ High-frequency live updates (odds changes, score updates) are pushed directly over WebSocket via Redis Pub/Sub fanout. These are **not persisted** to PostgreSQL — they are transient, in-memory events. This avoids writing millions of rows per minute during peak load.
    - _Targeted (persistent):_ User-specific alerts (bet confirmed, bet settled, payout credited, deposit completed) are consumed from RabbitMQ (`notification.push`) and persisted to `notification_schema.user_notifications` asynchronously via batch inserts, then pushed to the user's WebSocket connection.
  - **Horizontal Scaling:** WebSocket connections are stateful (sticky to a single pod). To ensure a domain event on Worker Node A reaches a client connected to WebSocket Node B, the Notification Worker uses **Redis Pub/Sub** as a cross-node broadcast channel. Each WebSocket server instance subscribes to a shared Redis channel and forwards matching messages to its local connections.
- **Management & Metrics:** Exposes APIs for management and metrics.

### 2.3 Event Bus & Messaging Layer

- **RabbitMQ:** Serves as our asynchronous central nervous system. Services never call each other. Instead, workflows execute sequentially via published events.
  - _Example Placement Flow:_ `Betting` saves a `Pending` state $\rightarrow$ Publishes `BetRequested` to RabbitMQ $\rightarrow$ `Wallet` consumes message and safely reserves balance $\rightarrow$ `Wallet` publishes `FundsLocked` $\rightarrow$ `Betting` switches state to `Confirmed`.

### 2.4 Persistent & Cache Layer

- **Datastore (PostgreSQL):** A single robust instance containing separated **Logical Schemas** per domain microservice (`users_schema`, `wallet_schema`, `bets_schema`). Cross-schema reads or modifications inside application logic are strictly forbidden to ensure schema boundary isolation.
- **Cache (Redis):** Handles transient state and high read data (such as live odds feeds). The **Wallet & Payments** service avoids Redis to ensure financial data retains maximum transactional consistency directly via Postgres.
- **Metric Warehouse (ClickHouse):** A dedicated column-oriented database optimized for analytics. Logging streams, tracking metrics, and historical line modifications are processed out of RabbitMQ and into ClickHouse to avoid overloading PostgreSQL.

### 2.5 The Nginx "Fast Path" for Odds

- **The Concept:** For a high-throughput betting site, a common production trick is to configure Nginx (often using OpenResty or Kong) to read directly from Redis for public, unauthenticated routes.

- **The Result:** When 10,000 users refresh the odds page, the request never even touches your Rust Events service. Nginx serves the JSON straight from Redis memory.

### 2.6 Mock External Service

The Mock Service simulates two external third-party providers that a real betting platform would integrate with. It is a standalone service with its own schema (`mock_schema`) — it is architecturally "outside" the platform boundary.

#### Payment Gateway Mock

Simulates a payment processor (e.g., Stripe, Adyen). Supports deposit, withdrawal, and payment method registration flows with their own portal and callback URLs.

- **Webhook Signature Verification (HMAC-SHA256):** All outbound webhooks from the Mock Payment Gateway to internal services (e.g., deposit confirmation callbacks) are signed using HMAC-SHA256. The signature is computed over the raw request body concatenated with a timestamp, using a shared `webhook_secret` provisioned during service registration:

  ```
  X-Webhook-Signature: t=<unix_timestamp>,v1=<HMAC-SHA256(timestamp.body, webhook_secret)>
  ```

  The receiving service (Wallet Service) must:
  1. Extract the timestamp from the header and reject if older than 5 minutes (replay protection).
  2. Recompute the HMAC over `timestamp.body` using its stored `webhook_secret`.
  3. Compare signatures using constant-time comparison to prevent timing attacks.

- **Idempotency Enforcement:** Every webhook delivery includes a unique `Idempotency-Key` header (the `transaction_id`). The receiving service must store processed `transaction_id` values and skip duplicate deliveries. The Mock Gateway itself enforces idempotency on mutation endpoints (`/withdraw`) via the `idempotencyKey` field — repeated requests with the same key return the original response without re-executing.

- **Webhook Retry with Backoff:** If the callback URL returns a non-2xx status or times out (5s), the Mock Gateway retries delivery up to 3 times with exponential backoff (1s → 5s → 25s). After 3 failures, the webhook is marked as `failed` and logged.

#### Odds Supplier Mock

Simulates a live sports data feed provider. Periodically generates new events, drifts odds, updates scores, and settles matches.

#### Chaos & Resilience Simulation

The Mock Service supports configurable failure injection to stress-test the platform's retry logic, circuit breakers, and Saga compensation flows:

| Parameter              | Default | Description                                                              |
| ---------------------- | ------- | ------------------------------------------------------------------------ |
| `FAILURE_RATE`         | `0.0`   | Probability (0.0–1.0) that any mock endpoint returns HTTP 500/503.       |
| `LATENCY_MIN_MS`       | `0`     | Minimum artificial delay (ms) added to responses.                        |
| `LATENCY_MAX_MS`       | `0`     | Maximum artificial delay (ms) added to responses.                        |
| `WEBHOOK_TIMEOUT_RATE` | `0.0`   | Probability that an outbound webhook "hangs" (simulating network issue). |

These are set via environment variables on the Mock Service container, allowing operators to toggle chaos during integration testing without code changes.

---

## 3. Infrastructure & Orchestration (Docker Swarm)

To simulate full-scale datacenter isolation on a single physical host machine, we configure a single-node **Docker Swarm**.

- **Network Strategy:** Services are joined across an abstract **Overlay Network** (`driver: overlay`). This allows containers to find one another natively using Docker's internal DNS resolution (e.g., connecting a database string via `postgres://db-host:5432`) without exposing high-risk system ports directly to the physical machine host interface.
- **Process Resilience:** High Availability is managed via systemic restart policies. If an application process crashes or panics, the orchestrator handles process reinitialization natively without manual intervention.
- **Image Management:** Because Docker Swarm operates over stack deployments, deployment execution ignores implicit local `build:` attributes. Application images must be compiled locally and written directly into the local daemon image cache prior to launching deployments.

---

## 4. API Specifications

API specs are displayed with Swagger UI at `/api/swagger` (hosted by Management Service) and autogenerated using `utoipa`.

| Service              | Endpoint                                    | Description                                                  | Authentication                       |
| -------------------- | :------------------------------------------ | :----------------------------------------------------------- | :----------------------------------- |
| User Service         | `POST /api/v1/auth/verify`                  | Verify a JWT and returns decoded http headers with user info | \_                                   |
| User Service         | `POST /api/v1/auth/register`                | Register a new user                                          | \_                                   |
| User Service         | `POST /api/v1/auth/login`                   | Login with username and password (Salting and Hashing)       | \_                                   |
| User Service         | `GET /api/v1/users/:id`                     | Get user profile                                             | Bearer JWT                           |
| User Service         | `PUT /api/v1/users/:id`                     | Update user profile                                          | Bearer JWT                           |
| User Service         | `DELETE /api/v1/users/:id`                  | Delete user                                                  | Bearer JWT                           |
| User Service         | `GET /api/v1/users`                         | Get all users                                                | Bearer JWT/Admin                     |
| Wallet Service       | `GET /api/v1/wallet/:id`                    | Get wallet balance for user id                               | Bearer JWT                           |
| Wallet Service       | `POST /api/v1/wallet/:id/deposit`           | Request to deposit funds (via payment gateway)               | Bearer JWT                           |
| Wallet Service       | `POST /api/v1/wallet/:id/withdraw`          | Request to withdraw funds (via payment gateway)              | Bearer JWT                           |
| Wallet Service       | `POST /api/v1/wallet/:id/register`          | Request payment method registration (via payment gateway)    | Bearer JWT                           |
| Wallet Service       | `POST /api/v1/wallet/:id/callback/payment`  | Callback from payment gateway to confirm payment request     | Client Secret (attached in metadata) |
| Wallet Service       | `POST /api/v1/wallet/:id/callback/register` | Callback from payment gateway to confirm payment request     | Client Secret (attached in metadata) |
| Event Service        | `GET /api/v1/events`                        | Get all events                                               | \_                                   |
| Event Service        | `GET /api/v1/events/:id`                    | Get event details                                            | \_                                   |
| Event Service        | `GET /api/v1/events/:id/odds`               | Get event odds                                               | \_                                   |
| Event Service        | `POST /api/v1/events/callback`              | Callback from event supplier to updates to events            | Client Secret (attached in metadata) |
| Event Service        | `POST /api/v1/events/mgmt/add`              | Add an event                                                 | Bearer JWT/Admin                     |
| Event Service        | `DELETE /api/v1/events/mgmt/:id`            | Delete an event                                              | Bearer JWT/Admin                     |
| Event Service        | `POST /api/v1/events/mgmt/:id/settle`       | Settle an event                                              | Bearer JWT/Admin                     |
| Betting Service      | `POST /api/v1/bets`                         | Place a bet                                                  | Bearer JWT                           |
| Betting Service      | `DELETE /api/v1/bets/:id`                   | Cancel a bet                                                 | Bearer JWT                           |
| Betting Service      | `GET /api/v1/bets/event/:id`                | Get bet metrics for event                                    | Bearer JWT                           |
| Betting Service      | `GET /api/v1/bets/user/:id`                 | Get all bets by user                                         | Bearer JWT                           |
| Management Service   | `GET /api/v1/management/metrics`            | Get metrics                                                  | Bearer JWT/Admin                     |
| All                  | `GET /api/v1/management/swagger`            | Get API specifications                                       | Bearer JWT/Admin                     |
| Notification Service | `GET /api/v1/notifications/:id`             | Get all notifications for a user                             | Bearer JWT                           |
| Notification Service | `PUT /api/v1/notifications/read`            | Mark (all) notification(s) as read (select via payload)      | Bearer JWT                           |
| Notification Service | `GET /api/v1/notification/websocket`        | Connect with websocket                                       | Bearer JWT                           |
| Notification Service | `RABBITMQ notification.push`                | Notify a user                                                | Internal                             |

**\*** Ensure `:id` is the same as user id embedded in JWT Token
**\*** Payment gateway should be put behind a general rust trait
**\*** All requests (either via API or RabbitMQ) should have a Trace ID, and should log to a central database (another clickhouse instance?)

> `/Admin` &mdash; for admin only

Bearer JWT with asymetric signature verification:

```json
{
  "header": {
    "alg": "RS256",
    "typ": "JWT"
  },
  "payload": {
    "sub": "1234567890",
    "username": "JohnDoe",
    "role": "admin",
    "iat": 1516239022,
    "exp": 1516239022
  },
  "signature": "..."
}
```

APIs are intended for public access via Nginx reverse proxy.
Services shouldn't directly communicate with one another
but instead use Saga pattern with RabbitMQ as event bus for
internal messaging.

### Mock Service

| Endpoint                              | Description                                                      | Request                                    | Response                                                   |
| ------------------------------------- | ---------------------------------------------------------------- | ------------------------------------------ | ---------------------------------------------------------- |
| `POST /mock/api/v1/deposit/request`   | Request a deposit (server to server)                             | `{amount, responseWebhook, metadata}`      | `{status, clientSecret, transactionId}`                    |
| `POST /mock/api/v1/register/request`  | Request a payment method information register (server to server) | `{serviceName, responseWebhook, metadata}` | `{status, clientSecret}` + paymentToken via webhook        |
| `POST /mock/deposit`                  | Deposit frontend (confirm transaction)                           | `{clientSecret}`                           | `{status, transactionId}`                                  |
| `POST /mock/register`                 | Register a payment method                                        | `{clientSecret}`                           | `{status}`                                                 |
| `POST /mock/api/v1/withdraw`          | Make a withdraw to user's bank account (server to server)        | `{amount, gatewayToken, idempotencyKey}`   | `{status, transactionId}`                                  |
| `GET /mock/api/v1/events`             | Get all events (simulated)                                       | \_                                         | `{events: [{id, name, description, status, teams, odds}]}` |
| `GET /mock/api/v1/events/:id`         | Get event details (simulated)                                    | \_                                         | `{id, name, description, status, teams, odds}`             |
| `GET /mock/api/v1/events/:id/odds`    | Get event odds (simulated)                                       | \_                                         | `{odds: [{team, value}]}`                                  |
| `POST /mock/api/v1/events/subscribe`  | Subcribe to update (new event, odds...) via webhook              | `{webhookUrl, metadata}`                   | `{status}`                                                 |
| `POST /mock/api/v1/events/add`        | Add an event (No implement)                                      | \_                                         | `{status}`                                                 |
| `DELETE /mock/api/v1/events/:id`      | Delete an event (No implement)                                   | \_                                         | `{status}`                                                 |
| `POST /mock/api/v1/events/:id/settle` | Settle an event (No implement)                                   | \_                                         | `{status}`                                                 |

**\*** Mock payment gateway generates paymentToken via user information, accepts any request after user confirmation
**\*** Mock events service will periodically randomly add new events, update existing ones, settle, update odds...
**\*** All outbound webhooks are signed with `X-Webhook-Signature` (HMAC-SHA256) — see §2.6 for verification protocol
**\*** All mutation endpoints enforce idempotency via `Idempotency-Key` or `transactionId` to prevent double-processing on retries

## 5. Directory Blueprint & Folder Structure

```text
<PROJECT_ROOT>/
├── .gitignore
├── README.md
├── docker-compose.yml              # Central stack composition layout for Swarm
├── Makefile                        # Tooling scripts for building images and starting stack
├── config/                         # Global configuration presets
│   ├── nginx.conf                  # Gateway configurations and JWT verification definitions
│   └── rabbitmq.conf               # Event bus queue configurations
│
├── src/                            # Isolated application domain containers
│   ├── frontend/                   # React application
│   ├── user-service/
│   │   ├── Cargo.toml
│   │   ├── Dockerfile
│   │   └── src/
│   │
│   ├── wallet-service/
│   │   ├── Cargo.toml
│   │   ├── Dockerfile
│   │   └── src/
│   │
│   ├── betting-service/
│   │   ├── Cargo.toml
│   │   ├── Dockerfile
│   │   └── src/
│   │
│   └── events-service/
│       ├── Cargo.toml
│       ├── Dockerfile
│       └── src/
└── migrations/                     # Data initialization mappings
    ├── postgres/                   # Schema separation initializations
    │   ├── 0001_init_user_schema.sql
    │   ├── 0002_init_wallet_schema.sql
    │   ├── 0003_init_bets_schema.sql
    │   └── 0004_init_events_schema.sql
    └── clickhouse/                 # Columnar layout definitions
        └── 0001_init_metrics.sql

```

---

## 6. Deployment Orchestration Blueprint

To launch this architecture onto the single-node cluster environment, deployment scripts wrap the underlying configurations into unified execution steps.

1. **Initialize Swarm Environment:**

```bash
docker swarm init

```

2. **Seed Secure Passphrases:**

```bash
echo "SuperSecretProductionPassword" | docker secret create db_password -

```

3. **Compile Services:**

```bash
docker build -t local/betting-service:latest ./src/betting-service

```

4. **Launch Stack Infrastructure:**

```bash
docker stack deploy -c docker-compose.yml betting_system

```
