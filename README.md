# Betting Website

This is a backend practice, only for educational purpose and not for deployment. The project feature a full frontend - backend infrastructure with Redis, PostgreSQL... providing a playground both for learning and exploring.

## Scale

Big US sports betting website:

- **Peak Concurrent Users: 1,000,000 to 3,000,000+** active sessions.
- **Transactions Per Second (TPS): 50,000 to 100,000+ TPS**. This includes not just bets being placed, but users refreshing pages, live-odds ticking every second, and cash-out calculations.

# Development

For a full specification of this project, see [specs.md](specs.md).

## Completed Highlights & TODO

- [x] RS256 Asymmetric JWT authentication with RSA keypair verification
- [x] Webhook HMAC signature verification with 5-minute replay protection and constant-time comparison
- [x] OpenResty Lua Redis fast-path direct read for odds (`/api/v1/events/:id/odds`)
- [x] Redis-backed real-time odds reading with RabbitMQ RPC fallback to event service
- [x] Mock service payment gateway deposit/withdraw flows with idempotency keys
- [x] PaymentGateway trait abstraction for financial operations
- [x] ClickHouse event logging consumer pipeline (`metrics_schema.events_log`) with trace ID and key metrics indexing
- [x] Argon2 password hashing in user & management service
- [x] Swarm deployment script cleanup (`deploy-swarm.sh` drops previous stack)
- [x] Server-wide request tracing (`X-Request-ID` / `X-Trace-ID` propagation)
- [x] Complete OpenAPI / Swagger UI specs for management service
- [ ] Comprehensive unit and integration test suite
- [ ] Frontend React client (Planned for future phase)
