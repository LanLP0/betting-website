CREATE SCHEMA IF NOT EXISTS mock_schema;

-- Payment method tokens (simulates external gateway's token vault)
CREATE TABLE IF NOT EXISTS mock_schema.payment_information (
    token VARCHAR(255) PRIMARY KEY NOT NULL,
    account_number VARCHAR(255) NOT NULL,
    account_name VARCHAR(255) NOT NULL,
    bank_name VARCHAR(255) NOT NULL,
    bank_code VARCHAR(255) NOT NULL
);

-- Pending deposits awaiting user confirmation on gateway portal
CREATE TABLE IF NOT EXISTS mock_schema.deposit_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    amount DECIMAL(19, 4) NOT NULL,
    client_secret VARCHAR(255) NOT NULL UNIQUE, -- Lookup key for user confirmation
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- pending, confirmed, expired
    webhook_url TEXT NOT NULL,
    webhook_secret VARCHAR(255) NOT NULL, -- HMAC-SHA256 secret
    expire_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mock_schema.payment_info_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_secret VARCHAR(255) NOT NULL UNIQUE, -- e.g. 'wallet-service', 'events-service'
    webhook_url TEXT NOT NULL,
    webhook_secret VARCHAR(255) NOT NULL, -- HMAC-SHA256 secret
    expire_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Webhook signing secrets (one per registered service/subscriber)
-- Used to compute HMAC-SHA256 signatures on outbound webhook payloads
CREATE TABLE IF NOT EXISTS mock_schema.webhook_secrets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name VARCHAR(255) NOT NULL UNIQUE, -- e.g. 'wallet-service', 'events-service'
    webhook_url TEXT NOT NULL,
    secret VARCHAR(255) NOT NULL, -- Shared HMAC-SHA256 secret
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Idempotency key store for mutation endpoints (withdraw, deposit confirmation)
-- Prevents double-processing on network retries
CREATE TABLE IF NOT EXISTS mock_schema.idempotency_keys (
    idempotency_key VARCHAR(255) PRIMARY KEY NOT NULL,
    response_status_code INT NOT NULL,
    response_body JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Webhook delivery log with retry tracking
CREATE TABLE IF NOT EXISTS mock_schema.webhook_deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    webhook_secret_id UUID NOT NULL REFERENCES mock_schema.webhook_secrets(id),
    event_type VARCHAR(100) NOT NULL, -- e.g. 'deposit.confirmed', 'payment.registered'
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- pending, delivered, failed
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Simulated events from odds supplier
CREATE TABLE IF NOT EXISTS mock_schema.events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'not-started', -- not-started, open, closed, settled
    teams TEXT ARRAY NOT NULL, -- array of team names
    winning_selection VARCHAR(255), -- winner selection for settlement
    odds DECIMAL(10, 2) ARRAY NOT NULL, -- array of odds corresponding to teams
    settled_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);