CREATE SCHEMA IF NOT EXISTS wallet_schema;

CREATE TABLE IF NOT EXISTS wallet_schema.wallets (
    user_id UUID PRIMARY KEY,
    balance DECIMAL(12, 2) NOT NULL DEFAULT 0.00,
    payment_gateway_token VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS wallet_schema.transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES wallet_schema.wallets(user_id) ON DELETE CASCADE,
    amount DECIMAL(12, 2) NOT NULL,
    type VARCHAR(50) NOT NULL, -- e.g., 'DEPOSIT', 'WITHDRAW', 'BET_PLACED', 'BET_WON'
    reference_id UUID, -- Can be null for deposits/withdrawals, or bet_id
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS wallet_schema.pending_deposit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES wallet_schema.wallets(user_id) ON DELETE CASCADE,
    amount DECIMAL(12, 2) NOT NULL,
    client_secret VARCHAR(255), -- clientSecret from deposit request
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);