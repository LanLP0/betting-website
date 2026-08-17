CREATE SCHEMA IF NOT EXISTS events_schema;

CREATE TABLE IF NOT EXISTS events_schema.events (
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
