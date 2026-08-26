CREATE SCHEMA IF NOT EXISTS notification_schema;

-- Notification types:
--   'bet_confirmed'     - Bet placement confirmed
--   'bet_settled'       - Bet outcome determined (win/loss)
--   'payout_credited'   - Winnings added to wallet
--   'deposit_complete'  - Deposit transaction completed
--   'withdraw_complete' - Withdrawal processed
--   'system'            - Platform announcements

CREATE TABLE IF NOT EXISTS notification_schema.user_notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    notification_type VARCHAR(50) NOT NULL DEFAULT 'system',
    title VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}', -- Notification content: {"content": [{type, ...}], ...} (type is text, image, video, action)
    status VARCHAR(50) NOT NULL DEFAULT 'unread', -- unread, read
    read_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Primary query: fetch unread notifications for a user
CREATE INDEX IF NOT EXISTS idx_user_notifications_user_status
    ON notification_schema.user_notifications (user_id, status);

-- Timeline query: fetch recent notifications ordered by time
CREATE INDEX IF NOT EXISTS idx_user_notifications_user_created
    ON notification_schema.user_notifications (user_id, created_at DESC);

-- Settings are not yet supported in this implementation