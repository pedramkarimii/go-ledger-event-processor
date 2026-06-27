CREATE TABLE processed_events (
event_key TEXT PRIMARY KEY,
event_type TEXT NOT NULL,
order_id TEXT NOT NULL,
processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE order_projections (
order_id TEXT PRIMARY KEY,
user_id TEXT NOT NULL,
side TEXT NOT NULL,
status TEXT NOT NULL,
base_asset_code TEXT NOT NULL,
quote_asset_code TEXT NOT NULL,
reserved_asset_code TEXT NOT NULL,
reserved_amount NUMERIC(36, 18) NOT NULL,
created_at TIMESTAMPTZ NOT NULL,
updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX order_projections_user_id_idx ON order_projections (user_id);
CREATE INDEX order_projections_status_idx ON order_projections (status);
