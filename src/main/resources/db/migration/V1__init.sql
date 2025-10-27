CREATE TABLE messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    message TEXT NOT NULL,
    username TEXT NOT NULL
);

ALTER TABLE messages REPLICA IDENTITY FULL;
