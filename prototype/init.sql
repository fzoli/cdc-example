CREATE TABLE messages (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  create_time TIMESTAMPTZ DEFAULT now(),
  message TEXT NOT NULL,
  username TEXT NOT NULL
);

-- required to get 'before' in case of update/delete
ALTER TABLE messages REPLICA IDENTITY FULL;

