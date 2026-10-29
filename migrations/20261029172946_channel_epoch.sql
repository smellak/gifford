-- +goose Up
-- The node control epoch: one row, written only by internal/channel.
CREATE TABLE channel_epoch (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  epoch INTEGER NOT NULL CHECK (epoch >= 0)
);
INSERT INTO channel_epoch (id, epoch) VALUES (1, 0);

-- +goose Down
DROP TABLE channel_epoch;
