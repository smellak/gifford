# internal/eventstore — rules for implementing agents

- Only `AppendEvents` writes `events`; nothing updates or deletes an event (triggers refuse it).
- A schema change is a new `migrations/<YYYYMMDDHHMMSS>_<name>.sql`, timestamped with `date -u +%Y%m%d%H%M%S` at creation (never invented or backdated), sorting after every existing file. Merged migrations are never edited, renamed or deleted.
- Every migration has `-- +goose Up` and `-- +goose Down`; Down drops what Up created, in reverse order. Wrap each `CREATE TRIGGER` in `-- +goose StatementBegin` / `-- +goose StatementEnd`.
- No `BEGIN`, `COMMIT`, `PRAGMA`, `VACUUM` or `ATTACH` in a migration: the runner owns the transaction and the connection settings.
- Times are TEXT from `FormatTime`; booleans INTEGER 1/0; JSON columns TEXT bound as a Go `string`. `Store.DB().BeginTx(ctx, nil)` is `BEGIN IMMEDIATE`; read-only transactions pass `&sql.TxOptions{ReadOnly: true}`.
- Tables other than `events` are created here and written by their owners through `Store.DB()`; this package adds no queries or writers for them. The four channel tables are written only by `internal/channel`.
