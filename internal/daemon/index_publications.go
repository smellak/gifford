package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
)

// ErrInvalidPublication means a pane index publication has invalid fields.
var ErrInvalidPublication = errors.New("invalid pane index publication")

// IndexPublicationStore records Alta index publications using caller-owned
// short transactions, mirroring AttemptStore: the caller opens the
// transaction, the store only inserts. The publication timestamp comes from
// the provision result, captured by provision right after the U/index
// directory fsync; the store never invents it, so it needs no clock.
type IndexPublicationStore struct{}

// Record inserts one pane index publication row.
func (IndexPublicationStore) Record(
	ctx context.Context,
	tx *sql.Tx,
	panelKey string,
	publishedAt time.Time,
) error {
	if tx == nil || panelKey == "" || publishedAt.IsZero() {
		return fmt.Errorf("%w: record", ErrInvalidPublication)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO pane_index_publications
		(panel_key, published_at)
		VALUES (?, ?)`,
		panelKey, eventstore.FormatTime(publishedAt))
	if err != nil {
		return fmt.Errorf("%w: insert: %w", ErrInvalidPublication, err)
	}
	return nil
}
