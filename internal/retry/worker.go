package retry

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/prananallari/mini-kafka/internal/broker"
)

type Worker struct {
	s  *broker.Service
	db *sql.DB
}

func NewWorker(s *broker.Service, db *sql.DB) *Worker {
	return &Worker{s: s, db: db}
}

func (w *Worker) Start(ctx context.Context) {
	timer := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-timer.C:
			w.processRetries(ctx)
		case <-ctx.Done():
			return
		}
	}

}

func (w *Worker) processRetries(ctx context.Context) {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()

	query := `
	SELECT id, topic_name, group_name, payload, attempts
	FROM retry_messages
	WHERE status = 'pending'
	FOR UPDATE SKIP LOCKED
	LIMIT 10
	`

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer rows.Close()

	type msg struct {
		id       int
		topic    string
		group    string
		payload  []byte
		attempts int
	}

	var messages []msg

	for rows.Next() {
		var m msg
		if err := rows.Scan(&m.id, &m.topic, &m.group, &m.payload, &m.attempts); err != nil {
			continue
		}
		messages = append(messages, m)
	}

	maxAttempts := 5

	for _, m := range messages {

		// ---- DLQ ----
		if m.attempts >= maxAttempts {
			if _, err := tx.ExecContext(ctx, `
				UPDATE retry_messages
				SET status = 'dead',
				    last_attempted_at = NOW()
				WHERE id = $1
			`, m.id); err != nil {
				slog.Error("failed to mark dead", "id", m.id, "err", err)
			}
			continue
		}

		// ---- retry ----
		err := w.s.Publish(ctx, m.topic, m.group, m.payload)

		if err == nil {
			// success → delete
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM retry_messages WHERE id = $1`,
				m.id,
			); err != nil {
				slog.Error("failed to delete retry", "id", m.id, "err", err)
			}
			continue
		}

		// failed → increment attempts
		if _, err := tx.ExecContext(ctx, `
			UPDATE retry_messages
			SET attempts = attempts + 1,
			    last_attempted_at = NOW()
			WHERE id = $1
		`, m.id); err != nil {
			slog.Error("failed to update retry", "id", m.id, "err", err)
		}
	}

	if err := tx.Commit(); err != nil {
		slog.Error("failed to commit retry txn", "err", err)
	}
}
