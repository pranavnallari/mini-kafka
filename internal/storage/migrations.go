package storage

import "database/sql"

func InitSchema(db *sql.DB) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS topics (
			id SERIAL PRIMARY KEY,
			name TEXT UNIQUE NOT NULL,
			partition_count INT NOT NULL
		);`,

		`CREATE TABLE IF NOT EXISTS partitions (
			id SERIAL PRIMARY KEY,
			topic_id INT REFERENCES topics(id) ON DELETE CASCADE,
			partition_index INT NOT NULL,
			redis_key TEXT NOT NULL,
			UNIQUE (topic_id, partition_index)
		);`,

		`CREATE TABLE IF NOT EXISTS consumer_groups (
			id SERIAL PRIMARY KEY,
			name TEXT UNIQUE NOT NULL
		);`,

		`CREATE TABLE IF NOT EXISTS consumer_offsets (
			group_name TEXT NOT NULL,
			topic_name TEXT NOT NULL,
			partition_index INT NOT NULL,
			current_offset INT NOT NULL DEFAULT 0,
			PRIMARY KEY (group_name, topic_name, partition_index)
		);`,
		`CREATE TABLE IF NOT EXISTS retry_messages (
			id SERIAL PRIMARY KEY,
			topic_name TEXT NOT NULL,
			partition_index INT NOT NULL,
			message_offset INT NOT NULL,
			group_name TEXT NOT NULL,
			payload TEXT NOT NULL,
			attempts INT NOT NULL DEFAULT 0,
			last_attempted_at TIMESTAMP NOT NULL,
			status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'dead')),
			UNIQUE (topic_name, group_name, partition_index, message_offset)
		);`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			return err
		}
	}

	return nil
}
