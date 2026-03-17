package broker

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/prananallari/mini-kafka/internal/partition"
	"github.com/redis/go-redis/v9"
)

type Service struct {
	db  *sql.DB
	rdb *redis.Client
}

type ServiceInterface interface {
	CreateTopic(ctx context.Context, name string, partitionCount int) error
	CreateGroup(ctx context.Context, groupName string) error
	Publish(ctx context.Context, topicName, key string, payload []byte) error
	Consume(ctx context.Context, topicName, groupName string, partitionIndex, limit int) ([][]byte, error)
	Retry(ctx context.Context, topicName, groupName string, offset, partitionIndex int, payload string) error
}

func NewService(db *sql.DB, rdb *redis.Client) *Service {
	return &Service{db, rdb}
}

func getRedisKey(name string, partitionIndex int) string {
	return "broker:" + name + ":" + strconv.Itoa(partitionIndex)
}

func (s *Service) CreateTopic(ctx context.Context, name string, partitionCount int) error {
	topicQuery := `INSERT INTO topics (name, partition_count) VALUES ($1, $2) RETURNING id`

	var id string
	err := s.db.QueryRowContext(ctx, topicQuery, name, partitionCount).Scan(&id)
	if err != nil {
		return err
	}

	partitionQuery := `INSERT INTO partitions (topic_id, partition_index, redis_key) VALUES ($1, $2, $3)`

	for i := range partitionCount {
		key := getRedisKey(name, i)
		_, err := s.db.ExecContext(ctx, partitionQuery, id, i, key)
		if err != nil {
			return err
		}

	}
	return nil
}

func (s *Service) CreateGroup(ctx context.Context, groupName string) error {
	query := `INSERT INTO consumer_groups (name) VALUES ($1)`

	_, err := s.db.ExecContext(ctx, query, groupName)
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) Publish(ctx context.Context, topicName, key string, payload []byte) error {
	query := `SELECT partition_count FROM topics where name = $1`
	var partitionCount uint32
	err := s.db.QueryRowContext(ctx, query, topicName).Scan(&partitionCount)
	if err != nil {
		return err
	}

	hash := partition.GetHash(key)
	partitionIndex := hash % partitionCount
	redisKey := getRedisKey(topicName, int(partitionIndex))
	err = s.rdb.RPush(ctx, redisKey, payload).Err()
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) Consume(ctx context.Context, topicName, groupName string, partitionIndex, limit int) ([][]byte, error) {
	var offset int
	redisKey := getRedisKey(topicName, partitionIndex)
	offsetQuery := `SELECT current_offset FROM consumer_offsets WHERE group_name = $1 AND topic_name = $2 AND partition_index = $3`

	err := s.db.QueryRowContext(ctx, offsetQuery, groupName, topicName, partitionIndex).Scan(&offset)
	if err == sql.ErrNoRows {
		offset = 0
	} else if err != nil {
		return nil, err
	}

	data, err := s.rdb.LRange(ctx, redisKey, int64(offset), int64(offset+limit-1)).Result()

	if err != nil {
		return nil, err
	}
	actualRecordsRead := len(data)
	if actualRecordsRead == 0 {
		return nil, nil
	}

	newOffset := offset + actualRecordsRead

	upsertQuery := `INSERT INTO consumer_offsets (group_name, topic_name, partition_index, current_offset) VALUES ($1, $2, $3, $4) ON CONFLICT (group_name, topic_name, partition_index) DO UPDATE SET current_offset = EXCLUDED.current_offset`

	_, err = s.db.ExecContext(ctx, upsertQuery, groupName, topicName, partitionIndex, newOffset)

	if err != nil {
		return nil, err
	}

	result := make([][]byte, actualRecordsRead)
	for i, v := range data {
		result[i] = []byte(v)
	}

	return result, nil

}

func (s *Service) Retry(ctx context.Context, topicName, groupName string, offset, partitionIndex int, payload string) error {
	query := `
	INSERT INTO retry_messages (
		topic_name,
		group_name,
		partition_index,
		message_offset,
		payload,
		attempts,
		status,
		last_attempted_at
	)
	VALUES ($1, $2, $3, $4, $5, 0, 'pending', NOW())
	ON CONFLICT (topic_name, group_name, partition_index, message_offset)
	DO NOTHING
	`

	_, err := s.db.ExecContext(ctx, query, topicName, groupName, partitionIndex, offset, []byte(payload))

	return err
}
