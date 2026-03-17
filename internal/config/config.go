package config

import (
	"errors"
	"os"
	"strconv"
)

type LocalConfig struct {
	RedisAddr   string
	PostgresURL string
	AppPort     int
}

func Load() (*LocalConfig, error) {
	var RedisAddr, PostgresURL, AppPort string
	if RedisAddr = os.Getenv("REDIS_ADDR"); RedisAddr == "" {
		return nil, errors.New("missing env : REDIS_ADDR")
	}
	if PostgresURL = os.Getenv("POSTGRES_URL"); PostgresURL == "" {
		return nil, errors.New("missing env : POSTGRES_URL")
	}
	if AppPort = os.Getenv("APP_PORT"); AppPort == "" {
		return nil, errors.New("missing env : APP_PORT")
	}

	AppPortInt, err := strconv.Atoi(AppPort)
	if err != nil {
		return nil, err
	}
	config := LocalConfig{
		RedisAddr:   RedisAddr,
		PostgresURL: PostgresURL,
		AppPort:     AppPortInt,
	}

	return &config, nil
}
