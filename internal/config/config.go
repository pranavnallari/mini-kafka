package config

import (
	"errors"
	"os"
)

type LocalConfig struct {
	RedisAddr     string
	RedisPassword string
	PostgresURL   string
	AppPort       string
}

func Load() (*LocalConfig, error) {
	var RedisAddr, PostgresURL, RedisPassword, AppPort string
	if RedisAddr = os.Getenv("REDIS_ADDR"); RedisAddr == "" {
		return nil, errors.New("missing env : REDIS_ADDR")
	}
	if RedisPassword = os.Getenv("REDIS_PASSWORD"); RedisPassword == "" {
		return nil, errors.New("missing env : REDIS_PASSWORD")
	}
	if PostgresURL = os.Getenv("POSTGRES_URL"); PostgresURL == "" {
		return nil, errors.New("missing env : POSTGRES_URL")
	}
	if AppPort = os.Getenv("APP_PORT"); AppPort == "" {
		return nil, errors.New("missing env : APP_PORT")
	}

	config := LocalConfig{
		RedisAddr:     RedisAddr,
		RedisPassword: RedisPassword,
		PostgresURL:   PostgresURL,
		AppPort:       AppPort,
	}

	return &config, nil
}
