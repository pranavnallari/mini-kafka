package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prananallari/mini-kafka/internal/api"
	"github.com/prananallari/mini-kafka/internal/broker"
	"github.com/prananallari/mini-kafka/internal/config"
	"github.com/prananallari/mini-kafka/internal/storage"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	defer stop()

	conf, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	postgresDB, err := storage.NewPostgres(conf.PostgresURL)
	if err != nil {
		slog.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}

	if err = storage.InitSchema(postgresDB); err != nil {
		slog.Error("failed to initialise schema", "error", err)
		os.Exit(1)
	}

	redisDB := storage.NewRedis(conf.RedisAddr, conf.RedisPassword)
	slog.Info("connected to postgres and redis")
	defer postgresDB.Close()
	defer redisDB.Close()

	serv := broker.NewService(postgresDB, redisDB)

	handler := api.NewHandler(serv)
	mux := api.NewRouter(handler)

	srv := &http.Server{Addr: ":" + conf.AppPort, Handler: mux}
	slog.Info("server starting", "port", conf.AppPort)

	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutdown signal received")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	} else {
		slog.Info("server stopped cleanly")
	}
}
