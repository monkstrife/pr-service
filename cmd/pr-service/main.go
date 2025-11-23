package main

import (
	"log/slog"
	"net/http"
	"os"

	"pr-service/internal/config"
	prhandlers "pr-service/internal/http-server/handlers/pullrequest"
	statshandlers "pr-service/internal/http-server/handlers/stats"
	teamhandlers "pr-service/internal/http-server/handlers/team"
	userhandlers "pr-service/internal/http-server/handlers/users"
	"pr-service/internal/lib/logger/handlers/slogpretty"
	"pr-service/internal/lib/logger/sl"
	"pr-service/internal/storage/sqlite"

	mwLogger "pr-service/internal/http-server/middleware/logger"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	_ "github.com/mattn/go-sqlite3"
)

const (
	envLocal = "local"
	envProd  = "prod"
	envDev   = "dev"
)

func main() {
	// Загружаем конфиг
	cfg := config.MustLoad()

	// Инициализируем логгер
	log := setupLogger(cfg.Env)

	// Инициализируем хранилище
	storage, err := sqlite.New(cfg.StoragePath)
	if err != nil {
		log.Error("failed to init storage", sl.Err(err))
		os.Exit(1)
	}

	// Инициализируем роутер
	router := chi.NewRouter()

	// Общие middlewares
	router.Use(middleware.RequestID)
	router.Use(middleware.Logger)
	router.Use(mwLogger.New(log))
	router.Use(middleware.Recoverer)
	router.Use(middleware.URLFormat)

	// Маршруты

	// Teams
	router.Post("/team/add", teamhandlers.Add(log, storage))
	router.Get("/team/get", teamhandlers.Get(log, storage))
	router.Post("/team/deactivateUsers", teamhandlers.DeactivateUsers(log, storage))

	// Users
	router.Post("/users/setIsActive", userhandlers.SetIsActive(log, storage))
	router.Get("/users/getReview", userhandlers.GetReview(log, storage))

	// PullRequests
	router.Post("/pullRequest/create", prhandlers.Create(log, storage))
	router.Post("/pullRequest/merge", prhandlers.Merge(log, storage))
	router.Post("/pullRequest/reassign", prhandlers.Reassign(log, storage))

	// Stats
	router.Get("/stats", statshandlers.Get(log, storage))

	// Запускаем сервис

	log.Info("starting server", slog.String("address", cfg.Address))
	srv := &http.Server{
		Addr:         cfg.Address,
		Handler:      router,
		ReadTimeout:  cfg.HTTPServer.Timeout,
		WriteTimeout: cfg.HTTPServer.Timeout,
		IdleTimeout:  cfg.HTTPServer.IdleTimeout,
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Error("failed to start server", sl.Err(err))
	}

	log.Error("server stopped")
}

func setupLogger(env string) *slog.Logger {
	var log *slog.Logger
	switch env {
	case envLocal:
		log = setupPrettySlog()
	case envDev:
		log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	case envProd:
		log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	default:
		log = setupPrettySlog()
	}

	return log
}

func setupPrettySlog() *slog.Logger {
	opts := slogpretty.PrettyHandlerOptions{
		SlogOpts: &slog.HandlerOptions{
			Level: slog.LevelDebug,
		},
	}
	handler := opts.NewPrettyHandler(os.Stdout)

	return slog.New(handler)
}
