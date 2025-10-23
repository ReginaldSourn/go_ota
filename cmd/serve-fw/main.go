package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	flagAddr := flag.String("addr", "", "HTTP listen address (overrides FW_HTTP_ADDR)")
	flag.Parse()

	_ = godotenv.Load()

	addr := valueOrDefault(*flagAddr, os.Getenv("FW_HTTP_ADDR"), ":8080")
	fwFile := valueOrDefault("", os.Getenv("FW_FILE"), "firmware/demo.bin")

	if fwFile == "" {
		exitErr(errors.New("firmware file path is required (FW_FILE)"))
	}

	if _, err := os.Stat(fwFile); err != nil {
		exitErr(fmt.Errorf("firmware file missing: %w", err))
	}

	root := filepath.Dir(fwFile)
	prefix := "/firmware/"

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	fileServer := http.StripPrefix(prefix, http.FileServer(http.Dir(root)))
	mux.Handle(prefix, logRequests(logger, fileServer))
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, prefix) {
			fileServer.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	}))

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("firmware server started", "addr", addr, "root", root)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logger.Info("shutting down firmware server")
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
}

func logRequests(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		logger.Info("http request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(start))
	})
}

func valueOrDefault(primary string, env string, fallback string) string {
	if primary != "" {
		return primary
	}
	if env != "" {
		return env
	}
	return fallback
}

func exitErr(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
