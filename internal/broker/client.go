package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"log/slog"
)

type Config struct {
	BrokerURL      string
	ClientID       string
	Username       string
	Password       string
	KeepAlive      time.Duration
	PingTimeout    time.Duration
	ConnectTimeout time.Duration
	Logger         *slog.Logger
}

type Client struct {
	client mqtt.Client
	log    *slog.Logger
	cfg    Config
}

func New(cfg Config) (*Client, error) {
	if cfg.BrokerURL == "" {
		return nil, errors.New("broker: broker URL is required")
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.BrokerURL)

	if cfg.ClientID != "" {
		opts.SetClientID(cfg.ClientID)
	}
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
	}
	if cfg.Password != "" {
		opts.SetPassword(cfg.Password)
	}

	keepAlive := cfg.KeepAlive
	if keepAlive == 0 {
		keepAlive = 60 * time.Second
	}
	opts.SetKeepAlive(keepAlive)

	pingTimeout := cfg.PingTimeout
	if pingTimeout == 0 {
		pingTimeout = 10 * time.Second
	}
	opts.SetPingTimeout(pingTimeout)

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	opts.OnConnect = func(_ mqtt.Client) {
		logger.Info("broker connected")
	}
	opts.OnConnectionLost = func(_ mqtt.Client, err error) {
		logger.Error("broker connection lost", "error", err)
	}
	opts.OnReconnecting = func(_ mqtt.Client, opts *mqtt.ClientOptions) {
		logger.Warn("broker reconnecting", "broker", opts.Servers)
	}

	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetOrderMatters(false)

	client := mqtt.NewClient(opts)

	return &Client{
		client: client,
		log:    logger,
		cfg:    cfg,
	}, nil
}

func (c *Client) Connect(ctx context.Context) error {
	if c.client.IsConnected() {
		return nil
	}

	token := c.client.Connect()
	if err := waitForToken(ctx, token); err != nil {
		return fmt.Errorf("broker connect: %w", err)
	}

	if err := token.Error(); err != nil {
		return fmt.Errorf("broker connect token: %w", err)
	}

	return nil
}

func (c *Client) Publish(ctx context.Context, topic string, payload []byte, qos byte, retained bool) error {
	if !c.client.IsConnectionOpen() {
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	token := c.client.Publish(topic, qos, retained, payload)
	if err := waitForToken(ctx, token); err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}
	return token.Error()
}

func (c *Client) Subscribe(ctx context.Context, topic string, qos byte, handler mqtt.MessageHandler) error {
	if handler == nil {
		return errors.New("broker subscribe: handler is nil")
	}
	if !c.client.IsConnectionOpen() {
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	token := c.client.Subscribe(topic, qos, handler)
	if err := waitForToken(ctx, token); err != nil {
		return fmt.Errorf("subscribe %s: %w", topic, err)
	}
	return token.Error()
}

func (c *Client) Unsubscribe(ctx context.Context, topics ...string) error {
	token := c.client.Unsubscribe(topics...)
	if err := waitForToken(ctx, token); err != nil {
		return fmt.Errorf("unsubscribe: %w", err)
	}
	return token.Error()
}

func (c *Client) Disconnect() {
	if c.client != nil {
		c.client.Disconnect(250)
	}
}

func waitForToken(ctx context.Context, token mqtt.Token) error {
	done := token.Done()
	if done == nil {
		token.Wait()
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}
