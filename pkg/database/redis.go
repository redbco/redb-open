package database

import (
	"context"
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/config"
	"github.com/redis/go-redis/v9"
)

// RedisConfig holds the Redis connection configuration
type RedisConfig struct {
	Host         string
	Port         int
	Password     string
	DB           int
	MaxRetries   int
	PoolSize     int
	MinIdleConns int
	MaxIdleTime  time.Duration
}

// DefaultRedisConfig returns a default configuration for local development
func DefaultRedisConfig() RedisConfig {
	return RedisConfig{
		Host:         "localhost",
		Port:         6379,
		Password:     "",
		DB:           0,
		MaxRetries:   3,
		PoolSize:     10,
		MinIdleConns: 2,
		MaxIdleTime:  time.Minute * 5,
	}
}

// RedisFromGlobalConfig creates a Redis config from the global configuration
func RedisFromGlobalConfig(cfg *config.Config) RedisConfig {
	return RedisConfig{
		Host:         "localhost",
		Port:         6379,
		Password:     "",
		DB:           0,
		MaxRetries:   3,
		PoolSize:     10,
		MinIdleConns: 2,
		MaxIdleTime:  time.Minute * 5,
	}
}

// Redis represents a Redis client connection pool
type Redis struct {
	client *redis.Client
}

// NewRedis creates a new Redis client using the provided configuration
func NewRedis(ctx context.Context, cfg RedisConfig) (*Redis, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password:     cfg.Password,
		DB:           cfg.DB,
		MaxRetries:   cfg.MaxRetries,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
	})

	// Test the connection
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, err
	}

	return &Redis{client: client}, nil
}

// Close closes the Redis client connection
func (r *Redis) Close() {
	if r.client != nil {
		r.client.Close()
	}
}

// Client returns the underlying Redis client
func (r *Redis) Client() *redis.Client {
	return r.client
}

// Ping checks if the Redis connection is alive
func (r *Redis) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}
