package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache struct {
	c      *redis.Client
	prefix string
	ttl    time.Duration
}

func New(prefix string, ttl time.Duration) (*Cache, error) {
	addr := os.Getenv("REDIS_URL")
	if addr == "" {
		addr = "redis:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 10})
	if err := c.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	return &Cache{c: c, prefix: prefix, ttl: ttl}, nil
}

func (ca *Cache) Get(ctx context.Context, key string, dest any) (bool, error) {
	v, err := ca.c.Get(ctx, ca.prefix+key).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal([]byte(v), dest)
}

func (ca *Cache) Set(ctx context.Context, key string, val any) error {
	b, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return ca.c.Set(ctx, ca.prefix+key, b, ca.ttl).Err()
}

func (ca *Cache) Del(ctx context.Context, key string) error {
	return ca.c.Del(ctx, ca.prefix+key).Err()
}

func (ca *Cache) Ping(ctx context.Context) error { return ca.c.Ping(ctx).Err() }
