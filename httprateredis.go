package httprateredis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-chi/httprate"
	"github.com/rueian/rueidis"
)

// Config defines the config of httprate-redis
type Config struct {
	// Addresses is a list of redis host/ports, delimited like so:
	// * []string{"127.0.0.1:6379"}
	Addresses []string `toml:"host"`
	// Password is the Redis password (if the cluster has one)
	Password string `toml:"password"`
	// DBIndex is the DB index to select
	DBIndex int `toml:"db_index"` // default 0
}

type redisCounter struct {
	Client       rueidis.Client
	windowLength time.Duration
}

var _ httprate.LimitCounter = &redisCounter{}

// WithRedisLimitCounter is middleware that can be fed to httprate.
// Example:
/*
	limiter := httprate.Limit(
		serverOptions.RequestLimit,
		serverOptions.RequestPeriod,
		httprate.WithKeyByRealIP(),
		httprateredis.WithRedisLimitCounter(
			&httprateredis.Config{
				Addresses: redisAddresses,
				Password:  redisPassword,
			},
		),
	)
*/
func WithRedisLimitCounter(cfg *Config) httprate.Option {
	rc, _ := NewRedisLimitCounter(cfg)
	return httprate.WithLimitCounter(rc)
}

// NewRedisLimitCounter returns a new redis-based LimitCounter.
func NewRedisLimitCounter(cfg *Config) (httprate.LimitCounter, error) {
	if cfg == nil {
		cfg = &Config{}
	}
	if len(cfg.Addresses) == 0 {
		cfg.Addresses[0] = "127.0.0.1:6379"
	}

	opts := rueidis.ClientOption{
		InitAddress: cfg.Addresses,
		SelectDB:    0,
	}

	if cfg.Password != "" {
		opts.Password = cfg.Password
	}

	if len(cfg.Addresses) > 1 {
		opts.ShuffleInit = true
	}

	if cfg.DBIndex != 0 {
		opts.SelectDB = cfg.DBIndex
	}

	client, err := rueidis.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("unable to c.Client.ct to redis: %w", err)
	}

	return &redisCounter{
		Client: client,
	}, nil
}

// Config modifies the current config of the counter
func (c *redisCounter) Config(requestLimit int, windowLength time.Duration) {
	c.windowLength = windowLength
}

func (c *redisCounter) Increment(key string, currentWindow time.Time) error {
	hkey := limitCounterKey(key, currentWindow)

	incrQuery := c.Client.B().Incr().Key(hkey).Build()
	expireQuery := c.Client.B().Expire().Key(hkey).Seconds(int64(c.windowLength.Seconds() * 3)).Build()

	result := c.Client.DoMulti(context.Background(), incrQuery, expireQuery)
	for _, response := range result {
		if response.Error() != nil {
			return fmt.Errorf("redis increment failed: %w", response.Error())
		}
	}

	return nil
}

func (c *redisCounter) Get(key string, currentWindow, previousWindow time.Time) (int, int, error) {
	getCurrValue := c.Client.B().Get().Key(limitCounterKey(key, currentWindow)).Build()
	getPrevValue := c.Client.B().Get().Key(limitCounterKey(key, previousWindow)).Build()

	result := c.Client.DoMulti(context.Background(), getCurrValue, getPrevValue)
	for _, response := range result {
		if response.Error() != nil {
			if response.Error() == rueidis.Nil {
				return 0, 0, errors.New("redis get failed: nil")
			}
			return 0, 0, fmt.Errorf("redis get failed: %w", response.Error())
		}
	}

	curr, err := result[0].ToInt64()
	if err != nil {
		return 0, 0, fmt.Errorf("redis int value: %w", err)
	}

	prev, err := result[1].ToInt64()
	if err != nil {
		return 0, 0, fmt.Errorf("redis int value: %w", err)
	}

	return int(curr), int(prev), nil
}

// limitCounterKey returns the current limit counter key
func limitCounterKey(key string, window time.Time) string {
	return fmt.Sprintf("httprate:%d", httprate.LimitCounterKey(key, window))
}
