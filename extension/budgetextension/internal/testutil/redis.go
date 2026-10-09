// Package testutil holds extension test helpers: a miniredis backed storage
// client with atomic increments, a storage extension and host, and pdata
// fixtures.
package testutil // import "github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/testutil"

import (
	"context"
	"errors"
	"net"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/xextension/storage"
)

// ErrInjected is returned by a RedisClient while Fail is set.
var ErrInjected = errors.New("injected failure")

// RedisClient is a storage.Client over Redis (miniredis in tests) that also
// implements BatchIncrementBy.
type RedisClient struct {
	rdb    *redis.Client
	prefix string
	// Fail makes every call fail.
	Fail atomic.Bool
	// FailAfterApply makes BatchIncrementBy apply its increments and then
	// fail, simulating a partial failure.
	FailAfterApply atomic.Bool
	// FailDial makes BatchIncrementBy fail before sending, like a refused
	// connection.
	FailDial   atomic.Bool
	Increments atomic.Int64
	// BeforeIncrement, when set, runs inside BatchIncrementBy before it
	// applies anything: a flush is in flight.
	BeforeIncrement func()
}

// NewRedisClient returns a client for addr with a key prefix.
func NewRedisClient(addr, prefix string) *RedisClient {
	return &RedisClient{rdb: redis.NewClient(&redis.Options{Addr: addr}), prefix: prefix}
}

// Get implements storage.Client.
func (c *RedisClient) Get(ctx context.Context, key string) ([]byte, error) {
	if c.Fail.Load() {
		return nil, ErrInjected
	}
	b, err := c.rdb.Get(ctx, c.prefix+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return b, err
}

// Set implements storage.Client.
func (c *RedisClient) Set(ctx context.Context, key string, value []byte) error {
	if c.Fail.Load() {
		return ErrInjected
	}
	return c.rdb.Set(ctx, c.prefix+key, value, 0).Err()
}

// Delete implements storage.Client.
func (c *RedisClient) Delete(ctx context.Context, key string) error {
	if c.Fail.Load() {
		return ErrInjected
	}
	return c.rdb.Del(ctx, c.prefix+key).Err()
}

// Batch implements storage.Client.
func (c *RedisClient) Batch(ctx context.Context, ops ...*storage.Operation) error {
	if c.Fail.Load() {
		return ErrInjected
	}
	for _, op := range ops {
		var err error
		switch op.Type {
		case storage.Get:
			op.Value, err = c.Get(ctx, op.Key)
		case storage.Set:
			err = c.Set(ctx, op.Key, op.Value)
		case storage.Delete:
			err = c.Delete(ctx, op.Key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Close implements storage.Client.
func (*RedisClient) Close(context.Context) error { return nil }

// BatchIncrementBy atomically adds deltas in one pipeline.
func (c *RedisClient) BatchIncrementBy(ctx context.Context, deltas map[string]int64) (map[string]int64, error) {
	if c.Fail.Load() {
		return nil, ErrInjected
	}
	if c.FailDial.Load() {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}
	if c.BeforeIncrement != nil {
		c.BeforeIncrement()
	}
	c.Increments.Add(1)
	p := c.rdb.Pipeline()
	cmds := make(map[string]*redis.IntCmd, len(deltas))
	for k, d := range deltas {
		cmds[k] = p.IncrBy(ctx, c.prefix+k, d)
	}
	if _, err := p.Exec(ctx); err != nil {
		return nil, err
	}
	if c.FailAfterApply.Load() {
		return nil, ErrInjected
	}
	out := make(map[string]int64, len(cmds))
	for k, cmd := range cmds {
		out[k] = cmd.Val()
	}
	return out, nil
}

// PlainClient hides BatchIncrementBy, like upstream redis_storage today.
type PlainClient struct{ storage.Client }

// StorageExtension serves a fixed client.
type StorageExtension struct {
	component.StartFunc
	component.ShutdownFunc
	Client storage.Client
}

// GetClient implements storage.Extension.
func (e *StorageExtension) GetClient(context.Context, component.Kind, component.ID, string) (storage.Client, error) {
	return e.Client, nil
}

// Host is a component.Host with fixed extensions.
type Host map[component.ID]component.Component

// GetExtensions implements component.Host.
func (h Host) GetExtensions() map[component.ID]component.Component { return h }

// NopComponent is a component that is not a storage extension.
type NopComponent struct {
	component.StartFunc
	component.ShutdownFunc
}
