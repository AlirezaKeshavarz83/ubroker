package broker

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/arcana261/ubroker/pkg/ubroker"
)

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {

	return &core{
		ch:      make(chan *ubroker.Delivery, 1000000000),
		msgById: make(map[int32]*ubroker.Message),
		ttl:     ttl,
	}
}

type core struct {
	isClosed bool
	ch       chan *ubroker.Delivery
	msgById  map[int32]*ubroker.Message
	ttl      time.Duration
	mu       sync.Mutex
}

func (c *core) Delivery(ctx context.Context) (<-chan *ubroker.Delivery, error) {
	if c.isClosed {
		return nil, ubroker.ErrClosed
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return c.ch, nil
}

func (c *core) Acknowledge(ctx context.Context, id int32) error {
	if c.isClosed {
		return ubroker.ErrClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.msgById[id]

	if !ok {
		return ubroker.ErrInvalidID
	}
	delete(c.msgById, id)

	return nil
}

func (c *core) ReQueue(ctx context.Context, id int32) error {
	if c.isClosed {
		return ubroker.ErrClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.mu.Lock()

	msg, ok := c.msgById[id]

	if !ok {
		c.mu.Unlock()
		return ubroker.ErrInvalidID
	}
	delete(c.msgById, id)
	c.mu.Unlock()

	return c.Publish(ctx, msg)
}

func (c *core) Publish(ctx context.Context, message *ubroker.Message) error {
	if c.isClosed {
		return ubroker.ErrClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	delv := &ubroker.Delivery{Id: rand.Int31(), Message: *message}

	c.mu.Lock()
	c.msgById[delv.Id] = message
	c.mu.Unlock()

	c.ch <- delv

	go func(id int32) {
		time.Sleep(c.ttl)
		c.mu.Lock()
		_, ok := c.msgById[id]
		c.mu.Unlock()
		if ok {
			c.ReQueue(ctx, id)
		}
	}(delv.Id)

	return nil
}

func (c *core) Close() error {
	c.isClosed = true

	close(c.ch)

	return nil
}
