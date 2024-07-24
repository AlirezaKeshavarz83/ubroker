package broker

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"ubroker/pkg/ubroker"
)

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	return &core{
		ch:      make(chan *ubroker.Delivery, 100000),
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
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if c.isClosed {
		return nil, ubroker.ErrClosed
	}

	return c.ch, nil
}

func (c *core) Acknowledge(ctx context.Context, id int32) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isClosed {
		return ubroker.ErrClosed
	}

	_, ok := c.msgById[id]

	if !ok {
		return ubroker.ErrInvalidID
	}
	delete(c.msgById, id)

	return nil
}

func (c *core) ReQueue(ctx context.Context, id int32) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.mu.Lock()

	if c.isClosed {
		c.mu.Unlock()
		return ubroker.ErrClosed
	}

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
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.mu.Lock()

	if c.isClosed {
		c.mu.Unlock()
		return ubroker.ErrClosed
	}

	delv := &ubroker.Delivery{Id: rand.Int31(), Message: message}

	c.msgById[delv.Id] = message
	c.ch <- delv

	c.mu.Unlock()

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

	c.mu.Lock()
	c.isClosed = true
	close(c.ch)
	c.mu.Unlock()

	return nil
}
