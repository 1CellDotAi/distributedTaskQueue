// Package ws implements a WebSocket hub that fans out Redis pub/sub events to
// browser clients connected to the API server.
package ws

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// Hub holds connected clients and broadcasts messages to all of them.
type Hub struct {
	mu      sync.RWMutex
	clients map[*client]struct{}

	upgrader websocket.Upgrader
	rdb      *redis.Client
	channel  string
}

type client struct {
	conn *websocket.Conn
	send chan []byte
}

// NewHub creates a Hub that subscribes to the given Redis channel.
func NewHub(rdb *redis.Client, channel string) *Hub {
	return &Hub{
		clients: map[*client]struct{}{},
		upgrader: websocket.Upgrader{
			CheckOrigin: func(_ *http.Request) bool { return true },
		},
		rdb:     rdb,
		channel: channel,
	}
}

// Run subscribes to Redis and fans out messages until ctx is canceled.
func (h *Hub) Run(ctx context.Context) error {
	sub := h.rdb.Subscribe(ctx, h.channel)
	defer sub.Close()
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			h.broadcast([]byte(msg.Payload))
		}
	}
}

func (h *Hub) broadcast(b []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		select {
		case c.send <- b:
		default:
			// drop slow clients
		}
	}
}

// ServeHTTP upgrades an HTTP request to a WebSocket connection and registers the client.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade: %v", err)
		return
	}
	c := &client{conn: conn, send: make(chan []byte, 64)}
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()

	go h.writeLoop(c)
	h.readLoop(c)
}

func (h *Hub) readLoop(c *client) {
	defer h.disconnect(c)
	c.conn.SetReadLimit(1 << 14)
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		if _, _, err := c.conn.NextReader(); err != nil {
			return
		}
	}
}

func (h *Hub) writeLoop(c *client) {
	pingTick := time.NewTicker(30 * time.Second)
	defer pingTick.Stop()
	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				return
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-pingTick.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (h *Hub) disconnect(c *client) {
	h.mu.Lock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
	}
	h.mu.Unlock()
	_ = c.conn.Close()
}

// ClientCount returns the current number of connected clients (useful for tests/metrics).
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}
