package ws

import (
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 5 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = 25 * time.Second
)

// Client holds two outbound paths on purpose. Book snapshots go to a one-slot
// conflated channel: a browser that falls behind should see the newest book,
// not a growing queue of stale ones. Everything else uses a small buffered
// channel and is dropped when full.
type Client struct {
	hub    *Hub
	conn   *websocket.Conn
	events chan []byte
	snap   chan []byte
	done   chan struct{}
}

func newClient(hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		hub:    hub,
		conn:   conn,
		events: make(chan []byte, 64),
		snap:   make(chan []byte, 1),
		done:   make(chan struct{}),
	}
}

// pushSnapshot replaces any snapshot the client has not read yet.
func (c *Client) pushSnapshot(payload []byte) {
	select {
	case c.snap <- payload:
	default:
		select {
		case <-c.snap:
		default:
		}
		select {
		case c.snap <- payload:
		default:
		}
	}
}

func (c *Client) pushEvent(payload []byte) {
	select {
	case c.events <- payload:
	default: // slow client, drop this event
	}
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		_ = c.conn.Close()
	}()
	c.conn.SetReadLimit(1024)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()
	for {
		select {
		case payload := <-c.snap:
			if !c.write(payload) {
				return
			}
		case payload := <-c.events:
			if !c.write(payload) {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *Client) write(payload []byte) bool {
	_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return c.conn.WriteMessage(websocket.TextMessage, payload) == nil
}
