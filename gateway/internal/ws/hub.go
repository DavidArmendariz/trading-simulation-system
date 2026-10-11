package ws

import (
	"log"
	"net/http"
	"sync/atomic"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 64 * 1024,
	// Local simulator: any origin may connect.
	CheckOrigin: func(*http.Request) bool { return true },
}

type Hub struct {
	clients    map[*Client]struct{}
	register   chan *Client
	unregister chan *Client
	snapshots  chan []byte
	events     chan []byte
	count      atomic.Int64
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]struct{}),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		snapshots:  make(chan []byte, 8),
		events:     make(chan []byte, 256),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.clients[c] = struct{}{}
			h.count.Store(int64(len(h.clients)))
			log.Printf("ws: client connected (%d total)", len(h.clients))
		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.done)
				h.count.Store(int64(len(h.clients)))
				log.Printf("ws: client disconnected (%d total)", len(h.clients))
			}
		case payload := <-h.snapshots:
			for c := range h.clients {
				c.pushSnapshot(payload)
			}
		case payload := <-h.events:
			for c := range h.clients {
				c.pushEvent(payload)
			}
		}
	}
}

// BroadcastSnapshot sends the latest book state, conflated per client.
func (h *Hub) BroadcastSnapshot(payload []byte) {
	select {
	case h.snapshots <- payload:
	default:
	}
}

// BroadcastEvent sends trades, stats and status updates.
func (h *Hub) BroadcastEvent(payload []byte) {
	select {
	case h.events <- payload:
	default:
	}
}

func (h *Hub) Clients() int64 { return h.count.Load() }

func (h *Hub) Handle(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws: upgrade failed: %v", err)
		return
	}
	c := newClient(h, conn)
	h.register <- c
	go c.writePump()
	go c.readPump()
}

// Envelope wraps an already-encoded JSON payload without re-marshaling it.
func Envelope(kind string, raw []byte) []byte {
	out := make([]byte, 0, len(raw)+len(kind)+24)
	out = append(out, `{"type":"`...)
	out = append(out, kind...)
	out = append(out, `","data":`...)
	out = append(out, raw...)
	out = append(out, '}')
	return out
}
