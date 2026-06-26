package observability

import "sync"

// Hub broadcasts AgentStep events to live subscribers without blocking agents.
type Hub struct {
	mu   sync.RWMutex
	subs map[int]chan AgentStep
	next int
}

func NewHub() *Hub {
	return &Hub{subs: make(map[int]chan AgentStep)}
}

// Subscribe registers a buffered channel for live step delivery.
func (h *Hub) Subscribe() (int, <-chan AgentStep) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs == nil {
		h.subs = make(map[int]chan AgentStep)
	}
	h.next++
	id := h.next
	ch := make(chan AgentStep, 128)
	h.subs[id] = ch
	return id, ch
}

// Unsubscribe removes and closes the subscriber channel.
func (h *Hub) Unsubscribe(id int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ch, ok := h.subs[id]; ok {
		delete(h.subs, id)
		close(ch)
	}
}

// Broadcast fan-outs one step to all subscribers using non-blocking sends.
func (h *Hub) Broadcast(step AgentStep) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ch := range h.subs {
		select {
		case ch <- step:
		default:
		}
	}
}
