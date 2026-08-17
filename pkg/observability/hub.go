package observability

import "sync"

const DefaultHubMaxSubscribers = 64

type HubStats struct {
	Subscribers    int `json:"subscribers"`
	MaxSubscribers int `json:"max_subscribers"`
	DroppedEvents  int `json:"dropped_events"`
}

// Hub broadcasts AgentStep events to live subscribers without blocking agents.
type Hub struct {
	mu             sync.RWMutex
	subs           map[int]chan AgentStep
	next           int
	maxSubscribers int
	droppedEvents  int
}

func NewHub() *Hub {
	return NewHubWithLimit(DefaultHubMaxSubscribers)
}

func NewHubWithLimit(maxSubscribers int) *Hub {
	if maxSubscribers <= 0 {
		maxSubscribers = DefaultHubMaxSubscribers
	}
	return &Hub{subs: make(map[int]chan AgentStep), maxSubscribers: maxSubscribers}
}

// Subscribe registers a buffered channel for live step delivery.
func (h *Hub) Subscribe() (int, <-chan AgentStep) {
	id, ch, ok := h.TrySubscribe()
	if ok {
		return id, ch
	}
	return 0, closedStepChannel()
}

// TrySubscribe registers a buffered channel unless the subscriber limit is reached.
func (h *Hub) TrySubscribe() (int, <-chan AgentStep, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs == nil {
		h.subs = make(map[int]chan AgentStep)
	}
	maxSubscribers := h.maxSubscribers
	if maxSubscribers <= 0 {
		maxSubscribers = DefaultHubMaxSubscribers
	}
	if len(h.subs) >= maxSubscribers {
		return 0, nil, false
	}
	h.next++
	id := h.next
	ch := make(chan AgentStep, 128)
	h.subs[id] = ch
	return id, ch, true
}

func closedStepChannel() <-chan AgentStep {
	ch := make(chan AgentStep)
	close(ch)
	return ch
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
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subs {
		select {
		case ch <- step:
		default:
			h.droppedEvents++
		}
	}
}

func (h *Hub) Stats() HubStats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	maxSubscribers := h.maxSubscribers
	if maxSubscribers <= 0 {
		maxSubscribers = DefaultHubMaxSubscribers
	}
	return HubStats{
		Subscribers:    len(h.subs),
		MaxSubscribers: maxSubscribers,
		DroppedEvents:  h.droppedEvents,
	}
}
