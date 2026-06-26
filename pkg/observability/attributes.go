package observability

// Canonical attribute keys used across spans, logs, and dashboard projections.
const (
	AttrCorrelationID = "correlation_id"
	AttrCausationID   = "causation_id"
	AttrThreadID      = "thread_id"
	AttrSquadID       = "squad_id"
	AttrAgentID       = "agent_id"
	AttrAgentType     = "agent_type"
	AttrMessageID     = "message_id"
	AttrModel         = "llm_model"
	AttrTokensIn      = "tokens_in"
	AttrTokensOut     = "tokens_out"
	AttrLatencyMS     = "latency_ms"
	AttrToolName      = "tool_name"
	AttrRetryCount    = "retry_count"
	AttrConfidence    = "confidence"
	AttrUserID        = "user_id"
	AttrSessionID     = "session_id"
)
