package synapse

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// ContextPayload is the typed payload contract for context messages.
type ContextPayload struct {
	Content   string
	Citations []any
}

// TaskPayload is the typed payload contract for delegated task messages.
type TaskPayload struct {
	TaskType      string
	Parameters    map[string]any
	ReplyToThread string
}

// CommandPayload is the typed payload contract for control-plane messages.
type CommandPayload struct {
	Command    string
	Parameters map[string]any
}

// Payload stores the flat JSON-compatible fields of a message behind an
// accessor API. Keeping the backing map private prevents callers from mutating
// a message without going through the validation and copy boundaries.
type Payload struct {
	values map[string]any
}

// Get returns a deep copy of a payload value.
func (p Payload) Get(key string) (any, bool) {
	value, ok := p.values[key]
	if !ok {
		return nil, false
	}
	return clonePayloadValue(value), true
}

// Set stores a deep copy of value in the payload.
func (p *Payload) Set(key string, value any) {
	if p.values == nil {
		p.values = make(map[string]any)
	}
	p.values[key] = clonePayloadValue(value)
}

// Clone returns an independent payload copy.
func (p Payload) Clone() Payload {
	clone := Payload{values: make(map[string]any, len(p.values))}
	for key, value := range p.values {
		clone.values[key] = clonePayloadValue(value)
	}
	return clone
}

// MarshalJSON preserves the existing flat payload representation.
func (p Payload) MarshalJSON() ([]byte, error) {
	values := make(map[string]any, len(p.values))
	for key, value := range p.values {
		values[key] = clonePayloadValue(value)
	}
	return json.Marshal(values)
}

// UnmarshalJSON restores the flat payload representation from storage.
func (p *Payload) UnmarshalJSON(data []byte) error {
	values := make(map[string]any)
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	p.values = make(map[string]any, len(values))
	for key, value := range values {
		p.values[key] = clonePayloadValue(value)
	}
	return nil
}

// AsContext returns the typed context payload.
func (p Payload) AsContext() (ContextPayload, error) {
	content, ok := p.Get("content")
	if !ok {
		return ContextPayload{}, fmt.Errorf("missing content")
	}
	contentString, ok := content.(string)
	if !ok {
		return ContextPayload{}, fmt.Errorf("content must be a string")
	}

	citationsValue, ok := p.Get("citations")
	if !ok {
		return ContextPayload{}, fmt.Errorf("missing citations")
	}
	citations, ok := citationsValue.([]any)
	if !ok && citationsValue != nil {
		return ContextPayload{}, fmt.Errorf("citations must be an array")
	}
	return ContextPayload{Content: contentString, Citations: citations}, nil
}

// AsTask returns the typed task payload.
func (p Payload) AsTask() (TaskPayload, error) {
	taskTypeValue, ok := p.Get("task_type")
	if !ok {
		return TaskPayload{}, fmt.Errorf("missing task_type")
	}
	taskType, ok := taskTypeValue.(string)
	if !ok || taskType == "" {
		return TaskPayload{}, fmt.Errorf("task_type must be a non-empty string")
	}

	parametersValue, ok := p.Get("parameters")
	if !ok {
		return TaskPayload{}, fmt.Errorf("missing parameters")
	}
	parameters, ok := parametersValue.(map[string]any)
	if !ok || parameters == nil {
		return TaskPayload{}, fmt.Errorf("parameters must be an object")
	}

	replyValue, ok := p.Get("reply_to_thread")
	if !ok {
		return TaskPayload{}, fmt.Errorf("missing reply_to_thread")
	}
	replyToThread, ok := replyValue.(string)
	if !ok {
		return TaskPayload{}, fmt.Errorf("reply_to_thread must be a string")
	}
	return TaskPayload{TaskType: taskType, Parameters: parameters, ReplyToThread: replyToThread}, nil
}

// AsCommand returns the typed command payload.
func (p Payload) AsCommand() (CommandPayload, error) {
	commandValue, ok := p.Get("command")
	if !ok {
		return CommandPayload{}, fmt.Errorf("missing command")
	}
	command, ok := commandValue.(string)
	if !ok || command == "" {
		return CommandPayload{}, fmt.Errorf("command must be a non-empty string")
	}

	parametersValue, ok := p.Get("parameters")
	if !ok {
		return CommandPayload{}, fmt.Errorf("missing parameters")
	}
	parameters, ok := parametersValue.(map[string]any)
	if !ok || parameters == nil {
		return CommandPayload{}, fmt.Errorf("parameters must be an object")
	}
	return CommandPayload{Command: command, Parameters: parameters}, nil
}

func clonePayloadValue(value any) any {
	cloned := clonePayloadReflectValue(reflect.ValueOf(value))
	if !cloned.IsValid() {
		return nil
	}
	return cloned.Interface()
}

func clonePayloadReflectValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}

	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := clonePayloadReflectValue(value.Elem())
		result := reflect.New(value.Type()).Elem()
		result.Set(cloned)
		return result
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			result.SetMapIndex(
				clonePayloadReflectValue(iter.Key()),
				clonePayloadReflectValue(iter.Value()),
			)
		}
		return result
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(clonePayloadReflectValue(value.Index(i)))
		}
		return result
	case reflect.Array:
		result := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(clonePayloadReflectValue(value.Index(i)))
		}
		return result
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.New(value.Type().Elem())
		result.Elem().Set(clonePayloadReflectValue(value.Elem()))
		return result
	default:
		return value
	}
}
