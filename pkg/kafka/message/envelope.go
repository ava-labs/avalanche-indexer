// internal/transport/kafka/message/envelope.go
package message

import "encoding/json"

type Envelope struct {
	Type    string          `json:"type"`
	Version int             `json:"version"`
	ID      string          `json:"id,omitempty"`
	TS      string          `json:"ts,omitempty"` // or time.Time with custom parsing
	Data    json.RawMessage `json:"data"`
}

func Open(data []byte) (*Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	return &env, nil
}

func New(msgType string, version int, id string, ts string, data json.RawMessage) *Envelope {
	return &Envelope{
		Type:    msgType,
		Version: version,
		ID:      id,
		TS:      ts,
		Data:    data,
	}
}
