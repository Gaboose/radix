package redis

import (
	"fmt"
)

type MessageType int

const (
	MessageSubscribe MessageType = iota
	MessageUnsubscribe
	MessagePSubscribe
	MessagePUnsubscribe
	MessageMessage
	MessagePMessage
	MessageError
)

// Pub/sub message
type Message struct {
	Type          MessageType
	Channel       string
	Pattern       string
	Subscriptions int
	Payload       string
	Error         error
}

// Returns a string representation of the message.
func (m *Message) String() string {
	return fmt.Sprintf("Message{ Type: %v, Channel: %v, Pattern: %v, Subscriptions: %v, Payload: %v, "+
		"Error: %v }", m.Type, m.Channel, m.Pattern, m.Subscriptions, m.Payload, m.Error)
}
