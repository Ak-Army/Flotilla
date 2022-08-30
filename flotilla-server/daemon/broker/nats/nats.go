package nats

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

const (
	subject = "ORDERS"
)

// Peer implements the peer interface for NATS.
type Peer struct {
	conn     *nats.Conn
	messages chan messages
	send     chan []byte
	errors   chan error
	done     chan bool
	js       nats.JetStreamContext
	subs     *nats.Subscription
}
type messages struct {
	message []byte
	err     error
}

// NewPeer creates and returns a new Peer for communicating with NATS.
func NewPeer(host string, jetStream bool) (*Peer, error) {
	conn, err := nats.Connect(fmt.Sprintf("nats://%s", host))
	if err != nil {
		return nil, err
	}

	var js nats.JetStreamContext
	if jetStream {
		js, err = conn.JetStream()
		if err != nil {
			return nil, err
		}
		// Create a Stream
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     subject,
			Subjects: []string{subject + ".all"},
			Storage:  nats.FileStorage,
		})
		if err != nil {
			return nil, err
		}
	}
	// We want to be alerted if we get disconnected, this will be due to Slow
	// Consumer.
	conn.Opts.AllowReconnect = false

	return &Peer{
		conn:     conn,
		js:       js,
		messages: make(chan messages),
		send:     make(chan []byte),
		errors:   make(chan error, 1),
		done:     make(chan bool),
	}, nil
}

// Subscribe prepares the peer to consume messages.
func (n *Peer) Subscribe() error {
	if n.js != nil {
		n.subs, _ = n.js.Subscribe(subject+".all", func(message *nats.Msg) {
			n.messages <- messages{message.Data, message.Ack()}
		})
		return nil
	}
	n.conn.Subscribe(subject+".all", func(message *nats.Msg) {
		n.messages <- messages{message.Data, nil}
	})
	return nil
}

// Recv returns a single message consumed by the peer. Subscribe must be called
// before this. It returns an error if the receive failed.
func (n *Peer) Recv() ([]byte, error) {
	m := <-n.messages
	return m.message, m.err
}

// Send returns a channel on which messages can be sent for publishing.
func (n *Peer) Send() chan<- []byte {
	return n.send
}

// Errors returns the channel on which the peer sends publish errors.
func (n *Peer) Errors() <-chan error {
	return n.errors
}

// Done signals to the peer that message publishing has completed.
func (n *Peer) Done() {
	n.done <- true
}

// Setup prepares the peer for testing.
func (n *Peer) Setup() {
	go func() {
		for {
			select {
			case msg := <-n.send:
				if err := n.sendMessage(msg); err != nil {
					n.errors <- err
				}
			case <-n.done:
				return
			}
		}
	}()
}

func (n *Peer) sendMessage(message []byte) error {
	if n.js != nil {
		_, err := n.js.PublishAsync(subject+".all", message)
		return err
	}

	return n.conn.Publish(subject+".all", message)
}

// Teardown performs any cleanup logic that needs to be performed after the
// test is complete.
func (n *Peer) Teardown() {
	if n.js != nil {
		n.js.DeleteStream(subject)
	}
	n.conn.Close()
}
