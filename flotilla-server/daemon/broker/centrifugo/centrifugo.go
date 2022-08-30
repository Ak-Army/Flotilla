package centrifugo

import (
	"context"
	"encoding/json"
	"log"

	"github.com/centrifugal/centrifuge-go"
	"github.com/nats-io/nats.go"
)

const (
	subject = "public:orders"
)

// Peer implements the peer interface for NATS.
type Peer struct {
	conn     *centrifuge.Client
	messages chan []byte
	send     chan []byte
	errors   chan error
	done     chan bool
	js       nats.JetStreamContext
	subs     *nats.Subscription
	sub      *centrifuge.Subscription
}

// NewPeer creates and returns a new Peer for communicating with NATS.
func NewPeer(host string) (*Peer, error) {
	client := centrifuge.NewJsonClient(
		"ws://"+host+"/connection/websocket",
		centrifuge.Config{},
	)

	client.OnConnecting(func(e centrifuge.ConnectingEvent) {
		log.Printf("Connecting - %d (%s)", e.Code, e.Reason)
	})
	client.OnError(func(e centrifuge.ErrorEvent) {
		log.Printf("Error - %s)", e.Error)
	})
	client.OnConnected(func(e centrifuge.ConnectedEvent) {
		log.Printf("Connected with ID %s", e.ClientID)
	})
	client.OnDisconnected(func(e centrifuge.DisconnectedEvent) {
		log.Printf("Disconnected: %d (%s)", e.Code, e.Reason)
	})

	p := &Peer{
		conn:     client,
		messages: make(chan []byte, 1),
		send:     make(chan []byte),
		errors:   make(chan error, 1),
		done:     make(chan bool),
	}
	client.Connect()

	p.sub, _ = p.conn.NewSubscription(subject, centrifuge.SubscriptionConfig{
		Recoverable: true,
		JoinLeave:   true,
	})

	p.sub.OnError(func(e centrifuge.SubscriptionErrorEvent) {
		log.Println("Sub error: ", e.Error)
	})
	p.sub.Subscribe()

	return p, nil
}

// Subscribe prepares the peer to consume messages.
func (n *Peer) Subscribe() error {
	n.sub.OnPublication(func(e centrifuge.PublicationEvent) {
		n.messages <- e.Data
	})
	return nil
}

// Recv returns a single message consumed by the peer. Subscribe must be called
// before this. It returns an error if the receive failed.
func (n *Peer) Recv() ([]byte, error) {
	return <-n.messages, nil
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
				b, _ := json.Marshal(string(msg))
				if _, err := n.sub.Publish(context.Background(), b); err != nil {
					n.errors <- err
				}
			case <-n.done:
				return
			}
		}
	}()
}

// Teardown performs any cleanup logic that needs to be performed after the
// test is complete.
func (n *Peer) Teardown() {
	close(n.done)
	n.sub.Unsubscribe()
	n.conn.Close()
}
