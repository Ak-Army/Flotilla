package centrifugo

import (
	"fmt"
	"log"
	"os/exec"
)

const (
	image        = "centrifugo/centrifugo"
	internalPort = "8000"
)

// Broker implements the broker interface for NATS.
type Broker struct {
	containerID string
}

// Start will start the message broker and prepare it for testing.
func (n *Broker) Start(host, port string) (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker run --ulimit nofile=65536:65536 -d "+
			"-v /opt/go/src/github.com/tianchaijz/Flotilla/flotilla-server/daemon/broker/centrifugo/config:/centrifugo "+
			"-p %s:%s %s centrifugo -c config.json", port, internalPort, image)).Output()
	if err != nil {
		log.Printf("Failed to start container %s: %s", image, err.Error())
		return "", err
	}

	log.Printf("Started container %s: %s", image, containerID)
	n.containerID = string(containerID)
	return string(containerID), nil
}

// Stop will stop the message broker.
func (n *Broker) Stop() (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker kill %s", n.containerID)).Output()
	if err != nil {
		log.Printf("Failed to stop container %s: %s", image, err.Error())
		return "", err
	}

	log.Printf("Stopped container %s: %s", image, n.containerID)
	n.containerID = ""
	return string(containerID), nil
}
