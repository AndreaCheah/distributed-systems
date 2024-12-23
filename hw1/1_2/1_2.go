package main

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
)

type Message struct {
	ID           int
	MessageCount int
	Timestamp    int
}

var numClients = 10
var numClientsServer = numClients + 1

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Client(id int, serverCh chan Message, clientCh chan Message, doneCh chan struct{}) {
	<-clientCh
	localTimestamp := 0
	fmt.Printf("%d: Client %d is running\n", localTimestamp, id)

	time.Sleep(2 * time.Second)

	go func() {
		for messageCount := 0; messageCount < 3; messageCount++ {
			localTimestamp++

			serverCh <- Message{id, messageCount, localTimestamp}
			fmt.Printf("%d: Client %d sends message %d\n", localTimestamp, id, messageCount)
			time.Sleep(2 * time.Second)
		}
		doneCh <- struct{}{}
	}()

	for {
		select {
		case msg, ok := <-clientCh:
			if !ok {
				return
			}
			// Update the local timestamp with max of local and received timestamps
			localTimestamp = max(localTimestamp, msg.Timestamp) + 1
			fmt.Printf("%d: Client %d receives message %d from Client %d\n", localTimestamp, id, msg.MessageCount, msg.ID)
		}
	}
}

func Server(serverCh chan Message, clientChs []chan Message, doneCh chan struct{}) {
	localTimestamp := 0
	fmt.Printf("%d: Server is running\n", localTimestamp)

	serverID := numClientsServer - 1
	for _, clientCh := range clientChs {
		clientCh <- Message{serverID, -1, localTimestamp}
	}

	// Holds messages received by clients
	var messageQueue []Message
	doneClients := 0

	for {
		select {
		case msg := <-serverCh:
			// Update the local timestamp with max of local and received timestamps
			localTimestamp = max(localTimestamp, msg.Timestamp) + 1
			fmt.Printf("%d: Server receives message %d from Client %d\n", localTimestamp, msg.MessageCount, msg.ID)

			if rand.Intn(2) == 0 {
				// Increment the local timestamp before forwarding the message
				localTimestamp++
				timestampCopy := localTimestamp

				for id, clientCh := range clientChs {
					if id != msg.ID {
						clientCh <- Message{msg.ID, msg.MessageCount, timestampCopy}
					}
				}
				fmt.Printf("%d: Server forwards message %d from Client %d\n", localTimestamp, msg.MessageCount, msg.ID)

				// Insert a copy of the message into the local queue
				msgCopy := Message{
					ID:           msg.ID,
					MessageCount: msg.MessageCount,
					Timestamp:    timestampCopy,
				}
				messageQueue = append(messageQueue, msgCopy)

				// Sort queue based on the server's timestamp
				sort.Slice(messageQueue, func(i, j int) bool {
					return messageQueue[i].Timestamp < messageQueue[j].Timestamp
				})
			} else {
				// Since dropping message has no impact on order of messages, it is not tracked in logical clock
				fmt.Printf("Server drops message %d from Client %d\n", msg.MessageCount, msg.ID)
			}

		case <-doneCh:
			// Once receive done signals from all clients, print the order of the message in which each client should read
			doneClients++
			if doneClients == numClients {
				fmt.Println("All clients are done sending messages.\nOrder of messages Clients should read:")
				for _, m := range messageQueue {
					fmt.Printf("%d: Message %d from Client %d\n", m.Timestamp, m.MessageCount, m.ID)
				}
				fmt.Println("Simulation is done. Press ctrl + c to terminate the process.")
				return
			}
		}
	}
}

func main() {
	fmt.Println("Main goroutine is running")

	serverCh := make(chan Message, numClients)
	clientChs := make([]chan Message, numClients)
	doneCh := make(chan struct{}, numClients)

	for i := range clientChs {
		clientChs[i] = make(chan Message, numClients)
	}

	go Server(serverCh, clientChs, doneCh)
	time.Sleep(1 * time.Second)
	for i := 0; i < numClients; i++ {
		go Client(i, serverCh, clientChs[i], doneCh)
	}

	time.Sleep(30 * time.Second)
}
