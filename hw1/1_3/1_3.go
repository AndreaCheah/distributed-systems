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
	Timestamps   []int
}

var numClients = 10
var numClientsServer = numClients + 1

// updates timestamps with max of the elements of the vectors
func UpdateTimestampsWithMax(id int, localTimestamps []int, msgTimestamps []int) {
	for i, msgTimestamp := range msgTimestamps {
		if msgTimestamp > localTimestamps[i] {
			localTimestamps[i] = msgTimestamp
		}
	}
}

// Detects causality violation based on vector clocks
func DetectCausalityViolation(id int, localTimestamps []int, msg Message) bool {
	for i := range localTimestamps {
		if i != msg.ID && msg.Timestamps[i] > localTimestamps[i] {
			// Violation detected
			fmt.Printf("Causality violation detected at Client %d while receiving message %d from Client %d\n", id, msg.MessageCount, msg.ID)
			fmt.Printf("Client %d's vector clock: %v, Message's vector clock: %v\n", id, localTimestamps, msg.Timestamps)
			return true
		}
	}
	return false
}

func Client(id int, serverCh chan Message, clientCh chan Message, doneCh chan struct{}) {
	<-clientCh
	localTimestamps := make([]int, numClientsServer)
	fmt.Printf("%v: Client %d is running\n", localTimestamps, id)

	time.Sleep(2 * time.Second)

	go func() {
		for messageCount := 0; messageCount < 3; messageCount++ {
			localTimestamps[id]++

			timestampsCopy := make([]int, len(localTimestamps))
			copy(timestampsCopy, localTimestamps)
			serverCh <- Message{id, messageCount, timestampsCopy}

			fmt.Printf("%v: Client %d sends message %d\n", timestampsCopy, id, messageCount)
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
			// Check for causality violation
			DetectCausalityViolation(id, localTimestamps, msg)
			
			// Update timestamps after detection
			UpdateTimestampsWithMax(id, localTimestamps, msg.Timestamps)
			localTimestamps[id]++
			fmt.Printf("%v: Client %d receives message %d from Client %d\n", localTimestamps, id, msg.MessageCount, msg.ID)
		}
	}
}

func Server(serverCh chan Message, clientChs []chan Message, doneCh chan struct{}) {
	localTimestamps := make([]int, numClientsServer)
	fmt.Printf("%v: Server is running\n", localTimestamps)

	serverID := numClientsServer - 1
	for _, clientCh := range clientChs {
		clientCh <- Message{serverID, -1, localTimestamps}
	}

	var messageQueue []Message
	doneClients := 0

	for {
		select {
		case msg := <-serverCh:
			UpdateTimestampsWithMax(serverID, localTimestamps, msg.Timestamps)
			localTimestamps[serverID]++
			fmt.Printf("%v: Server receives message %d from Client %d\n", localTimestamps, msg.MessageCount, msg.ID)

			if rand.Intn(2) == 0 {
				localTimestamps[serverID]++
				timestampsCopy := make([]int, len(localTimestamps))
				copy(timestampsCopy, localTimestamps)

				for id, clientCh := range clientChs {
					if id != msg.ID {
						clientCh <- Message{msg.ID, msg.MessageCount, timestampsCopy}
					}
				}
				fmt.Printf("%v: Server forwards message %d from Client %d\n", localTimestamps, msg.MessageCount, msg.ID)

				// Insert message copy into local queue
				msgCopy := Message{
					ID:           msg.ID,
					MessageCount: msg.MessageCount,
					Timestamps:   timestampsCopy,
				}
				messageQueue = append(messageQueue, msgCopy)
				sort.Slice(messageQueue, func(i, j int) bool {
					return messageQueue[i].Timestamps[numClientsServer-1] < messageQueue[j].Timestamps[numClientsServer-1]
				})
			} else {
				fmt.Printf("Server drops message %d from Client %d\n", msg.MessageCount, msg.ID)
			}

		case <-doneCh:
			doneClients++
			if doneClients == numClients {
				fmt.Println("All clients are done sending messages.\nOrder of messages Clients should read:")
				for _, m := range messageQueue {
					fmt.Printf("%v: Message %d from Client %d\n", m.Timestamps, m.MessageCount, m.ID)
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
