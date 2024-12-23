package main

import (
	"fmt"
	"math/rand"
	"time"
)

type Message struct {
	ID				int
	MessageCount	int
}

var numClients = 10
var numClientsServer = numClients + 1

func Client(id int, serverCh chan Message, clientCh chan Message) {
	// once receives message from server, means that server is ready. Then, client starts running
	msg := <- clientCh
	fmt.Printf("Client %d receives message %d from Server/Client %d\n", id, msg.MessageCount, msg.ID)
	fmt.Printf("Client %d is running\n", id)

	time.Sleep(2 * time.Second)	// buffer time for other clients to get running

	// sends message periodically
	go func() {
		for messageCount := 0; messageCount < 3; messageCount++ {
			serverCh <- Message{id, messageCount}
			fmt.Printf("Client %d sends message %d\n", id, messageCount)
			time.Sleep(2 * time.Second)
		}
	}()

	// listens for incoming messages
	for {
		select {
		case msg := <- clientCh:
			fmt.Printf("Client %d receives message %d from Client %d\n", id, msg.MessageCount, msg.ID)
		}
	}
}

func Server(serverCh chan Message, clientChs []chan Message) {
	fmt.Println("Server is running")
	serverID := numClientsServer - 1

	// sends message of -1 to each client to signal that the server is ready
	for _, clientCh := range clientChs {
		clientCh <- Message{serverID, -1}
	}

	for {
		// receives messages and decides to forward or drop
		msg := <- serverCh
		fmt.Printf("Server receives message %d from Client %d\n", msg.MessageCount, msg.ID)
		
		decision := rand.Intn(2)
		if decision == 0 {
			fmt.Printf("Server forwards message %d from Client %d\n", msg.MessageCount, msg.ID)
			for id, clientCh := range clientChs {
				if id != msg.ID {
					clientCh <- msg
				}
			}
		} else {
			fmt.Printf("Server drops message %d from Client %d\n", msg.MessageCount, msg.ID)
		}
	}
}

func main() {
	fmt.Println("Main goroutine is running")

	// create a server channel and 10 client channels for the 10 clients
	serverCh := make(chan Message, numClients)
	clientChs := make([]chan Message, numClients)
	for i := range clientChs {
		clientChs[i] = make(chan Message, numClients)
	}
	
	// create goroutines to simulate 10 clients and 1 server
	go Server(serverCh, clientChs)
	time.Sleep(1 * time.Second)	// add some buffer time to let server runs first
	for i := 0; i < numClients; i++ {
		go Client(i, serverCh, clientChs[i])
	}

	// let the program run for some time
	time.Sleep(10 * time.Second)
}