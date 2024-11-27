package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Node struct {
	ID       int
	NextNode *Node      // Pointer to the next node in the ring
	TokenCh  chan int   // Channel to receive tokens
	NeedsCS  bool       // Flag to indicate if the node needs the critical section
}

// Function to create a ring of nodes
func createRing(numNodes int) *Node {
	if numNodes <= 0 {
		return nil
	}

	// Create the first node
	head := &Node{ID: 0, TokenCh: make(chan int)}
	current := head

	// Create the rest of the nodes and link them
	for i := 1; i < numNodes; i++ {
		newNode := &Node{ID: i, TokenCh: make(chan int)}
		current.NextNode = newNode
		current = newNode
	}

	// Link the last node back to the head to complete the ring
	current.NextNode = head

	return head
}

// Function to simulate token passing and critical section entry
func startNode(node *Node, wg *sync.WaitGroup, csComplete chan bool) {
	defer wg.Done()
	for {
		// Wait for the token
		token := <-node.TokenCh

		// Check if the node needs the critical section
		if node.NeedsCS {
			fmt.Printf("Node %d entering critical section\n", node.ID)

			// Simulate work in the critical section
			time.Sleep(time.Duration(rand.Intn(500)+500) * time.Millisecond)

			fmt.Printf("Node %d exiting critical section\n", node.ID)

			// Notify that the CS has been completed
			csComplete <- true

			// Reset the flag after finishing the CS
			node.NeedsCS = false
		}

		// Pass the token to the next node
		fmt.Printf("Node %d passing the token to Node %d\n", node.ID, node.NextNode.ID)

		// Add a delay for better readability
		time.Sleep(200 * time.Millisecond)

		node.NextNode.TokenCh <- token
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Number of nodes and rounds
	numNodes := 10
	numRounds := 1

	// Create a ring of nodes
	ring := createRing(numNodes)

	// Wait group to ensure all goroutines complete
	var wg sync.WaitGroup

	// Channel to track CS completions
	csComplete := make(chan bool)

	// Start all nodes as goroutines
	current := ring
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go startNode(current, &wg, csComplete)
		current = current.NextNode
	}

	// Start the token passing
	fmt.Println("Starting token passing...")

	// Record start time
	startTime := time.Now()

	go func() {
		ring.TokenCh <- 1
	}()

	// Simulate each node requesting the CS in the round
	go func() {
		for round := 0; round < numRounds; round++ {
			for i := 0; i < numNodes; i++ {
				time.Sleep(time.Duration(rand.Intn(500)+500) * time.Millisecond) // Random delay
				current := ring
				for j := 0; j < i; j++ {
					current = current.NextNode
				}
				fmt.Printf("Node %d requesting critical section\n", current.ID)
				current.NeedsCS = true
			}
		}
	}()

	// Wait for all CS requests to complete
	for i := 0; i < numNodes*numRounds; i++ {
		<-csComplete
	}

	// Record end time
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	// Print performance
	fmt.Printf("All nodes completed their critical sections in %v\n", duration)

	// Wait indefinitely to keep the program alive (optional for debugging)
	wg.Wait()
}
