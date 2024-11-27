package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Node struct {
	ID       int
	NextNode *Node
	TokenCh  chan int
	NeedsCS  bool
}

// Function to create a ring of nodes
func createRing(numNodes int) *Node {
	if numNodes <= 0 {
		return nil
	}

	head := &Node{ID: 0, TokenCh: make(chan int)}
	current := head

	for i := 1; i < numNodes; i++ {
		newNode := &Node{ID: i, TokenCh: make(chan int)}
		current.NextNode = newNode
		current = newNode
	}

	current.NextNode = head

	return head
}

// Function to simulate token passing and critical section entry
func startNode(node *Node, wg *sync.WaitGroup, csComplete chan bool) {
	defer wg.Done()
	for {
		token := <-node.TokenCh

		if node.NeedsCS {
			fmt.Printf("Node %d entering critical section\n", node.ID)

			time.Sleep(time.Duration(rand.Intn(500)+500) * time.Millisecond)

			fmt.Printf("Node %d exiting critical section\n", node.ID)

			csComplete <- true

			node.NeedsCS = false
		}

		fmt.Printf("Node %d passing the token to Node %d\n", node.ID, node.NextNode.ID)

		time.Sleep(200 * time.Millisecond)

		node.NextNode.TokenCh <- token
	}
}

func main() {
	// Define command line flags
	numNodesPtr := flag.Int("n", 5, "number of nodes in the ring")
	numRoundsPtr := flag.Int("r", 1, "number of rounds")
	
	// Parse command line arguments
	flag.Parse()
	
	// Validate input
	if *numNodesPtr <= 0 {
		fmt.Println("Number of nodes must be positive")
		return
	}

	rand.Seed(time.Now().UnixNano())

	// Use the command line arguments
	numNodes := *numNodesPtr
	numRounds := *numRoundsPtr

	// Create a ring of nodes
	ring := createRing(numNodes)

	var wg sync.WaitGroup
	csComplete := make(chan bool)

	// Start all nodes as goroutines
	current := ring
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go startNode(current, &wg, csComplete)
		current = current.NextNode
	}

	fmt.Printf("Starting token passing with %d nodes...\n", numNodes)

	startTime := time.Now()

	go func() {
		ring.TokenCh <- 1
	}()

	go func() {
		for round := 0; round < numRounds; round++ {
			for i := 0; i < numNodes; i++ {
				time.Sleep(time.Duration(rand.Intn(500)+500) * time.Millisecond)
				current := ring
				for j := 0; j < i; j++ {
					current = current.NextNode
				}
				fmt.Printf("Node %d requesting critical section\n", current.ID)
				current.NeedsCS = true
			}
		}
	}()

	for i := 0; i < numNodes*numRounds; i++ {
		<-csComplete
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	fmt.Printf("All %d nodes completed their critical sections in %v\n", numNodes, duration)

	wg.Wait()
}