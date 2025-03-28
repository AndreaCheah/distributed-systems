// Package main implements a token ring mutual exclusion algorithm
// where nodes pass a token around in a ring to coordinate access to a critical section
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Node represents a process in the token ring
// Each node has an ID, a reference to the next node in the ring,
// a channel to receive the token, and a flag indicating if it needs
// to enter the critical section
type Node struct {
	ID       int        // Unique identifier for the node
	NextNode *Node      // Reference to the next node in the ring
	TokenCh  chan int   // Channel to receive the token
	NeedsCS  bool       // Flag indicating if node needs to enter critical section
}

// createRing creates a circular linked list of nodes
// returns the head node of the ring
func createRing(numNodes int) *Node {
	if numNodes <= 0 {
		return nil
	}

	// Create head node
	head := &Node{ID: 0, TokenCh: make(chan int)}
	current := head

	// Create remaining nodes and link them together
	for i := 1; i < numNodes; i++ {
		newNode := &Node{ID: i, TokenCh: make(chan int)}
		current.NextNode = newNode
		current = newNode
	}

	// Complete the ring by linking last node to head
	current.NextNode = head

	return head
}

// startNode runs the token passing logic for a single node
// It receives the token and either:
// 1. Enters critical section if needed
// 2. Passes token to next node
func startNode(node *Node, wg *sync.WaitGroup, csComplete chan bool, done chan bool) {
	defer wg.Done()
	for {
		select {
		case <-done: // Check if we should terminate
			return
		case token, ok := <-node.TokenCh: // Wait for token
			if !ok {
				return
			}

			// If this node needs to enter critical section
			if node.NeedsCS {
				fmt.Printf("Node %d entering critical section\n", node.ID)
				// Critical section execution would go here
				fmt.Printf("Node %d exiting critical section\n", node.ID)

				// Notify that critical section is complete
				select {
				case csComplete <- true:
				case <-done:
					return
				}

				node.NeedsCS = false
			}

			fmt.Printf("Node %d passing the token to Node %d\n", node.ID, node.NextNode.ID)

			// Pass token to next node
			select {
			case node.NextNode.TokenCh <- token:
			case <-done:
				return
			}
		}
	}
}

func main() {
	// Parse command line flags
	numNodesPtr := flag.Int("n", 5, "number of nodes in the ring")
	numRoundsPtr := flag.Int("r", 1, "number of rounds")
	
	flag.Parse()
	
	if *numNodesPtr <= 0 {
		fmt.Println("Number of nodes must be positive")
		return
	}

	// Initialize random number generator
	rand.Seed(time.Now().UnixNano())

	numNodes := *numNodesPtr
	numRounds := *numRoundsPtr

	// Create the token ring
	ring := createRing(numNodes)

	// Setup synchronization primitives
	var wg sync.WaitGroup
	csComplete := make(chan bool)    // Channel to signal completion of critical section
	done := make(chan bool)          // Channel to signal termination

	// Start all nodes
	current := ring
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go startNode(current, &wg, csComplete, done)
		current = current.NextNode
	}

	fmt.Printf("Starting token passing with %d nodes...\n", numNodes)

	startTime := time.Now()

	// Initialize token passing
	go func() {
		select {
		case ring.TokenCh <- 1: // Send initial token
		case <-done:
			return
		}
	}()

	// Simulate nodes requesting critical section access
	go func() {
		for round := 0; round < numRounds; round++ {
			for i := 0; i < numNodes; i++ {
				select {
				case <-done:
					return
				default:
					// Set NeedsCS flag for each node in sequence
					current := ring
					for j := 0; j < i; j++ {
						current = current.NextNode
					}
					fmt.Printf("Node %d requesting critical section\n", current.ID)
					current.NeedsCS = true
				}
			}
		}
	}()

	// Wait for all critical sections to complete or timeout
	completedCS := 0
	for completedCS < numNodes*numRounds {
		select {
		case <-csComplete:
			completedCS++
		case <-time.After(10 * time.Second): // Timeout after 10 seconds
			fmt.Println("Timeout waiting for critical sections to complete")
			close(done)
			wg.Wait()
			return
		}
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	fmt.Printf("All %d nodes completed their critical sections in %v\n", numNodes, duration)

	// Clean up
	close(done)          // Signal all nodes to terminate
	wg.Wait()           // Wait for all goroutines to finish
}