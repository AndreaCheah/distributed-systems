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

func startNode(node *Node, wg *sync.WaitGroup, csComplete chan bool, done chan bool) {
	defer wg.Done()
	for {
		select {
		case <-done:
			return
		case token, ok := <-node.TokenCh:
			if !ok {
				// Channel is closed, terminate gracefully
				return
			}

			if node.NeedsCS {
				fmt.Printf("Node %d entering critical section\n", node.ID)

				fmt.Printf("Node %d exiting critical section\n", node.ID)

				select {
				case csComplete <- true:
				case <-done:
					return
				}

				node.NeedsCS = false
			}

			fmt.Printf("Node %d passing the token to Node %d\n", node.ID, node.NextNode.ID)

			// Try to pass the token, but also check if we should terminate
			select {
			case node.NextNode.TokenCh <- token:
			case <-done:
				return
			}
		}
	}
}

func main() {
	numNodesPtr := flag.Int("n", 5, "number of nodes in the ring")
	numRoundsPtr := flag.Int("r", 1, "number of rounds")
	
	flag.Parse()
	
	if *numNodesPtr <= 0 {
		fmt.Println("Number of nodes must be positive")
		return
	}

	rand.Seed(time.Now().UnixNano())

	numNodes := *numNodesPtr
	numRounds := *numRoundsPtr

	ring := createRing(numNodes)

	var wg sync.WaitGroup
	csComplete := make(chan bool)
	done := make(chan bool)

	current := ring
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go startNode(current, &wg, csComplete, done)
		current = current.NextNode
	}

	fmt.Printf("Starting token passing with %d nodes...\n", numNodes)

	startTime := time.Now()

	// Start token passing with timeout handling
	go func() {
		select {
		case ring.TokenCh <- 1:
		case <-done:
			return
		}
	}()

	// Request critical sections
	go func() {
		for round := 0; round < numRounds; round++ {
			for i := 0; i < numNodes; i++ {
				select {
				case <-done:
					return
				default:
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

	// Wait for all critical sections to complete
	completedCS := 0
	for completedCS < numNodes*numRounds {
		select {
		case <-csComplete:
			completedCS++
		case <-time.After(10 * time.Second): // Add timeout
			fmt.Println("Timeout waiting for critical sections to complete")
			close(done)
			wg.Wait()
			return
		}
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	fmt.Printf("All %d nodes completed their critical sections in %v\n", numNodes, duration)

	// Signal all nodes to terminate
	close(done)

	// Wait for all goroutines to finish
	wg.Wait()
}