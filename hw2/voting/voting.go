package main

import (
	"flag"
	"fmt"
	"sync"
	"time"
)

type Message struct {
	Type      string // "REQUEST", "OK", "RELEASE"
	Timestamp int
	SenderID  int
}

type PendingRequest struct {
	SenderID  int
	Timestamp int
}

type Node struct {
	ID           int
	LogicalClock int
	MessageChan  chan Message
	Voted        bool
	VotedFor     *PendingRequest
	Quorum       []*Node
	VoteCount    int
	QuorumSize   int
	VoteCond     *sync.Cond
	RequestQueue []PendingRequest
	mu           sync.Mutex
}

func (n *Node) IncrementClock() {
	n.LogicalClock++
}

func (n *Node) SendMessage(msg Message, recipient *Node) {
	n.IncrementClock()
	msg.Timestamp = n.LogicalClock
	recipient.MessageChan <- msg
}

func (n *Node) handleRequest(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If we haven't voted or if this request has an earlier timestamp than the one we voted for
	if !n.Voted || (n.VotedFor != nil && msg.Timestamp < n.VotedFor.Timestamp) {
		if n.Voted {
			// Send RELEASE to the node we previously voted for
			releaseMsg := Message{Type: "RELEASE", SenderID: n.ID}
			n.SendMessage(releaseMsg, n.Quorum[n.VotedFor.SenderID-1])
		}
		
		// Grant the vote
		n.Voted = true
		n.VotedFor = &PendingRequest{SenderID: msg.SenderID, Timestamp: msg.Timestamp}
		okMsg := Message{Type: "OK", SenderID: n.ID}
		n.SendMessage(okMsg, n.Quorum[msg.SenderID-1])
		
		fmt.Printf("Node %d granted vote to Node %d (Timestamp: %d)\n", 
			n.ID, msg.SenderID, msg.Timestamp)
	} else {
		// Add to request queue if we've already voted
		n.RequestQueue = append(n.RequestQueue, 
			PendingRequest{SenderID: msg.SenderID, Timestamp: msg.Timestamp})
		fmt.Printf("Node %d queued REQUEST from Node %d (Timestamp: %d)\n", 
			n.ID, msg.SenderID, msg.Timestamp)
	}
}

func (n *Node) ReceiveMessage(wg *sync.WaitGroup, done chan struct{}) {
	defer wg.Done()
	for {
		select {
		case msg, ok := <-n.MessageChan:
			if !ok {
				return
			}
			// Synchronize logical clock
			if msg.Timestamp > n.LogicalClock {
				n.LogicalClock = msg.Timestamp
			}
			n.IncrementClock()

			switch msg.Type {
			case "REQUEST":
				n.handleRequest(msg)
			case "OK":
				n.mu.Lock()
				n.VoteCount++
				n.VoteCond.Signal()
				n.mu.Unlock()
				fmt.Printf("Node %d received OK vote from Node %d (Vote count: %d)\n", 
					n.ID, msg.SenderID, n.VoteCount)
			case "RELEASE":
				n.handleRelease(msg)
			}
		case <-done:
			return
		}
	}
}

func (n *Node) handleRelease(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Voted = false
	n.VotedFor = nil
	fmt.Printf("Node %d received RELEASE from Node %d and reset vote.\n", 
		n.ID, msg.SenderID)

	// Process queued requests if any exist
	if len(n.RequestQueue) > 0 {
		// Sort queue by timestamp
		nextRequest := n.RequestQueue[0]
		n.RequestQueue = n.RequestQueue[1:]
		
		// Grant vote to next request
		n.Voted = true
		n.VotedFor = &nextRequest
		okMsg := Message{Type: "OK", SenderID: n.ID}
		n.SendMessage(okMsg, n.Quorum[nextRequest.SenderID-1])
		
		fmt.Printf("Node %d granted queued vote to Node %d\n", 
			n.ID, nextRequest.SenderID)
	}
}

func (n *Node) RequestCriticalSection() {
	// Reset vote count before requesting
	n.mu.Lock()
	n.VoteCount = 0
	n.mu.Unlock()

	// Broadcast a REQUEST to all nodes in the quorum
	n.IncrementClock()
	requestTime := n.LogicalClock
	fmt.Printf("Node %d broadcasting REQUEST (Timestamp: %d)\n", n.ID, requestTime)
	
	for _, neighbor := range n.Quorum {
		if neighbor != nil && neighbor.ID != n.ID {
			reqMsg := Message{Type: "REQUEST", SenderID: n.ID, Timestamp: requestTime}
			n.SendMessage(reqMsg, neighbor)
		}
	}

	// Wait for all votes to be received
	n.VoteCond.L.Lock()
	for n.VoteCount < n.QuorumSize {
		n.VoteCond.Wait()
	}
	n.VoteCond.L.Unlock()

	fmt.Printf("Node %d entering critical section.\n", n.ID)
	// Simulate some work
	n.ReleaseCriticalSection()
}

func (n *Node) ReleaseCriticalSection() {
	fmt.Printf("Node %d exiting critical section. Broadcasting RELEASE.\n", n.ID)
	
	// Broadcast RELEASE to all nodes in the quorum
	for _, neighbor := range n.Quorum {
		if neighbor != nil && neighbor.ID != n.ID {
			releaseMsg := Message{Type: "RELEASE", SenderID: n.ID}
			n.SendMessage(releaseMsg, neighbor)
		}
	}

	// Reset vote count
	n.mu.Lock()
	n.VoteCount = 0
	n.mu.Unlock()
}

func main() {
	// Define command-line flag for number of nodes
	numNodes := flag.Int("n", 3, "Number of nodes in the system")
	flag.Parse()

	if *numNodes < 2 {
		fmt.Println("Number of nodes must be at least 2")
		return
	}

	// Calculate quorum size as majority (n/2 + 1)
	quorumSize := (*numNodes / 2) + 1

	fmt.Printf("Starting system with %d nodes (quorum size: %d)\n", *numNodes, quorumSize)

	// Create nodes dynamically
	nodes := make([]*Node, *numNodes)
	for i := 0; i < *numNodes; i++ {
		nodes[i] = &Node{
			ID:           i + 1,
			MessageChan:  make(chan Message, 10),
			VoteCond:     sync.NewCond(&sync.Mutex{}),
			RequestQueue: make([]PendingRequest, 0),
			QuorumSize:   quorumSize,
		}
	}

	// Set up quorums for all nodes
	for i := 0; i < *numNodes; i++ {
		nodes[i].Quorum = make([]*Node, *numNodes)
		copy(nodes[i].Quorum, nodes)
	}

	// Create done channel for cleanup
	done := make(chan struct{})

	// Start receiver goroutines for all nodes
	var wg sync.WaitGroup
	wg.Add(*numNodes)
	for i := 0; i < *numNodes; i++ {
		go nodes[i].ReceiveMessage(&wg, done)
	}

	// Create a WaitGroup for the request operations
	var opWg sync.WaitGroup
	opWg.Add(*numNodes)

	// Launch concurrent requests for all nodes
	startTime := time.Now()
	
	for i := 0; i < *numNodes; i++ {
		go func(node *Node) {
			defer opWg.Done()
			node.RequestCriticalSection()
		}(nodes[i])
	}

	// Wait for all operations to complete
	fmt.Println("Waiting for operations to complete...")
	opWg.Wait()

	// Calculate and print total execution time
	executionTime := time.Since(startTime)
	fmt.Printf("Total execution time for %d nodes: %v\n", *numNodes, executionTime)

	// Allow time for final messages to be processed
	time.Sleep(time.Millisecond * 100)

	// Signal done to all receiver goroutines
	close(done)

	// Close all message channels
	for i := 0; i < *numNodes; i++ {
		close(nodes[i].MessageChan)
	}

	// Wait for all receiver goroutines to finish
	wg.Wait()

	fmt.Println("All operations completed successfully")
}