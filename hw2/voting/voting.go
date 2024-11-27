package main

import (
	"flag"
	"fmt"
	"sync"
	"time"
)

// Message represents communication between nodes in the distributed system
// Types can be "REQUEST" (asking for critical section access),
// "OK" (voting to grant access), or "RELEASE" (indicating finished with critical section)
type Message struct {
	Type      string 
	Timestamp int    // Logical timestamp for maintaining causality
	SenderID  int    // ID of the node sending the message
}

// PendingRequest represents a request that cannot be immediately granted
// and needs to be queued for later processing
type PendingRequest struct {
	SenderID  int // ID of the requesting node
	Timestamp int // Timestamp of when the request was made
}

// Node represents a participant in the distributed system
type Node struct {
	ID           int            // Unique identifier for the node
	LogicalClock int           // Lamport's logical clock for ordering events
	MessageChan  chan Message   // Channel for receiving messages from other nodes
	Voted        bool          // Whether this node has voted for another node's request
	VotedFor     *PendingRequest // Details of the request this node voted for
	Quorum       []*Node        // All nodes in the system
	VoteCount    int           // Number of OK votes received for current request
	QuorumSize   int           // Minimum votes needed for critical section access
	VoteCond     *sync.Cond    // Condition variable for synchronizing vote collection
	RequestQueue []PendingRequest // Queue of pending requests that couldn't be granted immediately
	mu           sync.Mutex     // Mutex for protecting shared state
}

// IncrementClock increases the logical clock value
// Called when any event occurs at the node
func (n *Node) IncrementClock() {
	n.LogicalClock++
}

// SendMessage sends a message to another node while maintaining logical clock ordering
func (n *Node) SendMessage(msg Message, recipient *Node) {
	n.IncrementClock()
	msg.Timestamp = n.LogicalClock
	recipient.MessageChan <- msg
}

// handleRequest processes incoming REQUEST messages for critical section access
// Implements the core logic of the mutual exclusion algorithm
func (n *Node) handleRequest(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Grant vote if either:
	// 1. Node hasn't voted yet
	// 2. New request has higher priority (lower timestamp) than currently voted request
	if !n.Voted || (n.VotedFor != nil && msg.Timestamp < n.VotedFor.Timestamp) {
		// If already voted, send RELEASE to previously voted node
		if n.Voted {
			releaseMsg := Message{Type: "RELEASE", SenderID: n.ID}
			n.SendMessage(releaseMsg, n.Quorum[n.VotedFor.SenderID-1])
		}
		
		// Grant vote to new request
		n.Voted = true
		n.VotedFor = &PendingRequest{SenderID: msg.SenderID, Timestamp: msg.Timestamp}
		okMsg := Message{Type: "OK", SenderID: n.ID}
		n.SendMessage(okMsg, n.Quorum[msg.SenderID-1])
		
		fmt.Printf("Logical Clock [%d]: Node %d granted vote to Node %d (Request Timestamp: %d)\n", 
			n.LogicalClock, n.ID, msg.SenderID, msg.Timestamp)
	} else {
		// Queue request for later processing
		n.RequestQueue = append(n.RequestQueue, 
			PendingRequest{SenderID: msg.SenderID, Timestamp: msg.Timestamp})
		fmt.Printf("Logical Clock [%d]: Node %d queued REQUEST from Node %d (Request Timestamp: %d)\n", 
			n.LogicalClock, n.ID, msg.SenderID, msg.Timestamp)
	}
}

// ReceiveMessage handles all incoming messages in a separate goroutine
func (n *Node) ReceiveMessage(wg *sync.WaitGroup, done chan struct{}) {
	defer wg.Done()
	for {
		select {
		case msg, ok := <-n.MessageChan:
			if !ok {
				return
			}
			// Update logical clock based on received message
			if msg.Timestamp > n.LogicalClock {
				n.LogicalClock = msg.Timestamp
			}
			n.IncrementClock()

			// Process message based on its type
			switch msg.Type {
			case "REQUEST":
				n.handleRequest(msg)
			case "OK":
				n.mu.Lock()
				n.VoteCount++
				n.VoteCond.Signal()
				n.mu.Unlock()
				fmt.Printf("Logical Clock [%d]: Node %d received OK vote from Node %d (Vote count: %d)\n", 
					n.LogicalClock, n.ID, msg.SenderID, n.VoteCount)
			case "RELEASE":
				n.handleRelease(msg)
			}
		case <-done:
			return
		}
	}
}

// handleRelease processes RELEASE messages and grants votes to queued requests
func (n *Node) handleRelease(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Reset voting state
	n.Voted = false
	n.VotedFor = nil
	fmt.Printf("Logical Clock [%d]: Node %d received RELEASE from Node %d and reset vote.\n", 
		n.LogicalClock, n.ID, msg.SenderID)

	// Process next queued request if any exists
	if len(n.RequestQueue) > 0 {
		nextRequest := n.RequestQueue[0]
		n.RequestQueue = n.RequestQueue[1:]
		
		n.Voted = true
		n.VotedFor = &nextRequest
		okMsg := Message{Type: "OK", SenderID: n.ID}
		n.SendMessage(okMsg, n.Quorum[nextRequest.SenderID-1])
		
		fmt.Printf("Logical Clock [%d]: Node %d granted queued vote to Node %d\n", 
			n.LogicalClock, n.ID, nextRequest.SenderID)
	}
}

// RequestCriticalSection initiates the process of requesting access to the critical section
func (n *Node) RequestCriticalSection() {
	// Reset vote count for new request
	n.mu.Lock()
	n.VoteCount = 0
	n.mu.Unlock()

	n.IncrementClock()
	requestTime := n.LogicalClock
	fmt.Printf("Logical Clock [%d]: Node %d broadcasting REQUEST\n", 
		n.LogicalClock, n.ID)
	
	// Broadcast REQUEST to all other nodes
	for _, neighbor := range n.Quorum {
		if neighbor != nil && neighbor.ID != n.ID {
			reqMsg := Message{Type: "REQUEST", SenderID: n.ID, Timestamp: requestTime}
			n.SendMessage(reqMsg, neighbor)
		}
	}

	// Wait for quorum of OK votes
	n.VoteCond.L.Lock()
	for n.VoteCount < n.QuorumSize {
		n.VoteCond.Wait()
	}
	n.VoteCond.L.Unlock()

	// Critical section entry and exit
	fmt.Printf("Logical Clock [%d]: ====== Node %d ENTERING critical section ======\n", 
		n.LogicalClock, n.ID)
	
	n.IncrementClock()
	fmt.Printf("Logical Clock [%d]: ====== Node %d EXITING critical section ======\n", 
		n.LogicalClock, n.ID)
	
	n.ReleaseCriticalSection()
}

// ReleaseCriticalSection broadcasts RELEASE messages to all other nodes
func (n *Node) ReleaseCriticalSection() {
	fmt.Printf("Logical Clock [%d]: Node %d broadcasting RELEASE\n", 
		n.LogicalClock, n.ID)
	
	// Broadcast RELEASE to all other nodes
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
	// Parse command line arguments
	numNodes := flag.Int("n", 3, "Number of nodes in the system")
	flag.Parse()

	if *numNodes < 2 {
		fmt.Println("Number of nodes must be at least 2")
		return
	}

	// Calculate quorum size (majority)
	quorumSize := (*numNodes / 2) + 1
	fmt.Printf("Starting system with %d nodes (quorum size: %d)\n", *numNodes, quorumSize)

	// Initialize nodes
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

	// Set up quorum for each node
	for i := 0; i < *numNodes; i++ {
		nodes[i].Quorum = make([]*Node, *numNodes)
		copy(nodes[i].Quorum, nodes)
	}

	// Channel for signaling shutdown
	done := make(chan struct{})

	// Start message receiving goroutines
	var wg sync.WaitGroup
	wg.Add(*numNodes)
	for i := 0; i < *numNodes; i++ {
		go nodes[i].ReceiveMessage(&wg, done)
	}

	// Start concurrent critical section requests
	var opWg sync.WaitGroup
	opWg.Add(*numNodes)

	startTime := time.Now()
	fmt.Println("Starting concurrent requests...")
	
	for i := 0; i < *numNodes; i++ {
		go func(node *Node) {
			defer opWg.Done()
			node.RequestCriticalSection()
		}(nodes[i])
	}

	// Wait for all operations to complete
	fmt.Println("Waiting for operations to complete...")
	opWg.Wait()

	executionTime := time.Since(startTime)
	fmt.Printf("Total execution time for %d nodes: %v\n", *numNodes, executionTime)

	// Clean up
	close(done)
	for i := 0; i < *numNodes; i++ {
		close(nodes[i].MessageChan)
	}
	wg.Wait()
}