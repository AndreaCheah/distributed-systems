package main

import (
	"flag"
	"fmt"
	"sync"
	"time"
)

// Message represents communication between nodes in the distributed system.
// Types can be:
// - "REQUEST": Asking for critical section access.
// - "OK": Voting to grant access.
// - "RELEASE": Indicating that a node has finished with the critical section.
// - "RESCIND": Asking to rescind a vote.
// - "RESCIND_OK": Confirming a rescind of a vote.
type Message struct {
	Type      string // Type of message ("REQUEST", "OK", "RELEASE", etc.).
	Timestamp int    // Logical timestamp for maintaining causality.
	SenderID  int    // ID of the node sending the message.
}

// PendingRequest represents a request queued for later processing when immediate granting is not possible.
type PendingRequest struct {
	SenderID  int // ID of the requesting node.
	Timestamp int // Timestamp of when the request was made.
}

// Node represents a participant in the distributed system.
type Node struct {
	ID              int              // Unique identifier for the node.
	LogicalClock    int              // Lamport's logical clock for ordering events.
	MessageChan     chan Message     // Channel for receiving messages from other nodes.
	Voted           bool             // Whether this node has voted for another node's request.
	VotedFor        *PendingRequest  // Details of the request this node voted for.
	Quorum          []*Node          // All nodes in the system.
	VoteCount       int              // Number of OK votes received for the current request.
	QuorumSize      int              // Minimum votes needed for critical section access.
	VoteCond        *sync.Cond       // Condition variable for synchronizing vote collection.
	RequestQueue    []PendingRequest // Queue of pending requests that couldn't be granted immediately.
	mu              sync.Mutex       // Mutex for protecting shared state.
	pendingRescind  bool             // Tracks if the node is waiting for a rescind response.
}

// IncrementClock increments the logical clock value. This is called when any event occurs at the node.
func (n *Node) IncrementClock() {
	n.LogicalClock++
}

// SendMessage sends a message to another node while maintaining logical clock ordering.
func (n *Node) SendMessage(msg Message, recipient *Node) {
	n.IncrementClock()          // Increment the logical clock before sending the message.
	msg.Timestamp = n.LogicalClock
	recipient.MessageChan <- msg // Send the message to the recipient's message channel.
}

// handleRequest handles a "REQUEST" message from another node.
func (n *Node) handleRequest(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.Voted {
		// If the node has not voted yet, grant the vote.
		n.Voted = true
		n.VotedFor = &PendingRequest{SenderID: msg.SenderID, Timestamp: msg.Timestamp}
		okMsg := Message{Type: "OK", SenderID: n.ID}
		n.SendMessage(okMsg, n.Quorum[msg.SenderID-1])
	} else {
		// Compare the current vote with the new request to decide if it should rescind.
		currentRequest := n.VotedFor
		newRequest := PendingRequest{SenderID: msg.SenderID, Timestamp: msg.Timestamp}

		if shouldRescindVote(currentRequest, &newRequest) {
			// Ask the current holder to give up the vote.
			rescindMsg := Message{Type: "RESCIND", SenderID: n.ID}
			n.SendMessage(rescindMsg, n.Quorum[currentRequest.SenderID-1])
			n.pendingRescind = true
			// Queue the new request for later.
			n.RequestQueue = append(n.RequestQueue, newRequest)
		} else {
			// Queue the new request without rescinding the current vote.
			n.RequestQueue = append(n.RequestQueue, newRequest)
		}
	}
}

// shouldRescindVote determines whether the current vote should be rescinded.
func shouldRescindVote(current, new *PendingRequest) bool {
	if new.Timestamp < current.Timestamp {
		return true
	}
	if new.Timestamp == current.Timestamp {
		return new.SenderID < current.SenderID
	}
	return false
}

// handleRescind processes a "RESCIND" message from another node.
func (n *Node) handleRescind(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If the node hasn't entered the critical section, it can give up its vote.
	if n.VoteCount < n.QuorumSize {
		rescindOkMsg := Message{Type: "RESCIND_OK", SenderID: n.ID}
		n.SendMessage(rescindOkMsg, n.Quorum[msg.SenderID-1])
		n.VoteCount--
	}
}

// handleRescindOk processes a "RESCIND_OK" message from another node.
func (n *Node) handleRescindOk(_ Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.pendingRescind {
		// Reset voting state and process the next request in the queue.
		n.Voted = false
		n.VotedFor = nil
		n.pendingRescind = false

		if len(n.RequestQueue) > 0 {
			// Grant the vote to the next request in the queue.
			nextRequest := n.RequestQueue[0]
			n.RequestQueue = n.RequestQueue[1:]
			n.Voted = true
			n.VotedFor = &nextRequest
			okMsg := Message{Type: "OK", SenderID: n.ID}
			n.SendMessage(okMsg, n.Quorum[nextRequest.SenderID-1])
		}
	}
}

// ReceiveMessage listens for messages from other nodes and handles them appropriately.
func (n *Node) ReceiveMessage(wg *sync.WaitGroup, done chan struct{}) {
	defer wg.Done()
	for {
		select {
		case msg, ok := <-n.MessageChan:
			if !ok {
				return
			}
			// Update logical clock.
			if msg.Timestamp > n.LogicalClock {
				n.LogicalClock = msg.Timestamp
			}
			n.IncrementClock()

			// Handle the message based on its type.
			switch msg.Type {
			case "REQUEST":
				n.handleRequest(msg)
			case "OK":
				n.mu.Lock()
				n.VoteCount++
				n.VoteCond.Signal() // Signal that a vote has been received.
				n.mu.Unlock()
			case "RELEASE":
				n.handleRelease(msg)
			case "RESCIND":
				n.handleRescind(msg)
			case "RESCIND_OK":
				n.handleRescindOk(msg)
			}
		case <-done:
			return
		}
	}
}

// handleRelease processes "RELEASE" messages and grants votes to queued requests.
func (n *Node) handleRelease(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Reset voting state.
	n.Voted = false
	n.VotedFor = nil
	fmt.Printf("Logical Clock [%d]: Node %d received RELEASE from Node %d and reset vote.\n", 
		n.LogicalClock, n.ID, msg.SenderID)

	// Process the next queued request if any exist.
	if len(n.RequestQueue) > 0 {
		nextRequest := n.RequestQueue[0]
		n.RequestQueue = n.RequestQueue[1:]

		// Grant the vote to the next request.
		n.Voted = true
		n.VotedFor = &nextRequest
		okMsg := Message{Type: "OK", SenderID: n.ID}
		n.SendMessage(okMsg, n.Quorum[nextRequest.SenderID-1])

		fmt.Printf("Logical Clock [%d]: Node %d granted queued vote to Node %d\n", 
			n.LogicalClock, n.ID, nextRequest.SenderID)
	}
}

// RequestCriticalSection initiates the process of requesting access to the critical section.
func (n *Node) RequestCriticalSection() {
	// Reset vote count for the new request.
	n.mu.Lock()
	n.VoteCount = 0
	n.mu.Unlock()

	n.IncrementClock()
	requestTime := n.LogicalClock
	fmt.Printf("Logical Clock [%d]: Node %d broadcasting REQUEST\n", n.LogicalClock, n.ID)

	// Broadcast REQUEST to all other nodes.
	for _, neighbor := range n.Quorum {
		if neighbor != nil && neighbor.ID != n.ID {
			reqMsg := Message{Type: "REQUEST", SenderID: n.ID, Timestamp: requestTime}
			n.SendMessage(reqMsg, neighbor)
		}
	}

	// Wait for quorum of OK votes.
	n.VoteCond.L.Lock()
	for n.VoteCount < n.QuorumSize {
		n.VoteCond.Wait()
	}
	n.VoteCond.L.Unlock()

	// Simulate critical section entry and exit.
	fmt.Printf("Logical Clock [%d]: ====== Node %d ENTERING critical section ======\n", n.LogicalClock, n.ID)
	n.IncrementClock()
	fmt.Printf("Logical Clock [%d]: ====== Node %d EXITING critical section ======\n", n.LogicalClock, n.ID)

	n.ReleaseCriticalSection()
}

// ReleaseCriticalSection broadcasts RELEASE messages to all other nodes.
func (n *Node) ReleaseCriticalSection() {
	fmt.Printf("Logical Clock [%d]: Node %d broadcasting RELEASE\n", n.LogicalClock, n.ID)

	// Broadcast RELEASE to all other nodes.
	for _, neighbor := range n.Quorum {
		if neighbor != nil && neighbor.ID != n.ID {
			releaseMsg := Message{Type: "RELEASE", SenderID: n.ID}
			n.SendMessage(releaseMsg, neighbor)
		}
	}

	// Reset vote count.
	n.mu.Lock()
	n.VoteCount = 0
	n.mu.Unlock()
}

func main() {
	// Parse command-line arguments.
	numNodes := flag.Int("n", 3, "Number of nodes in the system")
	flag.Parse()

	if *numNodes < 2 {
		fmt.Println("Number of nodes must be at least 2")
		return
	}

	// Calculate quorum size (majority).
	quorumSize := (*numNodes / 2) + 1
	fmt.Printf("Starting system with %d nodes (quorum size: %d)\n", *numNodes, quorumSize)

	// Initialize nodes.
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

	// Set up quorum for each node.
	for i := 0; i < *numNodes; i++ {
		nodes[i].Quorum = make([]*Node, *numNodes)
		copy(nodes[i].Quorum, nodes)
	}

	// Channel for signaling shutdown.
	done := make(chan struct{})

	// Start message receiving goroutines.
	var wg sync.WaitGroup
	wg.Add(*numNodes)
	for i := 0; i < *numNodes; i++ {
		go nodes[i].ReceiveMessage(&wg, done)
	}

	// Start concurrent critical section requests.
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

	// Wait for all operations to complete.
	fmt.Println("Waiting for operations to complete...")
	opWg.Wait()

	executionTime := time.Since(startTime)
	fmt.Printf("Total execution time for %d nodes: %v\n", *numNodes, executionTime)

	// Clean up.
	close(done)
	for i := 0; i < *numNodes; i++ {
		close(nodes[i].MessageChan)
	}
	wg.Wait()
}
