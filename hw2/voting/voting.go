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

	if !n.Voted || (n.VotedFor != nil && msg.Timestamp < n.VotedFor.Timestamp) {
		if n.Voted {
			releaseMsg := Message{Type: "RELEASE", SenderID: n.ID}
			n.SendMessage(releaseMsg, n.Quorum[n.VotedFor.SenderID-1])
		}
		
		n.Voted = true
		n.VotedFor = &PendingRequest{SenderID: msg.SenderID, Timestamp: msg.Timestamp}
		okMsg := Message{Type: "OK", SenderID: n.ID}
		n.SendMessage(okMsg, n.Quorum[msg.SenderID-1])
		
		fmt.Printf("Logical Clock [%d]: Node %d granted vote to Node %d (Request Timestamp: %d)\n", 
			n.LogicalClock, n.ID, msg.SenderID, msg.Timestamp)
	} else {
		n.RequestQueue = append(n.RequestQueue, 
			PendingRequest{SenderID: msg.SenderID, Timestamp: msg.Timestamp})
		fmt.Printf("Logical Clock [%d]: Node %d queued REQUEST from Node %d (Request Timestamp: %d)\n", 
			n.LogicalClock, n.ID, msg.SenderID, msg.Timestamp)
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

func (n *Node) handleRelease(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Voted = false
	n.VotedFor = nil
	fmt.Printf("Logical Clock [%d]: Node %d received RELEASE from Node %d and reset vote.\n", 
		n.LogicalClock, n.ID, msg.SenderID)

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

func (n *Node) RequestCriticalSection() {
	n.mu.Lock()
	n.VoteCount = 0
	n.mu.Unlock()

	n.IncrementClock()
	requestTime := n.LogicalClock
	fmt.Printf("Logical Clock [%d]: Node %d broadcasting REQUEST\n", 
		n.LogicalClock, n.ID)
	
	for _, neighbor := range n.Quorum {
		if neighbor != nil && neighbor.ID != n.ID {
			reqMsg := Message{Type: "REQUEST", SenderID: n.ID, Timestamp: requestTime}
			n.SendMessage(reqMsg, neighbor)
		}
	}

	n.VoteCond.L.Lock()
	for n.VoteCount < n.QuorumSize {
		n.VoteCond.Wait()
	}
	n.VoteCond.L.Unlock()

	fmt.Printf("Logical Clock [%d]: ====== Node %d ENTERING critical section ======\n", 
		n.LogicalClock, n.ID)
	
	// Simulate some work in critical section
	time.Sleep(time.Millisecond * 100)
	
	n.IncrementClock()
	fmt.Printf("Logical Clock [%d]: ====== Node %d EXITING critical section ======\n", 
		n.LogicalClock, n.ID)
	
	n.ReleaseCriticalSection()
}

func (n *Node) ReleaseCriticalSection() {
	fmt.Printf("Logical Clock [%d]: Node %d broadcasting RELEASE\n", 
		n.LogicalClock, n.ID)
	
	for _, neighbor := range n.Quorum {
		if neighbor != nil && neighbor.ID != n.ID {
			releaseMsg := Message{Type: "RELEASE", SenderID: n.ID}
			n.SendMessage(releaseMsg, neighbor)
		}
	}

	n.mu.Lock()
	n.VoteCount = 0
	n.mu.Unlock()
}

func main() {
	numNodes := flag.Int("n", 3, "Number of nodes in the system")
	flag.Parse()

	if *numNodes < 2 {
		fmt.Println("Number of nodes must be at least 2")
		return
	}

	quorumSize := (*numNodes / 2) + 1
	fmt.Printf("Starting system with %d nodes (quorum size: %d)\n", *numNodes, quorumSize)

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

	for i := 0; i < *numNodes; i++ {
		nodes[i].Quorum = make([]*Node, *numNodes)
		copy(nodes[i].Quorum, nodes)
	}

	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(*numNodes)
	for i := 0; i < *numNodes; i++ {
		go nodes[i].ReceiveMessage(&wg, done)
	}

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

	fmt.Println("Waiting for operations to complete...")
	opWg.Wait()

	executionTime := time.Since(startTime)
	fmt.Printf("Total execution time for %d nodes: %v\n", *numNodes, executionTime)

	time.Sleep(time.Millisecond * 100)

	close(done)

	for i := 0; i < *numNodes; i++ {
		close(nodes[i].MessageChan)
	}

	wg.Wait()

	fmt.Println("All operations completed successfully")
}