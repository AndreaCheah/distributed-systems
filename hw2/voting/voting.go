package main

import (
	"fmt"
	"sync"
)

type Message struct {
	Type      string // "REQUEST", "OK", "RELEASE"
	Timestamp int
	SenderID  int
}

type Node struct {
	ID           int
	LogicalClock int
	MessageChan  chan Message
	Voted        bool
	Quorum       []*Node
	VoteCount    int
	QuorumSize   int
	VoteCond     *sync.Cond // Used to signal when all votes are received
}

func (n *Node) IncrementClock() {
	n.LogicalClock++
}

func (n *Node) SendMessage(msg Message, recipient *Node) {
	n.IncrementClock()
	msg.Timestamp = n.LogicalClock
	recipient.MessageChan <- msg
}

func (n *Node) ReceiveMessage(wg *sync.WaitGroup) {
	defer wg.Done()
	for msg := range n.MessageChan {
		// Synchronize logical clock
		if msg.Timestamp > n.LogicalClock {
			n.LogicalClock = msg.Timestamp
		}
		n.IncrementClock()

		switch msg.Type {
		case "REQUEST":
			n.handleRequest(msg)
		case "OK":
			n.VoteCond.L.Lock()
			n.VoteCount++
			n.VoteCond.Signal()
			n.VoteCond.L.Unlock()
			fmt.Printf("Node %d received OK vote from Node %d (Vote count: %d)\n", n.ID, msg.SenderID, n.VoteCount)
		case "RELEASE":
			n.Voted = false
			fmt.Printf("Node %d received RELEASE from Node %d and reset vote.\n", n.ID, msg.SenderID)
		}
	}
}

func (n *Node) handleRequest(msg Message) {
	if !n.Voted {
		// Grant the vote
		n.Voted = true
		okMsg := Message{Type: "OK", SenderID: n.ID}
		n.SendMessage(okMsg, n.Quorum[msg.SenderID-1]) // Send OK to the requester
	} else {
		fmt.Printf("Node %d rejected REQUEST from Node %d (already voted).\n", n.ID, msg.SenderID)
	}
}

func (n *Node) RequestCriticalSection() {
	// Broadcast a REQUEST to all nodes in the quorum
	n.IncrementClock()
	fmt.Printf("Node %d broadcasting REQUEST (Timestamp: %d)\n", n.ID, n.LogicalClock)
	for _, neighbor := range n.Quorum {
		if neighbor != nil && neighbor.ID != n.ID {
			reqMsg := Message{Type: "REQUEST", SenderID: n.ID}
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
	n.ReleaseCriticalSection()
}

func (n *Node) ReleaseCriticalSection() {
	// Broadcast a RELEASE to all nodes in the quorum
	fmt.Printf("Node %d exiting critical section. Broadcasting RELEASE.\n", n.ID)
	for _, neighbor := range n.Quorum {
		if neighbor != nil && neighbor.ID != n.ID {
			releaseMsg := Message{Type: "RELEASE", SenderID: n.ID}
			n.SendMessage(releaseMsg, neighbor)
		}
	}
}

func main() {
	// Create nodes
	node1 := &Node{ID: 1, MessageChan: make(chan Message, 10), VoteCond: sync.NewCond(&sync.Mutex{})}
	node2 := &Node{ID: 2, MessageChan: make(chan Message, 10), VoteCond: sync.NewCond(&sync.Mutex{})}
	node3 := &Node{ID: 3, MessageChan: make(chan Message, 10), VoteCond: sync.NewCond(&sync.Mutex{})}

	// Set up quorums
	node1.Quorum = []*Node{node2, node3}
	node2.Quorum = []*Node{node1, node3}
	node3.Quorum = []*Node{node1, node2}

	// Set quorum sizes
	node1.QuorumSize = 2
	node2.QuorumSize = 2
	node3.QuorumSize = 2

	// Run nodes
	var wg sync.WaitGroup
	wg.Add(3)
	go node1.ReceiveMessage(&wg)
	go node2.ReceiveMessage(&wg)
	go node3.ReceiveMessage(&wg)

	// Node 1 requests critical section
	node1.RequestCriticalSection()

	// Close channels to stop the listening goroutines
	close(node1.MessageChan)
	close(node2.MessageChan)
	close(node3.MessageChan)

	wg.Wait()
}
