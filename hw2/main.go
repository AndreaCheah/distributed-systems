package main

import (
	"fmt"
	"sync"
	// "time"
)

// Process represents a node in the distributed system
type Process struct {
    ID           int
    clock        int
    mutex        sync.Mutex
    requestQueue *PriorityQueue
    channels     map[int]chan Message

    requesting   bool           // True if this process is requesting CS
    replyCount   int           // Number of replies received
    replied      map[int]bool  // Track which processes have replied
    inCS        bool 

    numProcesses int  // Add this to track total number of processes
}

// Message represents communication between processes
type Message struct {
    Type      string // "REQUEST", "REPLY", "RELEASE"
    Timestamp int
    SenderID  int
}

// PriorityQueue to maintain request ordering
type PriorityQueue struct {
    requests []Request
}

type Request struct {
    processID  int
    timestamp  int
}

func NewProcess(id int, numProcesses int) *Process {
    return &Process{
        ID:           id,
        clock:        0,
        requestQueue: &PriorityQueue{requests: make([]Request, 0)},
        channels:     make(map[int]chan Message),
        requesting:   false,
        replyCount:   0,
        replied:      make(map[int]bool),
        inCS:        false,
        numProcesses: numProcesses,
    }
}

// Increment local clock
func (p *Process) incrementClock() {
    p.mutex.Lock()
    p.clock++
    p.mutex.Unlock()
}

// Get current local clock value
func (p *Process) getClock() int {
    p.mutex.Lock()
    defer p.mutex.Unlock()
    return p.clock
}

// Inserts a request in the correct position based on timestamp (and process ID as tiebreaker)
func (pq *PriorityQueue) Push(request Request) {
    // Find the correct position to insert based on timestamp
    pos := 0
    for i, req := range pq.requests {
        // If new request's timestamp is greater, keep searching
        if request.timestamp > req.timestamp {
            pos = i + 1
        } else if request.timestamp == req.timestamp {
            // If timestamps are equal, use process ID as tiebreaker
            if request.processID > req.processID {
                pos = i + 1
            }
        } else {
            break
        }
    }

    // Insert at the found position
    pq.requests = append(pq.requests[:pos], append([]Request{request}, pq.requests[pos:]...)...)
}

// Removes and returns the highest priority request
func (pq *PriorityQueue) Pop() (Request, bool) {
    if len(pq.requests) == 0 {
        return Request{}, false
    }
    request := pq.requests[0]
    pq.requests = pq.requests[1:]
    return request, true
}

// Returns the highest priority request without removing it
func (pq *PriorityQueue) Peek() (Request, bool) {
    if len(pq.requests) == 0 {
        return Request{}, false
    }
    return pq.requests[0], true
}

// Removes a request by process ID
func (pq *PriorityQueue) Remove(processID int) {
    for i, req := range pq.requests {
        if req.processID == processID {
            pq.requests = append(pq.requests[:i], pq.requests[i+1:]...)
            return
        }
    }
}

// Checks if a process's request is in the queue
func (pq *PriorityQueue) Contains(processID int) bool {
    for _, req := range pq.requests {
        if req.processID == processID {
            return true
        }
    }
    return false
}

// Helper function to print queue state (for debugging)
func (pq *PriorityQueue) Print() {
    fmt.Print("Queue: ")
    for _, req := range pq.requests {
        fmt.Printf("(P%d,T%d) ", req.processID, req.timestamp)
    }
    fmt.Println()
}

// Add a test function to verify the priority queue implementation
func testPriorityQueue() {
    pq := &PriorityQueue{requests: make([]Request, 0)}
    
    // Test cases
    testRequests := []Request{
        {processID: 1, timestamp: 5},
        {processID: 2, timestamp: 3},
        {processID: 3, timestamp: 5},
        {processID: 4, timestamp: 2},
    }
    
    fmt.Println("Testing Priority Queue:")
    
    // Push all requests
    for _, req := range testRequests {
        pq.Push(req)
        fmt.Printf("After pushing (P%d,T%d): ", req.processID, req.timestamp)
        pq.Print()
    }
    
    // Test removal
    pq.Remove(2)
    fmt.Print("After removing process 2: ")
    pq.Print()
    
    // Test contains
    fmt.Printf("Contains process 1: %v\n", pq.Contains(1))
    fmt.Printf("Contains process 2: %v\n", pq.Contains(2))
    
    // Pop all elements
    fmt.Println("\nPopping all elements:")
    for {
        if req, ok := pq.Pop(); ok {
            fmt.Printf("Popped: (P%d,T%d)\n", req.processID, req.timestamp)
        } else {
            break
        }
    }
}

// HandleMessage processes incoming messages from other nodes
func (p *Process) HandleMessage(msg Message) {
    p.mutex.Lock()
    defer p.mutex.Unlock()
    
    // Update logical clock
    p.clock = max(p.clock, msg.Timestamp) + 1
    
    switch msg.Type {
    case "REQUEST":
        p.handleRequest(msg)
    case "REPLY":
        p.handleReply(msg)
    case "RELEASE":
        p.handleRelease(msg)
    }
}

// Helper function
func max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

// Processes incoming requests and decides whether to send an immediate REPLY
func (p *Process) handleRequest(msg Message) {
    // If we're not requesting or the incoming request has priority, send REPLY
    if !p.requesting || p.hasHigherPriority(msg) {
        p.sendReply(msg.SenderID)
    }
    // Add request to queue
    p.requestQueue.Push(Request{
        processID: msg.SenderID,
        timestamp: msg.Timestamp,
    })
}

func (p *Process) hasHigherPriority(msg Message) bool {
    if !p.requesting {
        return true
    }
    // Compare timestamps
    if msg.Timestamp < p.clock {
        return true
    }
    // If timestamps are equal, compare process IDs
    if msg.Timestamp == p.clock {
        return msg.SenderID < p.ID
    }
    return false
}

func (p *Process) sendReply(targetID int) {
    reply := Message{
        Type:      "REPLY",
        Timestamp: p.clock,
        SenderID:  p.ID,
    }
    p.channels[targetID] <- reply
}

func testMessageHandling() {
    fmt.Println("\nTesting Message Handling:")
    
    p1 := NewProcess(1, 3)
    p2 := NewProcess(2, 3)
    
    // Set up channels
    ch1 := make(chan Message, 10)
    ch2 := make(chan Message, 10)
    
    p1.channels[2] = ch2
    p2.channels[1] = ch1
    
    // Test REQUEST handling
    requestMsg := Message{
        Type:      "REQUEST",
        Timestamp: 5,
        SenderID:  2,
    }
    
    fmt.Println("Testing REQUEST message:")
    p1.HandleMessage(requestMsg)
    
    // Verify the request was added to queue
    if !p1.requestQueue.Contains(2) {
        fmt.Println("Error: Request not added to queue")
    } else {
        fmt.Println("Success: Request added to queue")
    }
    
    // Check if REPLY was sent
    select {
    case reply := <-ch2:
        fmt.Printf("Success: Received reply: %+v\n", reply)
    default:
        fmt.Println("Error: No reply sent")
    }
    
    // Test REPLY handling
    replyMsg := Message{
        Type:      "REPLY",
        Timestamp: 6,
        SenderID:  2,
    }
    
    fmt.Println("\nTesting REPLY message:")
    // Set requesting to true to test reply handling
    p1.requesting = true
    p1.HandleMessage(replyMsg)
    
    if p1.replied[2] && p1.replyCount == 1 {
        fmt.Println("Success: Reply processed correctly")
    } else {
        fmt.Println("Error: Reply not processed correctly")
    }
    
    // Test RELEASE handling
    releaseMsg := Message{
        Type:      "RELEASE",
        Timestamp: 7,
        SenderID:  2,
    }
    
    fmt.Println("\nTesting RELEASE message:")
    p1.HandleMessage(releaseMsg)
    
    if !p1.requestQueue.Contains(2) {
        fmt.Println("Success: Request removed from queue after RELEASE")
    } else {
        fmt.Println("Error: Request still in queue after RELEASE")
    }
}

// Tracks received replies when we're requesting CS
func (p *Process) handleReply(msg Message) {
    // Only process replies if we're requesting CS
    if !p.requesting {
        return
    }
    
    // If we haven't counted this reply yet
    if !p.replied[msg.SenderID] {
        p.replied[msg.SenderID] = true
        p.replyCount++
    }
}

// Manages queue cleanup when a process releases CS
func (p *Process) handleRelease(msg Message) {
    // Remove the sender's request from queue
    p.requestQueue.Remove(msg.SenderID)
    
    // If we're at the head of the queue now, we can enter CS
    if p.requesting {
        if req, ok := p.requestQueue.Peek(); ok && req.processID == p.ID {
            p.inCS = true
        }
    }
}

func main() {
    testPriorityQueue()
    testMessageHandling()
}