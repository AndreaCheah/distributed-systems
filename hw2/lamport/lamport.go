package main

import (
	"fmt"
	"time"
)

// State represents the state of a process in the mutual exclusion algorithm.
type State int
type MessageType int

const (
    Released State = iota // Process is not interested in entering the CS.
    Wanted                // Process wants to enter the CS.
    Held                  // Process is in the CS.
)

const (
    Request MessageType = iota
    Reply
)

// Process represents a single process in the distributed system.
type Process struct {
    ID              int            // Unique identifier for the process.
    Clock           int            // Logical clock (Lamport timestamp).
    State           State          // Current state of the process.
    Inbox           chan Message   // Channel to receive incoming messages.
    Deferred        map[int]bool   // Deferred replies to other processes.
    TotalProcs      int            // Total number of processes in the system.
    OtherProcs      []*Process     // References to other processes.
    ReplyChan       chan bool      // Channel to receive REPLY notifications.
    RequestTimestamp int           // Timestamp when the process requested CS.
}

// Message represents a message sent between processes.
type Message struct {
    Type      MessageType // REQUEST or REPLY
    Timestamp int         // Lamport timestamp of the sender
    FromID    int         // ID of the sender process
}

// IncrementClock increments the process's logical clock.
func (p *Process) IncrementClock() {
    p.Clock++
}

// Start the process's message handling loop.
func (p *Process) Start() {
    go func() {
        for msg := range p.Inbox {
            p.HandleMessage(msg)
        }
    }()
}

// HandleMessage processes incoming messages.
func (p *Process) HandleMessage(msg Message) {
    // Update the logical clock.
    p.Clock = max(p.Clock, msg.Timestamp) + 1

    switch msg.Type {
    case Request:
        fmt.Printf("Process %d received REQUEST from %d at time %d\n", p.ID, msg.FromID, p.Clock)

        // Determine if we should defer the reply.
        deferReply := false
        if p.State == Held || (p.State == Wanted && (p.RequestTimestamp < msg.Timestamp || (p.RequestTimestamp == msg.Timestamp && p.ID < msg.FromID))) {
            deferReply = true
        }

        if deferReply {
            p.Deferred[msg.FromID] = true
            fmt.Printf("Process %d deferred REPLY to %d\n", p.ID, msg.FromID)
        } else {
            // Send REPLY immediately.
            p.Clock++
            replyMsg := Message{
                Type:      Reply,
                Timestamp: p.Clock,
                FromID:    p.ID,
            }
            p.SendMessage(msg.FromID, replyMsg)
        }
    case Reply:
        fmt.Printf("Process %d received REPLY from %d at time %d\n", p.ID, msg.FromID, p.Clock)
        p.ReplyChan <- true
    }
}

// SendMessage sends a message to another process.
func (p *Process) SendMessage(toID int, msg Message) {
    // Send the message to the target process.
    for _, proc := range p.OtherProcs {
        if proc.ID == toID {
            proc.Inbox <- msg
            break
        }
    }
}


// RequestCS initiates a request to enter the critical section.
func (p *Process) RequestCS() {
    p.Clock++ // Increment clock to reflect the passage of time
    p.State = Wanted
    p.RequestTimestamp = p.Clock // Store the timestamp of the request
    fmt.Printf("Process %d is requesting CS at time %d\n", p.ID, p.Clock)

    // Send REQUEST to all other processes.
    for _, proc := range p.OtherProcs {
        msg := Message{
            Type:      Request,
            Timestamp: p.Clock,
            FromID:    p.ID,
        }
        p.SendMessage(proc.ID, msg)
    }

    // Wait until all REPLY messages are received.
    for i := 0; i < len(p.OtherProcs); i++ {
        <-p.ReplyChan
    }

    // Enter the critical section.
    p.State = Held
    fmt.Printf("Process %d has entered CS at time %d\n", p.ID, p.Clock)
}

// ReleaseCS releases the critical section.
func (p *Process) ReleaseCS() {
    p.State = Released
    p.Clock++
    fmt.Printf("Process %d is releasing CS at time %d\n", p.ID, p.Clock)

    // Send deferred REPLY messages.
    for procID := range p.Deferred {
        p.Clock++
        replyMsg := Message{
            Type:      Reply,
            Timestamp: p.Clock,
            FromID:    p.ID,
        }
        p.SendMessage(procID, replyMsg)
        fmt.Printf("Process %d sent REPLY to %d at time %d\n", p.ID, procID, p.Clock)
        delete(p.Deferred, procID)
    }

    // Reset RequestTimestamp
    p.RequestTimestamp = -1
}

func max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

func main() {
    // Create two processes.
    p1 := &Process{
        ID:              1,
        Clock:           0,
        State:           Released,
        Inbox:           make(chan Message, 10),
        Deferred:        make(map[int]bool),
        TotalProcs:      2,
        ReplyChan:       make(chan bool),
        RequestTimestamp: -1,
    }

    p2 := &Process{
        ID:              2,
        Clock:           0,
        State:           Released,
        Inbox:           make(chan Message, 10),
        Deferred:        make(map[int]bool),
        TotalProcs:      2,
        ReplyChan:       make(chan bool),
        RequestTimestamp: -1,
    }

    // Set references to other processes.
    p1.OtherProcs = []*Process{p2}
    p2.OtherProcs = []*Process{p1}

    // Start the processes' message handling loops.
    p1.Start()
    p2.Start()

    // Simulate process 1 requesting the critical section.
    go func() {
        p1.RequestCS()
        // Simulate doing work in the critical section.
        time.Sleep(1 * time.Second)
        p1.ReleaseCS()
    }()

    // Simulate process 2 requesting the critical section after a delay.
    go func() {
        // Simulate delay before requesting CS
        time.Sleep(2 * time.Second)
        p2.RequestCS()
        // Simulate doing work in the critical section.
        time.Sleep(1 * time.Second)
        p2.ReleaseCS()
    }()

    // Wait for user input to end the program.
    var input string
    fmt.Scanln(&input)
}


