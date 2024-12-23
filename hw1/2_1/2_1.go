package main

import (
	"fmt"
	"time"
)

type Node struct {
	ID            	int
	In            	chan Message
	Out           	chan Message
	Data          	int
	CoordinatorID 	int
	IsCoordinator 	bool
	Timeout       	time.Duration
	LastHeartbeat 	time.Time
	SuccessorChs    	[]chan Message
}

type Message struct {
	Type			string
	Data			int
	Origin			int
	HopCount		int
}

const numNodes = 3
const initialCoordinatorID = numNodes - 1
const syncPeriod = 5 * time.Second
const timeout = syncPeriod * 2	// Timeout for detecting Coordinator failure
// const laterTimeout = syncPeriod * 5	// Simulate network latency, so that only one Node detects Coordinator failure

func main() {
	// Generate initial ring structure with Nodes and Channels
	genInitialRing()

	time.Sleep(120 * time.Second)
}

func genInitialRing() {
	// Initialise channels
	channels := make([]chan Message, numNodes)
	for id := range channels {
		channels[id] = make(chan Message, 10)
	}

	for id := 0; id < numNodes; id++ {
		// Assign in and out channels for each node
		in := channels[id]
		out := channels[(id+1)%numNodes]
		// Assign successors
		successors := []chan Message{
            channels[(id+1)%numNodes],
            channels[(id+2)%numNodes], // Next node in the ring as backup
        }

		// Set Coordinator, set initial data and timeout for all Nodes
		isCoordinator := (id == initialCoordinatorID)
		initialData := id * 10 // Simulates diverged data

		// Calculate distance from the Coordinator for staggered timeout
		distanceFromCoordinator := (id - initialCoordinatorID + numNodes) % numNodes
		nodeTimeout := timeout + time.Duration(distanceFromCoordinator)*syncPeriod

		// Generate goroutines to simulate Nodes
		go simNode(Node{
			ID: id, 
			In: in, 
			Out: out,
			Data: initialData, 
			CoordinatorID: initialCoordinatorID, 
			IsCoordinator: isCoordinator,
			Timeout: nodeTimeout,
			LastHeartbeat: time.Now(),
			SuccessorChs: successors,
		}, channels)
	}
}

// Sends message to immediate successor, if fails, sends to next immediate successor
func forwardMessage(node Node, msg Message) {
	for i, successorCh := range node.SuccessorChs {
		select {
		case successorCh <- msg:
			fmt.Printf("Node %d forwards %s message to successor %d.\n", node.ID, msg.Type, (node.ID+i+1)%numNodes)
            return // Message sent successfully, exit the function
		case <-time.After(100 * time.Millisecond):
			fmt.Printf("Node %d: Successor %d is down, trying next successor.\n", node.ID, (node.ID+i+1)%numNodes)
		}
	}
	fmt.Printf("Node %d: No active successors to forward message.\n", node.ID)
}

// Simulates a node
func simNode(node Node, channels []chan Message) {
	if node.IsCoordinator {
		// Ticker for periodically sending sync messages
		ticker := time.NewTicker(syncPeriod)
		defer ticker.Stop()

		// Timer to simulate Coordinator failure after 15 seconds
		failureTimer := time.NewTimer(10 * time.Second)

		for {
			select {
			// Periodically send sync message
			case <-ticker.C:
				node.Out <- Message{
					Type: "Sync",
					Data: node.Data,
					Origin: node.ID,
					HopCount: numNodes,
				}
				fmt.Printf("Node %d (Coordinator) sends sync message.\n", node.ID)
			case msg := <-node.In:
				if msg.Origin != node.ID {

				}
			case <-failureTimer.C:
				fmt.Printf("Node %d (Coordinator) fails.\n", node.ID)
				return
			}
		}
	} else {
		// Timer for detecting coordinator failure
		timer := time.NewTimer(node.Timeout)
		defer timer.Stop()

		for {
			select {
			case msg := <- node.In:
				// If Election message is received and the message circulates back to the original sender (the initiator of election),
				// the election initiator starts sending Announcement message
				if msg.Type == "Election" && msg.Origin == node.ID {
					timer.Reset(node.Timeout)
					announcementMsg := Message{
						Type:   "Announcement",
						Data:   msg.Data,
						Origin: node.ID,
						HopCount: numNodes,
					}
					forwardMessage(node, announcementMsg)
					fmt.Printf("Node %d starts announcing Node ID %d as Coordinator.\n", node.ID, msg.Data)
				}
				
				// If message is not from the node itself, forward the message
				if msg.Origin != node.ID {
					switch msg.Type {
					case "Sync":
						timer.Reset(node.Timeout)
						// Add a slight delay to simulate processing time
						time.Sleep(500 * time.Millisecond)
						fmt.Printf("Node %d sync its data from %d to %d, and forwards the sync message.\n", node.ID, node.Data, msg.Data)
						node.Data = msg.Data	// Sync node data with the data from sync message
						msg.HopCount--
						if msg.HopCount > 0 {
							forwardMessage(node, msg)
						}
					case "Election":
						// fmt.Printf("Node %d forwards Election message with candidate ID %d.\n", node.ID, msg.Data)
						timer.Reset(node.Timeout)
						if msg.Data < node.ID {
							msg.Data = node.ID	// Self-elect when own ID is higher than ID in Election message
						}
						msg.HopCount--
						if msg.HopCount > 0 {
							forwardMessage(node, msg)
						}
					case "Announcement":
						timer.Reset(node.Timeout)
						if msg.Data == node.ID {	// If the elected coordinator is the curr node, the curr node should set itself as Coordinator
							node.IsCoordinator = true
						} 
						node.CoordinatorID = msg.Data
						msg.HopCount--
						if msg.HopCount > 0 {
							forwardMessage(node, msg)
							// fmt.Printf("Node %d forwards Announcement message with Node %d as Coordinator.\n", node.ID, msg.Data)
						}
					}
				}
			case <-timer.C:
				fmt.Printf("Node %d detects Coordinator failure and initiates an election.\n", node.ID)

				// timer.Reset(node.Timeout)
				msg := Message{
					Type: "Election",
					Data: node.ID,
					Origin: node.ID,
					HopCount: numNodes,
				}
				forwardMessage(node, msg)
			}
		}
	}
}
