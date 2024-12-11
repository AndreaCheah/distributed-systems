package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Page represents a memory page in the system
type Page struct {
    ID      int
    Data    []byte
    Owner   int
    Version int
    mu      sync.RWMutex
}

// CentralManager handles page ownership and request routing
type CentralManager struct {
    pages     map[int]*Page    // Map of page ID to page metadata
    pageOwner map[int]int      // Map of page ID to current owner
    mu        sync.RWMutex
}

// Client represents a node in the system
type Client struct {
    ID     int
    CM     *CentralManager
    pages  map[int]*Page
    mu     sync.RWMutex
}

// Configuration holds the command line parameters
type Config struct {
    mode      string
    clients   int
    workload  string
    faults    string
    scenario  string
}

func parseFlags() *Config {
    config := &Config{}
    
    flag.StringVar(&config.mode, "mode", "basic", "Operation mode (basic/fault-tolerant)")
    flag.IntVar(&config.clients, "clients", 10, "Number of clients")
    flag.StringVar(&config.workload, "workload", "random", "Workload type (random/read-intensive/write-intensive)")
    flag.StringVar(&config.faults, "faults", "none", "Fault injection mode (none/single/multiple/primary-and-backup)")
    flag.StringVar(&config.scenario, "scenario", "", "Simulation scenario")
    
    flag.Parse()
    return config
}

// NewCentralManager creates a new central manager instance
func NewCentralManager() *CentralManager {
    return &CentralManager{
        pages:     make(map[int]*Page),
        pageOwner: make(map[int]int),
    }
}

// NewClient creates a new client instance
func NewClient(id int, cm *CentralManager) *Client {
    return &Client{
        ID:    id,
        CM:    cm,
        pages: make(map[int]*Page),
    }
}

// ReadPage handles a read request from a client
func (cm *CentralManager) ReadPage(pageID int, clientID int) (*Page, error) {
    simulateNetworkLatency() // Simulate network delay for request
    
    cm.mu.RLock()
    defer cm.mu.RUnlock()
    
    page, exists := cm.pages[pageID]
    if !exists {
        return nil, fmt.Errorf("page %d not found", pageID)
    }
    
    ownerID := cm.pageOwner[pageID]
    if ownerID == clientID {
        return page, nil
    }
    
    simulateNetworkLatency() // Simulate network delay for page transfer
    
    return &Page{
        ID:      page.ID,
        Data:    append([]byte(nil), page.Data...),
        Owner:   page.Owner,
        Version: page.Version,
    }, nil
}

// WritePage handles a write request from a client
func (cm *CentralManager) WritePage(pageID int, clientID int, data []byte) error {
    simulateNetworkLatency() // Simulate network delay for request
    
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    page, exists := cm.pages[pageID]
    if !exists {
        page = &Page{
            ID:   pageID,
            Data: make([]byte, 0),
        }
        cm.pages[pageID] = page
    }
    
    if page.Owner != clientID {
        simulateNetworkLatency() // Simulate network delay for ownership transfer
    }
    
    cm.pageOwner[pageID] = clientID
    page.Owner = clientID
    page.Data = append([]byte(nil), data...)
    page.Version++
    
    return nil
}

// Operation represents a single read or write operation
type Operation struct {
    isWrite  bool
    pageID   int
    clientID int
    data     []byte    // only used for writes
}

// ExecuteOperation performs either a read or write operation
func (client *Client) ExecuteOperation(op Operation) error {
    if op.isWrite {
        return client.CM.WritePage(op.pageID, client.ID, op.data)
    }
    
    _, err := client.CM.ReadPage(op.pageID, client.ID)
    return err
}

// Initialize pages in the central manager
func (cm *CentralManager) initializePages(numPages int) {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    for i := 0; i < numPages; i++ {
        cm.pages[i] = &Page{
            ID:      i,
            Data:    []byte(fmt.Sprintf("initial-data-%d", i)),
            Owner:   0, // Initially owned by client 0
            Version: 0,
        }
        cm.pageOwner[i] = 0
    }
}

// OperationMetrics stores timing information for operations
type OperationMetrics struct {
    startTime time.Time
    endTime   time.Time
    opType    string
    pageID    int
    clientID  int
}

func (m *OperationMetrics) duration() time.Duration {
    return m.endTime.Sub(m.startTime)
}

// Generate a random workload based on configuration
func generateWorkload(config *Config) []Operation {
    rand.Seed(time.Now().UnixNano())
    operations := make([]Operation, 10) // Start with just 10 operations for testing
    numPages := 5 // Use 5 pages for testing
    
    for i := 0; i < 10; i++ {
        isWrite := rand.Float32() < 0.5 // 50% chance of write for random workload
        if config.workload == "read-intensive" {
            isWrite = rand.Float32() < 0.1 // 10% chance of write
        } else if config.workload == "write-intensive" {
            isWrite = rand.Float32() < 0.9 // 90% chance of write
        }
        
        operations[i] = Operation{
            isWrite:  isWrite,
            pageID:   rand.Intn(numPages),
            clientID: rand.Intn(config.clients),
            data:     []byte(fmt.Sprintf("data-%d", i)),
        }
    }
    return operations
}

// simulateNetworkLatency adds a random delay to simulate network communication
func simulateNetworkLatency() {
    // Random latency between 1-5ms
    delay := time.Duration(1+rand.Intn(4)) * time.Millisecond
    time.Sleep(delay)
}

func main() {
    config := parseFlags()
    
    if config.mode != "basic" {
        log.Fatal("Only basic mode is implemented in this version")
    }
    
    // Create central manager and initialize pages
    cm := NewCentralManager()
    numPages := 5
    cm.initializePages(numPages)
    
    // Create clients
    clients := make([]*Client, config.clients)
    for i := 0; i < config.clients; i++ {
        clients[i] = NewClient(i, cm)
    }
    
    // Generate workload
    operations := generateWorkload(config)
    
    fmt.Printf("Running Ivy with configuration:\n")
    fmt.Printf("Mode: %s\n", config.mode)
    fmt.Printf("Clients: %d\n", config.clients)
    fmt.Printf("Workload: %s\n", config.workload)
    fmt.Printf("Number of pages: %d\n", numPages)
    
    // Track metrics
    metrics := make([]OperationMetrics, len(operations))
    var totalReadTime time.Duration
    var totalWriteTime time.Duration
    readCount := 0
    writeCount := 0
    
    fmt.Printf("\nExecuting operations:\n")
    for i, op := range operations {
        opType := "READ"
        if op.isWrite {
            opType = "WRITE"
        }
        
        client := clients[op.clientID]
        
        metrics[i].startTime = time.Now()
        metrics[i].opType = opType
        metrics[i].pageID = op.pageID
        metrics[i].clientID = op.clientID
        
        err := client.ExecuteOperation(op)
        
        metrics[i].endTime = time.Now()
        duration := metrics[i].duration()
        
        if op.isWrite {
            totalWriteTime += duration
            writeCount++
        } else {
            totalReadTime += duration
            readCount++
        }
        
        if err != nil {
            fmt.Printf("Operation %d: %s on page %d by client %d - ERROR: %v (took %.2fms)\n",
                i, opType, op.pageID, op.clientID, err, float64(duration.Microseconds())/1000)
        } else {
            fmt.Printf("Operation %d: %s on page %d by client %d - SUCCESS (took %.2fms)\n",
                i, opType, op.pageID, op.clientID, float64(duration.Microseconds())/1000)
        }
    }
    
    // Print summary statistics
    fmt.Printf("\nPerformance Summary:\n")
    if readCount > 0 {
        avgReadTime := totalReadTime / time.Duration(readCount)
        fmt.Printf("Average read time:  %.2fms\n", float64(avgReadTime.Microseconds())/1000)
    }
    if writeCount > 0 {
        avgWriteTime := totalWriteTime / time.Duration(writeCount)
        fmt.Printf("Average write time: %.2fms\n", float64(avgWriteTime.Microseconds())/1000)
    }
    fmt.Printf("Total operations:   %d (Reads: %d, Writes: %d)\n", 
        len(operations), readCount, writeCount)
}