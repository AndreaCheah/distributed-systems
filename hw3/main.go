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
    
    // First get the page reference using CM lock
    cm.mu.RLock()
    page, exists := cm.pages[pageID]
    cm.mu.RUnlock()
    
    if !exists {
        return nil, fmt.Errorf("page %d not found", pageID)
    }
    
    // Lock the specific page for reading
    page.mu.RLock()
    defer page.mu.RUnlock()
    
    simulateNetworkLatency() // Simulate network delay for page transfer
    
    // Create a copy of the page data
    dataCopy := append([]byte(nil), page.Data...)
    
    return &Page{
        ID:      page.ID,
        Data:    dataCopy,
        Owner:   page.Owner,
        Version: page.Version,
    }, nil
}

// WritePage handles a write request from a client
func (cm *CentralManager) WritePage(pageID int, clientID int, data []byte) error {
    simulateNetworkLatency() // Simulate network delay for request
    
    // First get or create the page using CM lock
    cm.mu.Lock()
    page, exists := cm.pages[pageID]
    if !exists {
        page = &Page{
            ID:   pageID,
            Data: make([]byte, 0),
        }
        cm.pages[pageID] = page
    }
    cm.mu.Unlock()
    
    // Lock the specific page for writing
    page.mu.Lock()
    defer page.mu.Unlock()
    
    if page.Owner != clientID {
        simulateNetworkLatency() // Simulate network delay for ownership transfer
        cm.mu.Lock()
        cm.pageOwner[pageID] = clientID
        cm.mu.Unlock()
        page.Owner = clientID
    }
    
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

// OperationResult stores the result of an operation
type OperationResult struct {
    operationID int
    duration    time.Duration
    err         error
    isWrite     bool
}

// ExecuteOperationsConcurrently runs all operations concurrently and collects results
func ExecuteOperationsConcurrently(clients []*Client, operations []Operation) []OperationResult {
    results := make([]OperationResult, len(operations))
    var wg sync.WaitGroup
    resultsChan := make(chan OperationResult, len(operations))

    // Launch each operation in its own goroutine
    for i, op := range operations {
        wg.Add(1)
        go func(opID int, operation Operation) {
            defer wg.Done()
            
            client := clients[operation.clientID]
            start := time.Now()
            
            err := client.ExecuteOperation(operation)
            
            resultsChan <- OperationResult{
                operationID: opID,
                duration:    time.Since(start),
                err:        err,
                isWrite:    operation.isWrite,
            }
        }(i, op)
    }

    // Start a goroutine to close results channel once all operations are done
    go func() {
        wg.Wait()
        close(resultsChan)
    }()

    // Collect results as they come in
    for result := range resultsChan {
        results[result.operationID] = result
    }

    return results
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
    fmt.Printf("Operations to perform: %d\n", len(operations))
    
    fmt.Printf("\nExecuting operations concurrently...\n")
    start := time.Now()
    
    // Execute operations concurrently and collect results
    results := ExecuteOperationsConcurrently(clients, operations)
    
    totalTime := time.Since(start)
    
    // Process and display results
    var totalReadTime time.Duration
    var totalWriteTime time.Duration
    readCount := 0
    writeCount := 0
    
    fmt.Printf("\nOperation Results:\n")
    for i, result := range results {
        opType := "READ"
        if operations[i].isWrite {
            opType = "WRITE"
        }
        
        if result.err != nil {
            fmt.Printf("Operation %d: %s on page %d by client %d - ERROR: %v (took %.2fms)\n",
                i, opType, operations[i].pageID, operations[i].clientID, result.err,
                float64(result.duration.Microseconds())/1000)
        } else {
            fmt.Printf("Operation %d: %s on page %d by client %d - SUCCESS (took %.2fms)\n",
                i, opType, operations[i].pageID, operations[i].clientID,
                float64(result.duration.Microseconds())/1000)
        }
        
        if operations[i].isWrite {
            totalWriteTime += result.duration
            writeCount++
        } else {
            totalReadTime += result.duration
            readCount++
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
    fmt.Printf("Total execution time: %.2fms\n", float64(totalTime.Microseconds())/1000)
}