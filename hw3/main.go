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
	copySets  map[int]map[int]bool  // pageID -> set of clientIDs that have copies
	clients   map[int]*Client
	writeQueue map[int][]WriteRequest  // pageID -> queue of write requests
	mu        sync.RWMutex
}

// Client represents a node in the system
type Client struct {
    ID     int
    CM     *CentralManager
    pages  map[int]*Page
    mu     sync.RWMutex
}

type WriteRequest struct {
    clientID int
    data     []byte
    done     chan error
}

// Configuration holds the command line parameters
type Config struct {
    mode           string
    clients        int
    workload       string
    faults         string
    scenario       string
    writeFraction  float64  
    numOperations  int      
}

func parseFlags() *Config {
    config := &Config{}
    
    flag.StringVar(&config.mode, "mode", "basic", "Operation mode (basic/fault-tolerant)")
    flag.IntVar(&config.clients, "clients", 10, "Number of clients")
    flag.StringVar(&config.workload, "workload", "random", "Workload type (random/read-intensive/write-intensive)")
    flag.StringVar(&config.faults, "faults", "none", "Fault injection mode (none/single/multiple/primary-and-backup)")
    flag.StringVar(&config.scenario, "scenario", "", "Simulation scenario")
    flag.Float64Var(&config.writeFraction, "write-fraction", -1.0, "Fraction of write operations (0.0-1.0)")
    flag.IntVar(&config.numOperations, "operations", 10, "Number of operations to perform")
    
    flag.Parse()

    // Set default write fractions based on workload type if not specified
    if config.writeFraction < 0 {
        switch config.workload {
        case "read-intensive":
            config.writeFraction = 0.1 // 10% writes by default
        case "write-intensive":
            config.writeFraction = 0.9 // 90% writes by default
        default:
            config.writeFraction = 0.5 // 50% writes for random workload
        }
    } else {
        // Validate write fraction
        if config.writeFraction < 0.0 || config.writeFraction > 1.0 {
            log.Fatal("Write fraction must be between 0.0 and 1.0")
        }
        
        // For read/write intensive workloads, validate the fraction makes sense
        if config.workload == "read-intensive" && config.writeFraction > 0.5 {
            log.Fatal("Read-intensive workload cannot have write fraction > 0.5")
        }
        if config.workload == "write-intensive" && config.writeFraction < 0.5 {
            log.Fatal("Write-intensive workload cannot have write fraction < 0.5")
        }
    }

    return config
}

// NewCentralManager creates a new central manager instance
func NewCentralManager() *CentralManager {
    return &CentralManager{
        pages:      make(map[int]*Page),
        pageOwner:  make(map[int]int),
        copySets:   make(map[int]map[int]bool),
        clients:    make(map[int]*Client),
        writeQueue: make(map[int][]WriteRequest),
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

func (c *Client) getLocalPage(pageID int) (*Page, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    
    page, exists := c.pages[pageID]
    if !exists {
        return nil, false
    }
    
    // Verify version with central manager
    c.CM.mu.RLock()
    cmPage, exists := c.CM.pages[pageID]
    c.CM.mu.RUnlock()
    
    if !exists || cmPage.Version > page.Version {
        // Local copy is stale, remove it
        delete(c.pages, pageID)
        fmt.Printf("Client %d: Detected stale version of page %d (local: %d, current: %d)\n",
            c.ID, pageID, page.Version, cmPage.Version)
        return nil, false
    }
    
    return page, true
}

func (c *Client) setLocalPage(page *Page) {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.pages[page.ID] = page
}

func (c *Client) invalidatePage(pageID int) {
    c.mu.Lock()
    delete(c.pages, pageID)
    c.mu.Unlock()
    fmt.Printf("Client %d: Invalidated cache for page %d\n", c.ID, pageID)
}

func (cm *CentralManager) RegisterClient(client *Client) {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    cm.clients[client.ID] = client
}

// ReadPage handles a read request from a client
func (cm *CentralManager) ReadPage(pageID int, clientID int) (*Page, error) {
    simulateNetworkLatency()
    
    cm.mu.RLock()
    page, exists := cm.pages[pageID]
    if !exists {
        cm.mu.RUnlock()
        return nil, fmt.Errorf("page %d not found", pageID)
    }
    
    // Add client to copy set
    if _, exists := cm.copySets[pageID]; !exists {
        cm.copySets[pageID] = make(map[int]bool)
    }
    cm.copySets[pageID][clientID] = true
    currentVersion := page.Version
    cm.mu.RUnlock()
    
    // Lock the specific page for reading
    page.mu.RLock()
    defer page.mu.RUnlock()
    
    // Create a copy of the page data
    dataCopy := append([]byte(nil), page.Data...)
    
    return &Page{
        ID:      page.ID,
        Data:    dataCopy,
        Owner:   page.Owner,
        Version: currentVersion,
    }, nil
}

// WritePage handles a write request from a client
func (cm *CentralManager) WritePage(pageID int, clientID int, data []byte) error {
    done := make(chan error, 1)
    req := WriteRequest{
        clientID: clientID,
        data:     data,
        done:     done,
    }

    cm.mu.Lock()
    if _, exists := cm.writeQueue[pageID]; !exists {
        cm.writeQueue[pageID] = make([]WriteRequest, 0)
    }
    cm.writeQueue[pageID] = append(cm.writeQueue[pageID], req)
    isFirst := len(cm.writeQueue[pageID]) == 1
    cm.mu.Unlock()

    if isFirst {
        // Process this request immediately
        go cm.processWriteRequest(pageID, req)
    }

    // Wait for write to complete
    err := <-done

    // If this request is done, process next in queue if any
    cm.mu.Lock()
    queue := cm.writeQueue[pageID]
    if len(queue) > 0 && queue[0].done == done {
        cm.writeQueue[pageID] = queue[1:]
        if len(cm.writeQueue[pageID]) > 0 {
            nextReq := cm.writeQueue[pageID][0]
            go cm.processWriteRequest(pageID, nextReq)
        }
    }
    cm.mu.Unlock()

    return err
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
    
    // Check local cache first
    if localPage, exists := client.getLocalPage(op.pageID); exists {
        fmt.Printf("Client %d: Cache hit for page %d (version %d)\n", 
            client.ID, op.pageID, localPage.Version)
        return nil
    }
    
    fmt.Printf("Client %d: Cache miss for page %d, fetching from CM\n", 
        client.ID, op.pageID)
    
    // Cache miss - fetch from central manager
    page, err := client.CM.ReadPage(op.pageID, client.ID)
    if err != nil {
        return err
    }
    
    // Store in local cache
    client.setLocalPage(page)
    
    // Update copySet in central manager
    client.CM.mu.Lock()
    if _, exists := client.CM.copySets[op.pageID]; !exists {
        client.CM.copySets[op.pageID] = make(map[int]bool)
    }
    client.CM.copySets[op.pageID][client.ID] = true
    client.CM.mu.Unlock()
    
    fmt.Printf("Client %d: Cached page %d (version %d)\n", 
        client.ID, op.pageID, page.Version)
    
    return nil
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

func (cm *CentralManager) processWriteRequest(pageID int, req WriteRequest) {
	cm.mu.Lock()
    // Verify this is the first request in queue
    if len(cm.writeQueue[pageID]) == 0 || cm.writeQueue[pageID][0].done != req.done {
        cm.mu.Unlock()
        req.done <- fmt.Errorf("write request out of order")
        return
    }
    cm.mu.Unlock()

	// Process invalidations
    cm.mu.Lock()
    if copySet, exists := cm.copySets[pageID]; exists {
        for cid := range copySet {
            if cid != req.clientID {
                fmt.Printf("CM: Sending invalidation for page %d to client %d\n", pageID, cid)
                if client, exists := cm.clients[cid]; exists {
                    client.invalidatePage(pageID)
                }
            }
        }
        // Clear the copy set, keeping only the writer
        cm.copySets[pageID] = map[int]bool{req.clientID: true}
    }

    page, exists := cm.pages[pageID]
    if !exists {
        page = &Page{
            ID:   pageID,
            Data: make([]byte, 0),
        }
        cm.pages[pageID] = page
    }

    // Transfer ownership
    if page.Owner != req.clientID {
        fmt.Printf("CM: Transferring ownership of page %d from client %d to client %d\n",
            pageID, page.Owner, req.clientID)
        cm.pageOwner[pageID] = req.clientID
        page.Owner = req.clientID
    }
    cm.mu.Unlock()

    // Perform the write
    page.mu.Lock()
    page.Data = append([]byte(nil), req.data...)
    page.Version++
    fmt.Printf("CM: Page %d updated to version %d by client %d\n", 
        pageID, page.Version, req.clientID)
    page.mu.Unlock()

    req.done <- nil
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
    operations := make([]Operation, config.numOperations)
    numPages := 5 // Use 5 pages for testing
    
    // Calculate exact number of writes needed
    numWrites := int(float64(config.numOperations) * config.writeFraction + 0.5) // Round to nearest integer
    
    // Create a slice to track which operations will be writes
    isWrite := make([]bool, config.numOperations)
    
    // Set the first numWrites operations to be writes
    for i := 0; i < numWrites; i++ {
        isWrite[i] = true
    }
    
    // Shuffle the isWrite slice to randomize write positions
    for i := len(isWrite) - 1; i > 0; i-- {
        j := rand.Intn(i + 1)
        isWrite[i], isWrite[j] = isWrite[j], isWrite[i]
    }
    
    // Generate the operations
    for i := 0; i < config.numOperations; i++ {
        operations[i] = Operation{
            isWrite:  isWrite[i],
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

func testCacheBehavior(clients []*Client) {
    fmt.Println("\n=== Testing Cache Behavior ===")
    
    testOps := []Operation{
        {isWrite: false, pageID: 0, clientID: 1}, // First read by client 1
        {isWrite: false, pageID: 0, clientID: 1}, // Second read by client 1 (should hit cache)
        {isWrite: false, pageID: 0, clientID: 2}, // Read by client 2 (should be cache miss)
        {isWrite: false, pageID: 0, clientID: 2}, // Second read by client 2 (should hit cache)
    }
    
    for i, op := range testOps {
        fmt.Printf("\nOperation %d:\n", i+1)
        err := clients[op.clientID].ExecuteOperation(op)
        if err != nil {
            fmt.Printf("Error: %v\n", err)
        }
    }
    
    // Print final copy set state
    fmt.Println("\nFinal copy set state:")
    clients[0].CM.mu.RLock()
    for pageID, copySet := range clients[0].CM.copySets {
        fmt.Printf("Page %d is cached by clients: ", pageID)
        for clientID := range copySet {
            fmt.Printf("%d ", clientID)
        }
        fmt.Println()
    }
    clients[0].CM.mu.RUnlock()
    fmt.Println("=== Cache Test Complete ===\n")
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
    
    // First run the cache behavior test
    testCacheBehavior(clients)
    
    // Then run the main workload
    fmt.Printf("\nRunning Ivy with configuration:\n")
    fmt.Printf("Mode: %s\n", config.mode)
    fmt.Printf("Clients: %d\n", config.clients)
    fmt.Printf("Workload: %s\n", config.workload)
    fmt.Printf("Write fraction: %.2f\n", config.writeFraction)
    fmt.Printf("Number of operations: %d\n", config.numOperations)
    fmt.Printf("Number of pages: %d\n", numPages)
    
    // Generate and execute workload
    operations := generateWorkload(config)
    fmt.Printf("Operations to perform: %d\n", len(operations))
    
    fmt.Printf("\nExecuting operations concurrently...\n")
    start := time.Now()
    
    // Execute operations concurrently and collect results
    results := ExecuteOperationsConcurrently(clients, operations)
    
    totalTime := time.Since(start)
    
    // Process and display results
    fmt.Printf("\nOperation Results:\n")
    
    var totalReadTime time.Duration
    var totalWriteTime time.Duration
    readCount := 0
    writeCount := 0
    
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