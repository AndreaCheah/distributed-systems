package main

import (
    "flag"
    "fmt"
    "log"
    "math/rand"
    "sync"
    "time"
)

func parseFlags() *Config {
    config := &Config{}
    
    flag.StringVar(&config.mode, "mode", "basic", "Operation mode (basic/ft)")
    flag.IntVar(&config.clients, "clients", 10, "Number of clients")
    flag.StringVar(&config.workload, "workload", "random", "Workload type (random/read-intensive/write-intensive)")
    flag.StringVar(&config.faults, "faults", "none", "Fault injection mode (none/single/multiple/primary-and-backup)")
    flag.StringVar(&config.scenario, "scenario", "", "Simulation scenario")
    flag.Float64Var(&config.writeFraction, "write-fraction", -1.0, "Fraction of write operations (0.0-1.0)")
    flag.IntVar(&config.numOperations, "operations", 10, "Number of operations to perform")
    
    // Add FT-specific flags
    flag.IntVar(&config.primaryFailures, "primary-failures", 0, "Number of times primary CM fails")
    flag.IntVar(&config.primaryRestarts, "primary-restarts", 0, "Number of times primary CM restarts")
    flag.IntVar(&config.backupFailures, "backup-failures", 0, "Number of times backup CM fails")
    flag.IntVar(&config.backupRestarts, "backup-restarts", 0, "Number of times backup CM restarts")
    flag.IntVar(&config.failureInterval, "failure-interval", 10, "Number of operations between failures")
    flag.IntVar(&config.restartDelay, "restart-delay", 1000, "Milliseconds to wait before restart")
    
    flag.Parse()

    if config.writeFraction < 0 {
        switch config.workload {
        case "read-intensive":
            config.writeFraction = 0.1
        case "write-intensive":
            config.writeFraction = 0.9
        default:
            config.writeFraction = 0.5
        }
    }

    // Validate FT-specific configuration
    if config.mode == "ft" {
        validateFTConfig(config)
    }

    return config
}

func validateFTConfig(config *Config) {
    if config.primaryRestarts > config.primaryFailures {
        log.Fatal("Primary CM restart count cannot exceed failure count")
    }
    if config.backupRestarts > config.backupFailures {
        log.Fatal("Backup CM restart count cannot exceed failure count")
    }
    if config.primaryFailures > 0 && config.failureInterval <= 0 {
        log.Fatal("Failure interval must be positive when failures are configured")
    }
}

func generateWorkload(config *Config) []Operation {
    rand.Seed(time.Now().UnixNano())
    operations := make([]Operation, config.numOperations)
    numPages := 5
    
    numWrites := int(float64(config.numOperations) * config.writeFraction + 0.5)
    
    isWrite := make([]bool, config.numOperations)
    for i := 0; i < numWrites; i++ {
        isWrite[i] = true
    }
    
    for i := len(isWrite) - 1; i > 0; i-- {
        j := rand.Intn(i + 1)
        isWrite[i], isWrite[j] = isWrite[j], isWrite[i]
    }
    
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

func simulateNetworkLatency() {
    delay := time.Duration(1+rand.Intn(4)) * time.Millisecond
    time.Sleep(delay)
}

func ExecuteOperationsConcurrently(clients []*Client, operations []Operation, config *Config) []OperationResult {
    results := make([]OperationResult, len(operations))
    var wg sync.WaitGroup
    resultsChan := make(chan OperationResult, len(operations))

    // Get reference to primary CM for failure injection
    var primaryCM *PrimaryCentralManager
    var backupCM *PrimaryCentralManager
    if config.mode == "ft" {
        primaryCM = clients[0].CM.(*PrimaryCentralManager)
        backupCM = primaryCM.partner
        primaryCM.failures = config.primaryFailures
    }

    for i, op := range operations {
        wg.Add(1)
        go func(opID int, operation Operation) {
            defer wg.Done()
            
            // Simulate primary failure if conditions are met
            if primaryCM != nil && primaryCM.failures > 0 {
                opCount := primaryCM.opCounter.Add(1)
                if opCount == int32(config.failureInterval) && !primaryCM.failed.Load() {
                    fmt.Printf("\nTriggering primary CM failure after %d operations\n", opCount)
                    primaryCM.failed.Store(true)
                    primaryCM.failures--
                    
                    // Promote backup to active
                    if backupCM != nil {
                        err := backupCM.PromoteToActive()
                        if err != nil {
                            fmt.Printf("Error promoting backup: %v\n", err)
                        }
                    }
                }
            }
            
            client := clients[operation.clientID]
            start := time.Now()
            
            // Check if primary has failed
            var err error
            if primaryCM != nil && primaryCM.failed.Load() && client.CM == primaryCM {
                // Switch client to use backup CM
                client.CM = backupCM
                fmt.Printf("Client %d switched to backup CM\n", client.ID)
            }
            
            err = client.ExecuteOperation(operation)
            
            resultsChan <- OperationResult{
                operationID: opID,
                duration:    time.Since(start),
                err:        err,
                isWrite:    operation.isWrite,
            }
        }(i, op)
    }

    go func() {
        wg.Wait()
        close(resultsChan)
    }()

    for result := range resultsChan {
        results[result.operationID] = result
    }

    return results
}

// Update testCacheBehavior to use interface methods
func testCacheBehavior(clients []*Client) {
    fmt.Println("\n=== Testing Cache Behavior ===")
    
    testOps := []Operation{
        {isWrite: false, pageID: 0, clientID: 1},
        {isWrite: false, pageID: 0, clientID: 1},
        {isWrite: false, pageID: 0, clientID: 2},
        {isWrite: false, pageID: 0, clientID: 2},
    }
    
    for i, op := range testOps {
        fmt.Printf("\nOperation %d:\n", i+1)
        err := clients[op.clientID].ExecuteOperation(op)
        if err != nil {
            fmt.Printf("Error: %v\n", err)
        }
    }
    
    fmt.Println("\nFinal copy set state:")
    clients[0].CM.RLock()
    for pageID, copySet := range clients[0].CM.GetCopySets() {
        fmt.Printf("Page %d is cached by clients: ", pageID)
        for clientID := range copySet {
            fmt.Printf("%d ", clientID)
        }
        fmt.Println()
    }
    clients[0].CM.RUnlock()
    fmt.Println("=== Cache Test Complete ===")
}

// Helper function to print results
func printResults(results []OperationResult, operations []Operation, totalTime time.Duration) {
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

func main() {
    config := parseFlags()
    
    // Declare manager variable
    var manager ManagerInterface
    
    // Create appropriate manager based on mode
    var primaryCM *PrimaryCentralManager
    var backupCM *PrimaryCentralManager
    
    if config.mode == "ft" {
        // Create primary and backup CMs for fault-tolerant mode
        primaryCM = NewPrimaryCentralManager(true)
        backupCM = NewPrimaryCentralManager(false)
        SetupReplication(primaryCM, backupCM)
        
        // Start periodic sync
        primaryCM.startPeriodicSync(5 * time.Second)
        
        manager = primaryCM
        
        fmt.Println("\n=== Initial Fault Tolerant Setup ===")
        fmt.Printf("Primary CM created and active: %v\n", primaryCM.isActive)
        fmt.Printf("Backup CM created and active: %v\n", backupCM.isActive)
    } else {
        manager = NewCentralManager()
    }
    
    // Rest of the main function remains the same...
    // Initialize pages
    numPages := 5
    manager.initializePages(numPages)
    
    if config.mode == "ft" {
        fmt.Println("\n=== Verifying Initial Page Sync ===")
        primaryCM.mu.RLock()
        backupCM.mu.RLock()
        
        fmt.Println("\nPrimary CM pages:")
        for pageID, page := range primaryCM.pages {
            page.mu.RLock()
            fmt.Printf("Page %d: Version %d, Owner %d, Data %s\n",
                pageID, page.Version, page.Owner, string(page.Data))
            page.mu.RUnlock()
        }
        
        fmt.Println("\nBackup CM pages:")
        for pageID, page := range backupCM.pages {
            page.mu.RLock()
            fmt.Printf("Page %d: Version %d, Owner %d, Data %s\n",
                pageID, page.Version, page.Owner, string(page.Data))
            page.mu.RUnlock()
        }
        
        primaryCM.mu.RUnlock()
        backupCM.mu.RUnlock()
    }
    
    // Create and register clients
    clients := make([]*Client, config.clients)
    for i := 0; i < config.clients; i++ {
        clients[i] = NewClient(i, manager)
        manager.RegisterClient(clients[i])
    }
    
    // Run cache behavior tests if not in FT mode
    if _, ok := manager.(*CentralManager); ok {
        testCacheBehavior(clients)
    } else {
        fmt.Println("\nSkipping cache behavior test in FT mode")
    }
    
    fmt.Printf("\nRunning Ivy with configuration:\n")
    fmt.Printf("Mode: %s\n", config.mode)
    fmt.Printf("Clients: %d\n", config.clients)
    fmt.Printf("Workload: %s\n", config.workload)
    fmt.Printf("Write fraction: %.2f\n", config.writeFraction)
    fmt.Printf("Number of operations: %d\n", config.numOperations)
    fmt.Printf("Number of pages: %d\n", numPages)
    
    operations := generateWorkload(config)
    fmt.Printf("Operations to perform: %d\n", len(operations))
    
    fmt.Printf("\nExecuting operations concurrently...\n")
    start := time.Now()
    
    results := ExecuteOperationsConcurrently(clients, operations, config)
    
    totalTime := time.Since(start)
    
    // Print results and statistics
    printResults(results, operations, totalTime)
    
    if config.mode == "ft" {
        fmt.Println("\n=== Final Sync Verification ===")
        fmt.Println("\nVerifying final state synchronization between Primary and Backup...")
        
        primaryCM.mu.RLock()
        backupCM.mu.RLock()
        
        // Compare pages
        syncSuccess := true
        for pageID, primaryPage := range primaryCM.pages {
            backupPage, exists := backupCM.pages[pageID]
            if !exists {
                fmt.Printf("ERROR: Page %d exists in Primary but not in Backup\n", pageID)
                syncSuccess = false
                continue
            }
            
            primaryPage.mu.RLock()
            backupPage.mu.RLock()
            
            if primaryPage.Version != backupPage.Version ||
               primaryPage.Owner != backupPage.Owner ||
               string(primaryPage.Data) != string(backupPage.Data) {
                fmt.Printf("ERROR: Page %d mismatch:\n", pageID)
                fmt.Printf("  Primary: Version %d, Owner %d, Data %s\n",
                    primaryPage.Version, primaryPage.Owner, string(primaryPage.Data))
                fmt.Printf("  Backup:  Version %d, Owner %d, Data %s\n",
                    backupPage.Version, backupPage.Owner, string(backupPage.Data))
                syncSuccess = false
            }
            
            primaryPage.mu.RUnlock()
            backupPage.mu.RUnlock()
        }
        
        // Compare copy sets
        for pageID, primaryCopySet := range primaryCM.copySets {
            backupCopySet, exists := backupCM.copySets[pageID]
            if !exists {
                fmt.Printf("ERROR: Copy set for page %d exists in Primary but not in Backup\n", pageID)
                syncSuccess = false
                continue
            }
            
            for clientID := range primaryCopySet {
                if !backupCopySet[clientID] {
                    fmt.Printf("ERROR: Client %d in Primary's copy set for page %d but not in Backup's\n",
                        clientID, pageID)
                    syncSuccess = false
                }
            }
        }
        
        primaryCM.mu.RUnlock()
        backupCM.mu.RUnlock()
        
        if syncSuccess {
            fmt.Println("SUCCESS: Primary and Backup CMs are fully synchronized!")
        } else {
            fmt.Println("WARNING: Synchronization issues detected between Primary and Backup CMs")
        }
    }
}