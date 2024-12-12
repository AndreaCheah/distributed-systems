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
    
    flag.StringVar(&config.mode, "mode", "basic", "Operation mode (basic/fault-tolerant)")
    flag.IntVar(&config.clients, "clients", 10, "Number of clients")
    flag.StringVar(&config.workload, "workload", "random", "Workload type (random/read-intensive/write-intensive)")
    flag.StringVar(&config.faults, "faults", "none", "Fault injection mode (none/single/multiple/primary-and-backup)")
    flag.StringVar(&config.scenario, "scenario", "", "Simulation scenario")
    flag.Float64Var(&config.writeFraction, "write-fraction", -1.0, "Fraction of write operations (0.0-1.0)")
    flag.IntVar(&config.numOperations, "operations", 10, "Number of operations to perform")
    
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
    } else {
        if config.writeFraction < 0.0 || config.writeFraction > 1.0 {
            log.Fatal("Write fraction must be between 0.0 and 1.0")
        }
        
        if config.workload == "read-intensive" && config.writeFraction > 0.5 {
            log.Fatal("Read-intensive workload cannot have write fraction > 0.5")
        }
        if config.workload == "write-intensive" && config.writeFraction < 0.5 {
            log.Fatal("Write-intensive workload cannot have write fraction < 0.5")
        }
    }

    return config
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

func ExecuteOperationsConcurrently(clients []*Client, operations []Operation) []OperationResult {
    results := make([]OperationResult, len(operations))
    var wg sync.WaitGroup
    resultsChan := make(chan OperationResult, len(operations))

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

    go func() {
        wg.Wait()
        close(resultsChan)
    }()

    for result := range resultsChan {
        results[result.operationID] = result
    }

    return results
}

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
    
    cm := NewCentralManager()
    numPages := 5
    cm.initializePages(numPages)
    
    clients := make([]*Client, config.clients)
    for i := 0; i < config.clients; i++ {
        clients[i] = NewClient(i, cm)
        cm.RegisterClient(clients[i])
    }
    
    testCacheBehavior(clients)
    
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
    
    results := ExecuteOperationsConcurrently(clients, operations)
    
    totalTime := time.Since(start)
    
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