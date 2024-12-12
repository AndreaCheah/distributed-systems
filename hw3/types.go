package main

import (
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
    pages      map[int]*Page              // Map of page ID to page metadata
    pageOwner  map[int]int               // Map of page ID to current owner
    copySets   map[int]map[int]bool      // pageID -> set of clientIDs that have copies
    clients    map[int]*Client
    writeQueue map[int][]WriteRequest     // pageID -> queue of write requests
    mu         sync.RWMutex
}

// Client represents a node in the system
type Client struct {
    ID    int
    CM    *CentralManager
    pages map[int]*Page
    mu    sync.RWMutex
}

// WriteRequest represents a pending write operation
type WriteRequest struct {
    clientID int
    data     []byte
    done     chan error
}

// Operation represents a single read or write operation
type Operation struct {
    isWrite  bool
    pageID   int
    clientID int
    data     []byte    // only used for writes
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

// OperationMetrics stores timing information for operations
type OperationMetrics struct {
    startTime time.Time
    endTime   time.Time
    opType    string
    pageID    int
    clientID  int
}

// OperationResult stores the result of an operation
type OperationResult struct {
    operationID int
    duration    time.Duration
    err         error
    isWrite     bool
}

func (m *OperationMetrics) duration() time.Duration {
    return m.endTime.Sub(m.startTime)
}