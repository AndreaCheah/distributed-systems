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
    CM    ManagerInterface
    pages map[int]*Page
    mu    sync.RWMutex
}

func NewClient(id int, manager ManagerInterface) *Client {
    return &Client{
        ID:    id,
        CM:    manager,
        pages: make(map[int]*Page),
    }
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

	// FT-specific fields
	primaryFailures int
	primaryRestarts int
	backupFailures  int
	backupRestarts  int
	failureInterval int
	restartDelay    int
}

// OperationResult stores the result of an operation
type OperationResult struct {
    operationID int
    duration    time.Duration
    err         error
    isWrite     bool
}

type BackupCentralManager struct {
    *CentralManager
    isPrimary bool
    isActive  bool
    partner   *BackupCentralManager
    mu        sync.RWMutex
}

type MetadataUpdate struct {
    Type      string      // "page", "owner", "copyset", "write"
    PageID    int
    Data      interface{} // The actual update data
    Timestamp time.Time
}

// ManagerInterface defines the common interface for both basic and FT modes
type ManagerInterface interface {
    ReadPage(pageID int, clientID int) (*Page, error)
    WritePage(pageID int, clientID int, data []byte) error
    RegisterClient(client *Client)
    initializePages(numPages int)
    GetCopySets() map[int]map[int]bool
    GetPages() map[int]*Page
    Lock()
    Unlock()
    RLock()
    RUnlock()
}

// CentralManager implements ManagerInterface
func (cm *CentralManager) GetCopySets() map[int]map[int]bool {
    return cm.copySets
}

func (cm *CentralManager) GetPages() map[int]*Page {
    return cm.pages
}

func (cm *CentralManager) Lock() {
    cm.mu.Lock()
}

func (cm *CentralManager) Unlock() {
    cm.mu.Unlock()
}

func (cm *CentralManager) RLock() {
    cm.mu.RLock()
}

func (cm *CentralManager) RUnlock() {
    cm.mu.RUnlock()
}

// Modify BackupCentralManager to implement ManagerInterface
func (bcm *BackupCentralManager) GetCopySets() map[int]map[int]bool {
    return bcm.CentralManager.GetCopySets()
}

func (bcm *BackupCentralManager) GetPages() map[int]*Page {
    return bcm.CentralManager.GetPages()
}

func (bcm *BackupCentralManager) Lock() {
    bcm.mu.Lock()
}

func (bcm *BackupCentralManager) Unlock() {
    bcm.mu.Unlock()
}

func (bcm *BackupCentralManager) RLock() {
    bcm.mu.RLock()
}

func (bcm *BackupCentralManager) RUnlock() {
    bcm.mu.RUnlock()
}

// Factory function to create appropriate manager based on mode
func CreateManager(config *Config) ManagerInterface {
    if config.mode == "ft" {
        // Create primary and backup CMs for fault-tolerant mode
        primaryCM := NewBackupCentralManager(true)
        backupCM := NewBackupCentralManager(false)
        SetupReplication(primaryCM, backupCM)
        
        // Start periodic sync
        primaryCM.startPeriodicSync(5 * time.Second)
        
        return primaryCM
    }
    
    // Return basic CentralManager for non-FT mode
    return NewCentralManager()
}
