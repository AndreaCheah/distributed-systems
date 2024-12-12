// ft_cm.go

package main

import (
    "fmt"
    "sync"
    "time"
)

// CMRole represents the role of a central manager
type CMRole int

const (
    RolePrimary CMRole = iota
    RoleBackup
    RoleFailed
)

func (r CMRole) String() string {
    switch r {
    case RolePrimary:
        return "Primary"
    case RoleBackup:
        return "Backup"
    case RoleFailed:
        return "Failed"
    default:
        return "Unknown"
    }
}

// ReplicatedCentralManager extends CentralManager with fault tolerance
type ReplicatedCentralManager struct {
    *CentralManager
    role      CMRole
    backup    *ReplicatedCentralManager
    stateMu   sync.RWMutex
    heartbeat chan bool
    id        string
    
    // Synchronization tracking
    pageVersions    map[int]int
    syncMu          sync.RWMutex
    operationLog    []Operation
    lastSyncedIndex int
}

// NewReplicatedCentralManager creates a new replicated central manager
func NewReplicatedCentralManager(isPrimary bool) *ReplicatedCentralManager {
    role := RoleBackup
    id := "Backup"
    if isPrimary {
        role = RolePrimary
        id = "Primary"
    }
    
    rcm := &ReplicatedCentralManager{
        CentralManager:   NewCentralManager(),
        role:           role,
        heartbeat:       make(chan bool),
        id:             id,
        pageVersions:    make(map[int]int),
        operationLog:    make([]Operation, 0),
        lastSyncedIndex: -1,
    }
    
    fmt.Printf("\n[%s CM] Initialized in %s state\n", rcm.id, rcm.role)
    return rcm
}

// SetupReplication establishes primary-backup relationship
func SetupReplication(primary, backup *ReplicatedCentralManager) {
    primary.backup = backup
    backup.backup = primary
    fmt.Printf("[System] Replication configured between Primary and Backup CMs\n")
    
    // Start heartbeat monitoring if this is the backup
    if backup.role == RoleBackup {
        go backup.monitorPrimary()
    }
}

func (rcm *ReplicatedCentralManager) monitorPrimary() {
    ticker := time.NewTicker(2 * time.Second)
    defer ticker.Stop()
    
    missedHeartbeats := 0
    maxMissed := 3
    
    for range ticker.C {
        select {
        case <-rcm.heartbeat:
            fmt.Printf("[%s CM] Received heartbeat from primary\n", rcm.id)
            missedHeartbeats = 0
        default:
            missedHeartbeats++
            fmt.Printf("[%s CM] Missed heartbeat #%d\n", rcm.id, missedHeartbeats)
            
            if missedHeartbeats >= maxMissed {
                rcm.handlePrimaryFailure()
                return
            }
        }
    }
}

func (rcm *ReplicatedCentralManager) handlePrimaryFailure() {
    rcm.stateMu.Lock()
    if rcm.role != RoleBackup {
        rcm.stateMu.Unlock()
        return
    }
    
    fmt.Printf("\n[%s CM] Primary failure detected, promoting to Primary\n", rcm.id)
    rcm.role = RolePrimary
    rcm.id = "Primary (was Backup)"
    rcm.stateMu.Unlock()
    
    rcm.mu.Lock()
    for clientID, client := range rcm.clients {
        client.CM = rcm.CentralManager
        fmt.Printf("[%s CM] Redirected client %d to new primary\n", rcm.id, clientID)
    }
    rcm.mu.Unlock()
}

// func (rcm *ReplicatedCentralManager) simulateFailure() {
//     rcm.stateMu.Lock()
//     if rcm.role == RolePrimary {
//         fmt.Printf("\n[%s CM] Simulating primary failure...\n", rcm.id)
//         rcm.role = RoleFailed
//         if rcm.backup != nil {
//             go rcm.backup.handlePrimaryFailure()
//         }
//     }
//     rcm.stateMu.Unlock()
// }

func (rcm *ReplicatedCentralManager) WritePage(pageID int, clientID int, data []byte) error {
    rcm.stateMu.RLock()
    currentRole := rcm.role
    rcm.stateMu.RUnlock()
    
    if currentRole == RoleFailed {
        if rcm.backup != nil && rcm.backup.role == RolePrimary {
            fmt.Printf("[%s CM] Redirecting write to backup CM\n", rcm.id)
            return rcm.backup.WritePage(pageID, clientID, data)
        }
        return fmt.Errorf("no available central manager")
    }
    
    err := rcm.CentralManager.WritePage(pageID, clientID, data)
    if err == nil {
        rcm.logOperation(Operation{
            isWrite:  true,
            pageID:   pageID,
            clientID: clientID,
            data:     data,
        })
        
        if currentRole == RolePrimary {
            err = rcm.syncWithBackup()
            if err != nil {
                fmt.Printf("[%s CM] Failed to sync with backup: %v\n", rcm.id, err)
            }
        }
    }
    return err
}

func (rcm *ReplicatedCentralManager) logOperation(op Operation) {
    rcm.syncMu.Lock()
    defer rcm.syncMu.Unlock()
    
    rcm.operationLog = append(rcm.operationLog, op)
    fmt.Printf("[%s CM] Logged operation: %s on page %d by client %d\n",
        rcm.id,
        map[bool]string{true: "WRITE", false: "READ"}[op.isWrite],
        op.pageID,
        op.clientID)
}

func (rcm *ReplicatedCentralManager) syncWithBackup() error {
    if rcm.backup == nil {
        return nil
    }
    
    rcm.syncMu.Lock()
    defer rcm.syncMu.Unlock()
    
    if len(rcm.operationLog) == 0 || rcm.lastSyncedIndex >= len(rcm.operationLog)-1 {
        return nil
    }
    
    // Sync new operations
    for i := rcm.lastSyncedIndex + 1; i < len(rcm.operationLog); i++ {
        op := rcm.operationLog[i]
        if op.isWrite {
            fmt.Printf("[%s CM] Syncing write operation for page %d to backup\n",
                rcm.id, op.pageID)
            
            rcm.backup.CentralManager.mu.Lock()
            if page, exists := rcm.backup.CentralManager.pages[op.pageID]; exists {
                page.Data = append([]byte(nil), op.data...)
                page.Version++
                fmt.Printf("[%s CM] Synced page %d (version %d) to backup\n",
                    rcm.id, op.pageID, page.Version)
            }
            rcm.backup.CentralManager.mu.Unlock()
        }
    }
    
    rcm.lastSyncedIndex = len(rcm.operationLog) - 1
    return nil
}
