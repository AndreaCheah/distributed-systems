package main

import (
    "fmt"
    "time"
)

// NewPrimaryCentralManager creates a new backup CM instance
func NewPrimaryCentralManager(isPrimary bool) *PrimaryCentralManager {
    return &PrimaryCentralManager{
        CentralManager: NewCentralManager(),
        isPrimary:     isPrimary,
        isActive:      isPrimary, // Primary starts active, backup starts inactive
    }
}

// SetupReplication establishes the connection between primary and backup CMs
func SetupReplication(primary, backup *PrimaryCentralManager) {
    primary.partner = backup
    backup.partner = primary
}

// Overridden methods to handle replication

func (bcm *PrimaryCentralManager) WritePage(pageID int, clientID int, data []byte) error {
    if !bcm.isPrimary && !bcm.isActive {
        return fmt.Errorf("backup CM is not active")
    }

    // Create metadata update for replication
    update := MetadataUpdate{
        Type:      "write",
        PageID:    pageID,
        Data: struct {
            ClientID int
            Data    []byte
        }{clientID, data},
        Timestamp: time.Now(),
    }

    // If primary, replicate to backup before processing
    if bcm.isPrimary {
        bcm.replicateUpdate(update)
    }

    return bcm.CentralManager.WritePage(pageID, clientID, data)
}

func (bcm *PrimaryCentralManager) ReadPage(pageID int, clientID int) (*Page, error) {
    if !bcm.isPrimary && !bcm.isActive {
        return nil, fmt.Errorf("backup CM is not active")
    }

    // Create metadata update for replication
    update := MetadataUpdate{
        Type:   "copyset",
        PageID: pageID,
        Data: struct {
            ClientID int
            Action  string
        }{clientID, "add"},
        Timestamp: time.Now(),
    }

    // If primary, replicate to backup
    if bcm.isPrimary {
        bcm.replicateUpdate(update)
    }

    return bcm.CentralManager.ReadPage(pageID, clientID)
}

func (bcm *PrimaryCentralManager) RegisterClient(client *Client) {
    bcm.mu.Lock()
    defer bcm.mu.Unlock()

    bcm.CentralManager.RegisterClient(client)

    if bcm.isPrimary {
        // Replicate client registration to backup
        update := MetadataUpdate{
            Type: "client",
            Data: client.ID,
            Timestamp: time.Now(),
        }
        bcm.replicateUpdate(update)
    }
}

// Replication helpers

func (bcm *PrimaryCentralManager) replicateUpdate(update MetadataUpdate) error {
    if bcm.partner == nil {
        return fmt.Errorf("no backup CM configured")
    }

    return bcm.partner.applyUpdate(update)
}

func (bcm *PrimaryCentralManager) applyUpdate(update MetadataUpdate) error {
    bcm.mu.Lock()
    defer bcm.mu.Unlock()

    switch update.Type {
    case "write":
        data := update.Data.(struct {
            ClientID int
            Data    []byte
        })
        // Update local page data
        if page, exists := bcm.pages[update.PageID]; exists {
            page.mu.Lock()
            page.Data = append([]byte(nil), data.Data...)
            page.Version++
            page.Owner = data.ClientID
            page.mu.Unlock()
        }

    case "copyset":
        data := update.Data.(struct {
            ClientID int
            Action  string
        })
        if _, exists := bcm.copySets[update.PageID]; !exists {
            bcm.copySets[update.PageID] = make(map[int]bool)
        }
        if data.Action == "add" {
            bcm.copySets[update.PageID][data.ClientID] = true
        } else {
            delete(bcm.copySets[update.PageID], data.ClientID)
        }

    case "owner":
        newOwner := update.Data.(int)
        bcm.pageOwner[update.PageID] = newOwner
        if page, exists := bcm.pages[update.PageID]; exists {
            page.Owner = newOwner
        }

    case "client":
        clientID := update.Data.(int)
        if client, exists := bcm.clients[clientID]; exists {
            bcm.clients[clientID] = client
        }
    }

    return nil
}

// Failover support

func (bcm *PrimaryCentralManager) PromoteToActive() error {
    bcm.mu.Lock()
    defer bcm.mu.Unlock()

    if bcm.isPrimary {
        return fmt.Errorf("cannot promote primary CM")
    }

    bcm.isActive = true
    fmt.Printf("Backup CM promoted to active\n")
    return nil
}

func (bcm *PrimaryCentralManager) Deactivate() error {
    bcm.mu.Lock()
    defer bcm.mu.Unlock()

    if bcm.isPrimary {
        return fmt.Errorf("cannot deactivate primary CM")
    }

    bcm.isActive = false
    fmt.Printf("Backup CM deactivated\n")
    return nil
}

// Periodic synchronization
func (bcm *PrimaryCentralManager) startPeriodicSync(interval time.Duration) {
    if !bcm.isPrimary {
        return // Only primary should initiate sync
    }

    go func() {
        ticker := time.NewTicker(interval)
        defer ticker.Stop()
        
        for {
            select {
            case <-ticker.C:
                bcm.synchronizeMetadata()
            }
        }
    }()
}

func (bcm *PrimaryCentralManager) synchronizeMetadata() {
    bcm.mu.RLock()
    defer bcm.mu.RUnlock()

    if bcm.partner == nil {
        return
    }

    // Sync pages
    for pageID, page := range bcm.pages {
        page.mu.RLock()
        update := MetadataUpdate{
            Type:   "page",
            PageID: pageID,
            Data: struct {
                Data    []byte
                Owner   int
                Version int
            }{
                Data:    append([]byte(nil), page.Data...),
                Owner:   page.Owner,
                Version: page.Version,
            },
            Timestamp: time.Now(),
        }
        page.mu.RUnlock()
        bcm.replicateUpdate(update)
    }

    // Sync copy sets
    for pageID, copySet := range bcm.copySets {
        update := MetadataUpdate{
            Type:      "copyset",
            PageID:    pageID,
            Data:      copySet,
            Timestamp: time.Now(),
        }
        bcm.replicateUpdate(update)
    }
}

// Initialize pages for both primary and backup
func (bcm *PrimaryCentralManager) initializePages(numPages int) {
    bcm.mu.Lock()
    defer bcm.mu.Unlock()
    
    bcm.CentralManager.initializePages(numPages)
    
    // If primary, replicate initial state to backup
    if bcm.isPrimary && bcm.partner != nil {
        for i := 0; i < numPages; i++ {
            update := MetadataUpdate{
                Type:   "page",
                PageID: i,
                Data: struct {
                    Data    []byte
                    Owner   int
                    Version int
                }{
                    Data:    []byte(fmt.Sprintf("initial-data-%d", i)),
                    Owner:   0,
                    Version: 0,
                },
                Timestamp: time.Now(),
            }
            bcm.replicateUpdate(update)
        }
    }
}