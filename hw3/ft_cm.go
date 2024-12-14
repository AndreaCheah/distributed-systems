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
        snapshots:     make(map[int]int),
    }
}

// SetupReplication establishes the connection between primary and backup CMs
func SetupReplication(primary, backup *PrimaryCentralManager) {
    primary.partner = backup
    backup.partner = primary
    
    // Copy initial state from primary to backup
    primary.mu.RLock()
    defer primary.mu.RUnlock()
    
    backup.mu.Lock()
    defer backup.mu.Unlock()
    
    // Deep copy pages
    for pageID, page := range primary.pages {
        page.mu.RLock()
        backup.pages[pageID] = &Page{
            ID:      page.ID,
            Data:    append([]byte(nil), page.Data...),
            Owner:   page.Owner,
            Version: page.Version,
        }
        page.mu.RUnlock()
    }
    
    // Copy page owners
    for pageID, owner := range primary.pageOwner {
        backup.pageOwner[pageID] = owner
    }
    
    // Deep copy copy sets
    for pageID, copySet := range primary.copySets {
        backup.copySets[pageID] = make(map[int]bool)
        for clientID := range copySet {
            backup.copySets[pageID][clientID] = true
        }
    }
    
    fmt.Println("Initial state synchronized between Primary and Backup CMs")
}

// Overridden methods to handle replication

func (bcm *PrimaryCentralManager) WritePage(pageID int, clientID int, data []byte) error {
    // First check if we're in failed state
    if bcm.failed.Load() {
        if bcm.partner != nil && bcm.partner.isActive {
            return bcm.partner.WritePage(pageID, clientID, data)
        }
        return fmt.Errorf("primary CM is failed and backup is not active")
    }

    if !bcm.isPrimary && !bcm.isActive {
        return fmt.Errorf("backup CM is not active")
    }

    // Gather invalidations first without holding the lock
    var invalidations []int
    bcm.mu.RLock()
    if copySet, exists := bcm.copySets[pageID]; exists {
        for cid := range copySet {
            if cid != clientID {
                invalidations = append(invalidations, cid)
            }
        }
    }
    bcm.mu.RUnlock()

    // Send invalidations
    for _, cid := range invalidations {
        if client, exists := bcm.clients[cid]; exists {
            client.invalidatePage(pageID)
        }
        
        // Send invalidation update to backup
        if bcm.isPrimary {
            invalidateUpdate := MetadataUpdate{
                Type:   "invalidate",
                PageID: pageID,
                Data: struct {
                    ClientID int
                    PageID  int
                }{cid, pageID},
                Timestamp: time.Now(),
            }
            bcm.replicateUpdate(invalidateUpdate)
        }
    }

    // Now take write lock for the actual update
    bcm.mu.Lock()
    
    // Update copy sets
    if _, exists := bcm.copySets[pageID]; !exists {
        bcm.copySets[pageID] = make(map[int]bool)
    }
    // Clear all copies except the writer
    bcm.copySets[pageID] = map[int]bool{clientID: true}

    // Update page data
    if page, exists := bcm.pages[pageID]; exists {
        page.mu.Lock()
        page.Data = append([]byte(nil), data...)
        page.Version++
        page.Owner = clientID
        version := page.Version
        page.mu.Unlock()

        // Create write update with current state
        writeUpdate := MetadataUpdate{
            Type:   "write",
            PageID: pageID,
            Data: struct {
                ClientID int
                Data    []byte
                Version int
                CopySet map[int]bool
            }{
                ClientID: clientID,
                Data:    data,
                Version: version,
                CopySet: bcm.copySets[pageID],
            },
            Timestamp: time.Now(),
        }
        bcm.mu.Unlock()

        // Send write update to backup
        if bcm.isPrimary {
            bcm.replicateUpdate(writeUpdate)
        }

        return nil
    }
    bcm.mu.Unlock()
    return fmt.Errorf("page %d not found", pageID)
}

func (bcm *PrimaryCentralManager) ReadPage(pageID int, clientID int) (*Page, error) {
    if !bcm.isPrimary && !bcm.isActive {
        return nil, fmt.Errorf("backup CM is not active")
    }

    // Execute read first
    page, err := bcm.CentralManager.ReadPage(pageID, clientID)
    if err != nil {
        return nil, err
    }

    // Update copysets for reads
    bcm.mu.Lock()
    if _, exists := bcm.copySets[pageID]; !exists {
        bcm.copySets[pageID] = make(map[int]bool)
    }
    
    if !bcm.copySets[pageID][clientID] {
        bcm.copySets[pageID][clientID] = true
        // Send copyset update
        update := MetadataUpdate{
            Type:   "copyset",
            PageID: pageID,
            Data:   CopySetUpdate{PageID: pageID, ClientID: clientID, Action: "add"},
            Timestamp: time.Now(),
        }
        if bcm.isPrimary {
            bcm.replicateUpdate(update)
        }
    }
    bcm.mu.Unlock()

    return page, nil
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

    fmt.Printf("Replicating update: Type=%s, PageID=%d\n", update.Type, update.PageID)
    
    err := bcm.partner.applyUpdate(update)
    if err != nil {
        fmt.Printf("Error replicating update: %v\n", err)
        return err
    }
    return nil
}

func (bcm *PrimaryCentralManager) applyUpdate(update MetadataUpdate) error {
    switch update.Type {
    case "write":
        bcm.mu.Lock()
        data := update.Data.(struct {
            ClientID int
            Data    []byte
            Version int
            CopySet map[int]bool
        })
        
        if _, exists := bcm.pages[update.PageID]; !exists {
            bcm.pages[update.PageID] = &Page{
                ID:      update.PageID,
                Data:    make([]byte, 0),
                Version: 0,
            }
        }
        
        page := bcm.pages[update.PageID]
        page.mu.Lock()
        page.Data = append([]byte(nil), data.Data...)
        page.Version = data.Version
        page.Owner = data.ClientID
        page.mu.Unlock()
        
        bcm.copySets[update.PageID] = make(map[int]bool)
        for clientID := range data.CopySet {
            bcm.copySets[update.PageID][clientID] = true
        }
        bcm.mu.Unlock()

    case "copyset":
        bcm.mu.Lock()
        data := update.Data.(struct {
            CopySet map[int]bool
            Source  string
        })
        if _, exists := bcm.copySets[update.PageID]; !exists {
            bcm.copySets[update.PageID] = make(map[int]bool)
        }
        for clientID := range data.CopySet {
            bcm.copySets[update.PageID][clientID] = true
        }
        bcm.mu.Unlock()

    case "ownership":
        bcm.mu.Lock()
        data := update.Data.(struct {
            Owner  int
            Source string
        })
        bcm.pageOwner[update.PageID] = data.Owner
        if page, exists := bcm.pages[update.PageID]; exists {
            page.mu.Lock()
            page.Owner = data.Owner
            page.mu.Unlock()
        }
        bcm.mu.Unlock()
    }

    return nil
}

func (bcm *PrimaryCentralManager) PromoteToActive() error {
    bcm.mu.Lock()
    if bcm.isPrimary {
        bcm.mu.Unlock()
        return fmt.Errorf("cannot promote primary CM")
    }

    bcm.isActive = true
    
    // Create a deep copy of all copy sets at time of promotion
    copySetSnapshot := make(map[int]map[int]bool)
    for pageID, copySet := range bcm.copySets {
        copySetSnapshot[pageID] = make(map[int]bool)
        for clientID := range copySet {
            copySetSnapshot[pageID][clientID] = true
        }
    }
    bcm.mu.Unlock()

    fmt.Printf("\n=== Backup CM Promoted to Active ===\n")
    
    // Reset operation counter
    bcm.opCounter.Store(0)

    // Synchronize copy sets after promotion
    bcm.mu.Lock()
    bcm.copySets = copySetSnapshot
    bcm.mu.Unlock()

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
                bcm.syncCopySets()     // Add copy set sync
                bcm.verifyState()      // Add state verification
            }
        }
    }()
}

func (bcm *PrimaryCentralManager) synchronizeMetadata() {
    bcm.mu.RLock()
    defer bcm.mu.RUnlock()

	// Sync version vectors first
	bcm.versionVector.mu.RLock()
	update := MetadataUpdate{
		Type: "version_vector",
		Data: bcm.versionVector.versions,
		Timestamp: time.Now(),
	}
	bcm.versionVector.mu.RUnlock()
	bcm.replicateUpdate(update)

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

func (bcm *PrimaryCentralManager) snapshotState() {
    // Save current versions of all pages
    bcm.snapshots = make(map[int]int)
    for pageID, page := range bcm.pages {
        page.mu.RLock()
        bcm.snapshots[pageID] = page.Version
        page.mu.RUnlock()
    }
}

func (bcm *PrimaryCentralManager) syncCopySets() {
    bcm.mu.RLock()
    defer bcm.mu.RUnlock()

    if !bcm.isActive || bcm.partner == nil {
        return
    }

    for pageID, copySet := range bcm.copySets {
        update := MetadataUpdate{
            Type:   "copyset",
            PageID: pageID,
            Data: struct {
                CopySet map[int]bool
                Source  string
            }{
                CopySet: copySet,
                Source:  bcm.roleString(),
            },
            Timestamp: time.Now(),
        }
        bcm.replicateUpdate(update)
    }

    // Also sync page ownership information
    for pageID, owner := range bcm.pageOwner {
        update := MetadataUpdate{
            Type:   "ownership",
            PageID: pageID,
            Data: struct {
                Owner  int
                Source string
            }{
                Owner:  owner,
                Source: bcm.roleString(),
            },
            Timestamp: time.Now(),
        }
        bcm.replicateUpdate(update)
    }
}

func (bcm *PrimaryCentralManager) verifyState() {
    bcm.mu.RLock()
    defer bcm.mu.RUnlock()

    if bcm.partner == nil {
        return
    }

    // Verify pages
    for pageID, page := range bcm.pages {
        page.mu.RLock()
        partnerPage, exists := bcm.partner.pages[pageID]
        if !exists {
            fmt.Printf("Warning: Page %d exists in %s but not in partner\n",
                pageID, bcm.roleString())
            page.mu.RUnlock()
            continue
        }

        partnerPage.mu.RLock()
        if page.Version != partnerPage.Version || 
           page.Owner != partnerPage.Owner {
            fmt.Printf("Warning: State mismatch for page %d:\n"+
                "  %s: Version=%d, Owner=%d\n"+
                "  Partner: Version=%d, Owner=%d\n",
                pageID, bcm.roleString(),
                page.Version, page.Owner,
                partnerPage.Version, partnerPage.Owner)
        }
        partnerPage.mu.RUnlock()
        page.mu.RUnlock()
    }

    // Verify copy sets
    bcm.verifyCopySets()
}

func (bcm *PrimaryCentralManager) roleString() string {
    if bcm.isPrimary {
        return "Primary"
    }
    return "Backup"
}

func (bcm *PrimaryCentralManager) verifyCopySets() {
    for pageID, copySet := range bcm.copySets {
        partnerCopySet, exists := bcm.partner.copySets[pageID]
        if !exists {
            fmt.Printf("Warning: Copy set for page %d exists in %s but not in partner\n",
                pageID, bcm.roleString())
            continue
        }

        // Compare copy sets
        for clientID := range copySet {
            if !partnerCopySet[clientID] {
                fmt.Printf("Warning: Client %d in %s copy set for page %d but not in partner's\n",
                    clientID, bcm.roleString(), pageID)
            }
        }
    }
}
