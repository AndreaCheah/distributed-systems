package main

import (
    "fmt"
)

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

// CentralManager methods
func (cm *CentralManager) RegisterClient(client *Client) {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    cm.clients[client.ID] = client
}

func (cm *CentralManager) ReadPage(pageID int, clientID int) (*Page, error) {
    simulateNetworkLatency()
    
    cm.mu.RLock()
    page, exists := cm.pages[pageID]
    if !exists {
        cm.mu.RUnlock()
        return nil, fmt.Errorf("page %d not found", pageID)
    }
    
    if _, exists := cm.copySets[pageID]; !exists {
        cm.copySets[pageID] = make(map[int]bool)
    }
    cm.copySets[pageID][clientID] = true
    currentVersion := page.Version
    cm.mu.RUnlock()
    
    page.mu.RLock()
    defer page.mu.RUnlock()
    
    dataCopy := append([]byte(nil), page.Data...)
    
    return &Page{
        ID:      page.ID,
        Data:    dataCopy,
        Owner:   page.Owner,
        Version: currentVersion,
    }, nil
}

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
        go cm.processWriteRequest(pageID, req)
    }

    err := <-done

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

func (cm *CentralManager) processWriteRequest(pageID int, req WriteRequest) {
    cm.mu.Lock()
    if len(cm.writeQueue[pageID]) == 0 || cm.writeQueue[pageID][0].done != req.done {
        cm.mu.Unlock()
        req.done <- fmt.Errorf("write request out of order")
        return
    }
    cm.mu.Unlock()

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

    if page.Owner != req.clientID {
        fmt.Printf("CM: Transferring ownership of page %d from client %d to client %d\n",
            pageID, page.Owner, req.clientID)
        cm.pageOwner[pageID] = req.clientID
        page.Owner = req.clientID
    }
    cm.mu.Unlock()

    page.mu.Lock()
    page.Data = append([]byte(nil), req.data...)
    page.Version++
    fmt.Printf("CM: Page %d updated to version %d by client %d\n", 
        pageID, page.Version, req.clientID)
    page.mu.Unlock()

    req.done <- nil
}

func (cm *CentralManager) initializePages(numPages int) {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    for i := 0; i < numPages; i++ {
        cm.pages[i] = &Page{
            ID:      i,
            Data:    []byte(fmt.Sprintf("initial-data-%d", i)),
            Owner:   0,
            Version: 0,
        }
        cm.pageOwner[i] = 0
    }
}
