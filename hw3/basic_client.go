package main

import (
    "fmt"
)

// Operation execution methods
func (client *Client) ExecuteOperation(op Operation) error {
    if op.isWrite {
        return client.CM.WritePage(op.pageID, client.ID, op.data)
    }
    
    if localPage, exists := client.getLocalPage(op.pageID); exists {
        fmt.Printf("Client %d: Cache hit for page %d (version %d)\n", 
            client.ID, op.pageID, localPage.Version)
        return nil
    }
    
    fmt.Printf("Client %d: Cache miss for page %d, fetching from CM\n", 
        client.ID, op.pageID)
    
    page, err := client.CM.ReadPage(op.pageID, client.ID)
    if err != nil {
        return err
    }
    
    client.setLocalPage(page)
    
    // Use interface methods for copy set management
    client.CM.Lock()
    copySets := client.CM.GetCopySets()
    if _, exists := copySets[op.pageID]; !exists {
        copySets[op.pageID] = make(map[int]bool)
    }
    copySets[op.pageID][client.ID] = true
    client.CM.Unlock()
    
    fmt.Printf("Client %d: Cached page %d (version %d)\n", 
        client.ID, op.pageID, page.Version)
    
    return nil
}

func (c *Client) getLocalPage(pageID int) (*Page, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    
    page, exists := c.pages[pageID]
    if !exists {
        return nil, false
    }
    
    c.CM.RLock()
    pages := c.CM.GetPages()
    cmPage, exists := pages[pageID]
    c.CM.RUnlock()
    
    if !exists || cmPage.Version > page.Version {
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
