package main

import (
    "fmt"
)

// NewClient creates a new client instance
func NewClient(id int, cm *CentralManager) *Client {
    return &Client{
        ID:    id,
        CM:    cm,
        pages: make(map[int]*Page),
    }
}

// Client methods
func (c *Client) getLocalPage(pageID int) (*Page, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    
    page, exists := c.pages[pageID]
    if !exists {
        return nil, false
    }
    
    c.CM.mu.RLock()
    cmPage, exists := c.CM.pages[pageID]
    c.CM.mu.RUnlock()
    
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
