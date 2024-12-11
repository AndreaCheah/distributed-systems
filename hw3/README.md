## Part 1
Basic Ivy architecture without fault tolerance requires:  
- A Central Manager (CM) to manage read and write requests for pages.  
- Nodes that can request read and write access to pages.  
- Mechanisms to track:  
    - Page ownership (who owns a page).  
    - Read copies (who has read-only copies of a page).  

