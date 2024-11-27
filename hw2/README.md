## Requirements
- **Lamport and Ring Protocols:** Require at least 2 nodes.
- **Voting Protocol:** Requires at least 3 nodes. 

Performance is evaluated for configurations with **3 to 12 nodes**.

## How to run the code

### Run Individual Protocols
To run a specific protocol, execute the following command from the **parent directory**:
```bash
go run <protocol>/<protocol>.go -n <number_of_nodes>
```
- Replace \<protocol> with `lamport`, `ring`, or `voting`.  
- Replace <number_of_nodes> with the desired number of nodes.  
- The number of nodes for `lamport` and `ring` must be at least `2`, and for `voting` must be at least `3`.

Example  
```bash
go run ring/ring.go -n 10
```

### Run Performance Evaluation
To view the performance of all three protocols in a table:
```bash
go run performance.go
```
- This script evaluates each protocol with 3 to 12 nodes and displays the results in a performance table.  
- If some time measurements are missed, the script will automatically rerun the test.
