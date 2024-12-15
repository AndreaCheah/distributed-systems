## Answering the written questions
### Part 3  

Sequential consistency requires that the results of all operations appear as if they were executed in some sequential order that respects the program order of each individual process, and the fault-tolerant version of Ivy does not strictly guarantee sequential consistency due to:  
- During a failure, the backup CM might not have the latest state from the primary CM, leading to stale reads  
- During a failure, the backup CM takes over as the active CM. However, synchronization lags or dropped updates could result in missed writes being visible in subsequent operations.
- During a failure, clients relying on invalidations might operate on stale data if invalidations are delayed or lost.  

### Experiments
#### Experiment 1
```
go run . --mode basic --clients 10 --workload random

go run . --mode ft --clients 10 --workload random
```
After running those, the time taken for 10 random operations for 10 clients for basic and fault-tolerant versions are 8.86 ms and 5.65 ms respectively.

#### Experiment 2
```
go run . --mode basic --clients 10 --workload read-intensive --write-fraction 0.1

go run . --mode ft --clients 10 --workload read-intensive --write-fraction 0.1
```
After running those, the time taken for 10 read-intensive operations for 10 clients for basic and fault-tolerant versions are 6.12 ms and 5.92 ms respectively.

```
go run . --mode basic --clients 10 --workload write-intensive --write-fraction 0.9

go run . --mode ft --clients 10 --workload write-intensive --write-fraction 0.9 
```
After running those, the time taken for 10 write-intensive operations for 10 clients for basic and fault-tolerant versions are 9.30 ms and 6.52 ms respectively.

#### Other Experiments
Basic Ivy is attempted. Though fault-tolerant Ivy is also attempted, it is not complete.  
- When primary central manager fails, there might be synchronisation issues with the backup central manager. 
- There is some attempt on primary failure simulation and restart, but the protocol is not complete.
- No attempt on backup failure and restart.

## How to run the code
You can modify the mode, number of clients, workload type, write-fraction, and number of operations with command-line arguments.  
Example commands for basic Ivy:
```
go run . --mode basic --clients 10 --workload random

go run . --mode basic --clients 10 --workload write-intensive --write-fraction 0.75 --operations 50

go run . --mode basic --clients 10 --workload read-intensive --write-fraction 0.05 --operations 100  
```
Example commands for fault-tolerant Ivy:
```
go run . --mode ft --clients 10 --workload write-intensive --operations 10

go run . --mode ft --clients 10 --workload write-intensive --operations 10 --primary-failures 1 --primary-restarts 1
```
