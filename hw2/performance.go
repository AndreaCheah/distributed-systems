package main

import (
	"context"
	"flag"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const MAX_RETRIES = 10

func main() {
	startNodes := flag.Int("start", 10, "Starting number of nodes")
	skipNodes := flag.Int("skip", 1, "Number of nodes to skip between tests")
	endNodes := flag.Int("end", 19, "Ending number of nodes")
	flag.Parse()

	algorithms := []string{"ring", "lamport", "voting"}
	results := make(map[string]map[int]time.Duration)
	
	for _, algo := range algorithms {
		results[algo] = make(map[int]time.Duration)
	}

	for _, algo := range algorithms {
		fmt.Printf("\nTesting %s algorithm...\n", algo)
		for nodes := *startNodes; nodes <= *endNodes; nodes += *skipNodes {
			fmt.Printf("  Testing with %d nodes...\n", nodes)
			
			var duration time.Duration
			var success bool
			
			for attempt := 1; attempt <= MAX_RETRIES; attempt++ {
				if attempt > 1 {
					fmt.Printf("    Retry attempt %d/%d: ", attempt, MAX_RETRIES)
				} else {
					fmt.Printf("    First attempt: ")
				}
				
				// Create context with timeout that increases with node count
				timeout := time.Duration(nodes) * 2 * time.Second
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				cmd := exec.CommandContext(ctx, "go", "run", fmt.Sprintf("%s/%s.go", algo, algo), "-n", fmt.Sprintf("%d", nodes))
				output, err := cmd.CombinedOutput()
				cancel()

				if err != nil {
					if ctx.Err() == context.DeadlineExceeded {
						fmt.Printf("Timeout after %v\n", timeout)
					} else {
						fmt.Printf("Error running command: %v\n", err)
					}
					continue
				}
				
				duration, err = parseExecutionTime(string(output), algo)
				if err != nil {
					fmt.Printf("Error parsing time: %v\n", err)
					continue
				}
				
				if duration != 0 {
					fmt.Printf("%v\n", duration)
					success = true
					break
				} else {
					fmt.Printf("Invalid measurement\n")
				}
				
				// Increase sleep time between retries for larger node counts
				sleepTime := time.Duration(nodes) * 2 * time.Second
				time.Sleep(sleepTime)
			}
			
			if success {
				results[algo][nodes] = duration
			} else {
				fmt.Printf("    Failed to get valid measurement after %d attempts\n", MAX_RETRIES)
			}
		}
	}

	printResultsTable(results, *startNodes, *endNodes, *skipNodes)
}

func parseExecutionTime(output string, algo string) (time.Duration, error) {
	outputLines := strings.Split(output, "\n")
	
	switch algo {
	case "ring":
		for _, line := range outputLines {
			if strings.Contains(line, "nodes completed their critical sections in") {
				parts := strings.Split(line, "completed their critical sections in")
				if len(parts) > 1 {
					timeStr := strings.TrimSpace(parts[1])
					return time.ParseDuration(timeStr)
				}
			}
		}
	case "lamport":
		for _, line := range outputLines {
			if strings.Contains(line, "Total time from first request to completion:") {
				parts := strings.Split(line, "Total time from first request to completion:")
				if len(parts) > 1 {
					timeStr := strings.TrimSpace(parts[1])
					return time.ParseDuration(timeStr)
				}
			}
		}
	case "voting":
		for _, line := range outputLines {
			if strings.Contains(line, "Total execution time for") {
				parts := strings.Split(line, "Total execution time for")
				if len(parts) > 1 {
					timeParts := strings.Split(parts[1], ":")
					if len(timeParts) > 1 {
						timeStr := strings.TrimSpace(timeParts[1])
						return time.ParseDuration(timeStr)
					}
				}
			}
		}
	}
	
	return 0, fmt.Errorf("no execution time found in output")
}

func printResultsTable(results map[string]map[int]time.Duration, start, end, skip int) {
	fmt.Println("\nPerformance Results (in milliseconds):")
	fmt.Println("Nodes | Ring | Lamport | Voting")
	fmt.Println("------|-------|---------|--------")
	
	for nodes := start; nodes <= end; nodes += skip {
		fmt.Printf("%5d |", nodes)
		for _, algo := range []string{"ring", "lamport", "voting"} {
			if duration, ok := results[algo][nodes]; ok {
				fmt.Printf("%8.2f |", float64(duration.Microseconds())/1000.0)
			} else {
				fmt.Printf("%8s |", "N/A")
			}
		}
		fmt.Println()
	}
}