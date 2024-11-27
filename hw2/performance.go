package main

import (
	"fmt"
	"os/exec"
	"strings"
	"time"
)

func main() {
	algorithms := []string{"ring", "lamport", "voting"}
	results := make(map[string]map[int]time.Duration)
	
	// Initialize results map
	for _, algo := range algorithms {
		results[algo] = make(map[int]time.Duration)
	}

	// Test each algorithm with node counts 1-10
	for _, algo := range algorithms {
		fmt.Printf("\nTesting %s algorithm...\n", algo)
		for nodes := 1; nodes <= 10; nodes++ {
			fmt.Printf("  Running with %d nodes...\n", nodes)
			
			// Run the program and capture its output
			cmd := exec.Command("go", "run", fmt.Sprintf("%s/%s.go", algo, algo), "-n", fmt.Sprintf("%d", nodes))
			output, err := cmd.CombinedOutput()
			if err != nil {
				fmt.Printf("Error running %s with %d nodes: %v\n", algo, nodes, err)
				continue
			}
			
			// Parse the execution time from output
			duration, err := parseExecutionTime(string(output), algo)
			if err != nil {
				fmt.Printf("Error parsing time for %s with %d nodes: %v\n", algo, nodes, err)
				continue
			}
			
			results[algo][nodes] = duration
			fmt.Printf("  Time: %v\n", duration)
			
			// Add delay between runs
			time.Sleep(1 * time.Second)
		}
	}

	// Print results in a formatted table
	printResultsTable(results)
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

func printResultsTable(results map[string]map[int]time.Duration) {
	fmt.Println("\nPerformance Results (in milliseconds):")
	fmt.Println("Nodes | Ring | Lamport | Voting")
	fmt.Println("------|-------|---------|--------")
	
	for nodes := 1; nodes <= 10; nodes++ {
		fmt.Printf("%5d |", nodes)
		for _, algo := range []string{"ring", "lamport", "voting"} {
			if duration, ok := results[algo][nodes]; ok {
				fmt.Printf("%8.2f |", float64(duration.Microseconds())/1000.0) // Convert to milliseconds
			} else {
				fmt.Printf("%8s |", "N/A")
			}
		}
		fmt.Println()
	}
}
