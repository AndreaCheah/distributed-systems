package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const RUNS_PER_TEST = 3 // Number of runs for each configuration

func main() {
	algorithms := []string{"ring", "lamport", "voting"}
	results := make(map[string]map[int][]time.Duration)
	
	// Initialize results map
	for _, algo := range algorithms {
		results[algo] = make(map[int][]time.Duration)
	}

	// Test each algorithm with node counts 3-12
	for _, algo := range algorithms {
		fmt.Printf("\nTesting %s algorithm...\n", algo)
		for nodes := 3; nodes <= 12; nodes++ {
			if algo == "voting" && nodes > 10 {
				fmt.Printf("  Skipping %d nodes for voting (not supported)\n", nodes)
				continue
			}
			
			fmt.Printf("  Testing with %d nodes...\n", nodes)
			var validRuns []time.Duration
			
			// Multiple runs for each configuration
			for run := 1; run <= RUNS_PER_TEST; run++ {
				fmt.Printf("    Run %d/%d: ", run, RUNS_PER_TEST)
				
				cmd := exec.Command("go", "run", fmt.Sprintf("%s/%s.go", algo, algo), "-n", fmt.Sprintf("%d", nodes))
				output, err := cmd.CombinedOutput()
				if err != nil {
					fmt.Printf("Error: %v\n", err)
					continue
				}
				
				duration, err := parseExecutionTime(string(output), algo)
				if err != nil {
					fmt.Printf("Error parsing time: %v\n", err)
					continue
				}
				
				if duration != 0 {
					validRuns = append(validRuns, duration)
					fmt.Printf("%v\n", duration)
				} else {
					fmt.Printf("Invalid measurement\n")
				}
				
				time.Sleep(1 * time.Second)
			}
			
			if len(validRuns) > 0 {
				results[algo][nodes] = validRuns
			}
		}
	}

	// Print results
	printResultsTable(results)
	saveResultsToCSV(results)
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

func calculateAverage(durations []time.Duration) float64 {
	if len(durations) == 0 {
		return 0
	}
	
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	return float64(sum.Microseconds()) / float64(len(durations)) / 1000.0 // Convert to milliseconds
}

func printResultsTable(results map[string]map[int][]time.Duration) {
	fmt.Println("\nPerformance Results (average time in milliseconds):")
	fmt.Println("Nodes | Ring | Lamport | Voting")
	fmt.Println("------|-------|---------|--------")
	
	for nodes := 3; nodes <= 12; nodes++ {
		fmt.Printf("%5d |", nodes)
		for _, algo := range []string{"ring", "lamport", "voting"} {
			if durations, ok := results[algo][nodes]; ok && len(durations) > 0 {
				avg := calculateAverage(durations)
				fmt.Printf("%8.2f |", avg)
			} else {
				fmt.Printf("%8s |", "N/A")
			}
		}
		fmt.Println()
	}
}

func saveResultsToCSV(results map[string]map[int][]time.Duration) {
	file, err := os.Create("performance_results.csv")
	if err != nil {
		fmt.Printf("Error creating CSV file: %v\n", err)
		return
	}
	defer file.Close()

	// Write header
	file.WriteString("Nodes,Ring,Lamport,Voting\n")

	// Write data
	for nodes := 3; nodes <= 12; nodes++ {
		row := fmt.Sprintf("%d", nodes)
		for _, algo := range []string{"ring", "lamport", "voting"} {
			if durations, ok := results[algo][nodes]; ok && len(durations) > 0 {
				avg := calculateAverage(durations)
				row += fmt.Sprintf(",%.2f", avg)
			} else {
				row += ",N/A"
			}
		}
		file.WriteString(row + "\n")
	}

	fmt.Println("\nResults have been saved to performance_results.csv")
}