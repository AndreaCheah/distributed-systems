package main

import "fmt"

func NewFTCentralManager(baseCM *CentralManager, isBackup bool) *FTCentralManager {
	return &FTCentralManager{
		CentralManager: baseCM,
		isBackup:       isBackup,
		isAvailable:    true,
	}
}

func (cm *FTCentralManager) simulateFailure() {
	if !cm.isAvailable {
		return
	}
	cm.isAvailable = false
	cm.failureCount++
	if cm.isBackup {
		fmt.Printf("Backup CM failed (failure #%d)\n", cm.failureCount)
	} else {
		fmt.Printf("Primary CM failed (failure #%d)\n", cm.failureCount)
	}
}
