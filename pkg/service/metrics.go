package service

import (
	"runtime"
	"syscall"
)

func getMemoryUsage() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.Alloc)
}

func getCPUUsage() float64 {
	// TODO: Implement CPU usage tracking
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		return 0
	}

	// Convert to percentage
	return float64(rusage.Utime.Sec+rusage.Stime.Sec) / 100.0
}
