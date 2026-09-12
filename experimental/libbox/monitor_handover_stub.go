//go:build !darwin || !with_wlt

package libbox

func beginWLTEpoch(*platformDefaultInterfaceMonitor) uint64 { return 0 }

func wltEpochCurrent(*platformDefaultInterfaceMonitor, uint64) bool { return true }

func prepareWLTEarlyHandover(*platformDefaultInterfaceMonitor, string, int32, uint64) {}
