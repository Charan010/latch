package main

import (
	"fmt"
	"time"

	"github.com/Charan010/latch/lease"
)

func main() {
	go lease.ExpiryWorker()

	lockID := "resource-1"

	fmt.Println("A taking lock")
	tokenA, ok := lease.AcquireLock(lockID, "client-A", 3*time.Second)
	if !ok {
		panic("client-A failed to acquire lock")
	}
	fmt.Println("A succesfully took the lock ", tokenA)

	fmt.Println("B trying to pick up the lock")
	_, ok = lease.AcquireLock(lockID, "client-B", 3*time.Second)
	if !ok {
		fmt.Println(" Bfailed to acquire lock")
	} else {
		panic("client-B should NOT have acquired the lock")
	}

	fmt.Println(" A holding lock, not releasing")

	time.Sleep(4 * time.Second)

	fmt.Println("B trying again after expiry")
	tokenB, ok := lease.AcquireLock(lockID, "client-B", 3*time.Second)
	if !ok {
		panic("client-B should have acquired lock after expiry")
	}
	fmt.Println("B acquired lock with token", tokenB)

	fmt.Println(" A trying to release with stale token")
	ok = lease.ReleaseLock(lockID, "client-A", tokenA)
	if !ok {
		fmt.Println(" A release failed (stale token, correct behavior)")
	} else {
		panic("stale release should not succeed tho.")
	}

	time.Sleep(2 * time.Second)
}
