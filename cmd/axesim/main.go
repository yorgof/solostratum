// axesim is a software miner that behaves like a Bitaxe on the wire. It is
// meant for trying out a solostratum setup without mining hardware:
//
//	go run ./cmd/axesim -addr 127.0.0.1:3333 -user test
//
// It hashes on the CPU, so it is millions of times slower than an ASIC. Set
// "min_difficulty" low (for example 0.0005) in solostratum.conf to see
// shares arrive every few seconds.
package main

import (
	"flag"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yorgof/solostratum/internal/axesim"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:3333", "solostratum address (host:port)")
	user := flag.String("user", "axesim", "Stratum username: a worker name, or address.workername")
	suggest := flag.Uint("suggest", 0, "difficulty to suggest, as a Bitaxe does (0 = none)")
	threads := flag.Int("threads", 1, "number of simulated devices to run")
	duration := flag.Duration("duration", 0, "stop after this long (0 = run until interrupted)")
	flag.Parse()

	var accepted, rejected, hashes atomic.Uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := mine(*addr, *user, uint32(*suggest), stop, &accepted, &rejected, &hashes); err != nil {
					log.Printf("connection lost: %v; reconnecting in 5s", err)
					select {
					case <-stop:
						return
					case <-time.After(5 * time.Second):
					}
				}
			}
		}()
	}

	started := time.Now()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	var deadline <-chan time.Time
	if *duration > 0 {
		deadline = time.After(*duration)
	}
	for {
		select {
		case <-ticker.C:
			log.Printf("shares: %d accepted, %d rejected; %.2f MH/s", accepted.Load(), rejected.Load(),
				float64(hashes.Load())/time.Since(started).Seconds()/1e6)
		case <-deadline:
			close(stop)
			wg.Wait()
			log.Printf("done: %d accepted, %d rejected", accepted.Load(), rejected.Load())
			return
		}
	}
}

func mine(addr, user string, suggest uint32, stop chan struct{}, accepted, rejected, hashes *atomic.Uint64) error {
	m, err := axesim.Dial(addr, axesim.Options{User: user, Password: "x", SuggestDifficulty: suggest, ExtranonceSub: true})
	if err != nil {
		return err
	}
	defer m.Close()
	log.Printf("connected to %s as %q, version mask %08x", addr, user, m.VersionMask())

	const chunk = 100_000
	for {
		select {
		case <-stop:
			return nil
		default:
		}
		job, err := m.WaitJob(0, 2*time.Minute, nil)
		if err != nil {
			return err
		}
		share, found := m.Mine(job, m.Difficulty(), chunk)
		hashes.Add(chunk)
		if !found {
			continue
		}
		res, err := m.Submit(share)
		if err != nil {
			return err
		}
		if res.Accepted {
			accepted.Add(1)
			if d := axesim.HashDifficulty(share.Hash); d >= job.NetworkDifficulty() {
				log.Printf("BLOCK FOUND: %s", axesim.DisplayHash(share.Hash))
			}
		} else {
			rejected.Add(1)
			log.Printf("share rejected: %d %s", res.Code, res.Message)
		}
	}
}
