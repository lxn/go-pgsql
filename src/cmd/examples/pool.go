package main

import (
	"fmt"
	"log"
	"sync"
	"pgsql"
)

func main() {
	// Create a connection pool with up to 3 connections, automatically closing
	// idle connections after the default timeout period (5 minutes).
	pool, err := pgsql.NewPool("dbname=postgres user=postgres", 3, 3, pgsql.DEFAULT_IDLE_TIMEOUT)
	if err != nil {
		log.Fatalf("Error opening connection pool: %s\n", err)
	}
	pool.Debug = true

	// Create 10 worker goroutines each of which acquires and uses a
	// connection from the pool.
	var wg sync.WaitGroup
	nthreads := 10
	wg.Add(nthreads)
	for i := 0; i < nthreads; i++ {
		go worker(i+1, pool, &wg)
	}
	wg.Wait()    // Wait for all the workers to finish.
	pool.Close() // Close all pool connections.
}

func worker(id int, pool *pgsql.Pool, wg *sync.WaitGroup) {
	conn, err := pool.Acquire()
	if err != nil {
		log.Printf("Error acquiring connection: %s\n", err)
	} else {
		res, err := conn.Query("SELECT now();")
		if err != nil {
			log.Printf("Error executing query: %s\n", err)
		} else {
			if hasRow, _ := res.FetchNext(); !hasRow {
				log.Println("Couldn't advance result cursor")
			} else {
				var now string
				if err := res.Scan(&now); err != nil {
					log.Printf("Error scanning result: %s\n", err)
				} else {
					fmt.Printf("Timestamp returned for worker %d: %s\n", id, now)
				}
			}
		}
	}
	// Return the connection back to the pool.
	pool.Release(conn)
	wg.Done()
}
