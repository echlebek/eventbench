package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	sensu "github.com/sensu/sensu-go/api/core/v2"

	_ "github.com/lib/pq"
)

var (
	concurrency = flag.Int("c", 10, "max number of read/write goroutines")
	numEvents   = flag.Int("n", 1000, "total number of events")
	pgURL       = flag.String("d", "host=/run/postgresql sslmode=disable", "postgresql url")

	atomicCounter int64
)

const ddl = `CREATE TABLE IF NOT EXISTS events (
  ns        text,
  entity    text,
  checkname text,
  data      jsonb,
  PRIMARY KEY (ns, entity, checkname)
);`

const readEventQuery = `SELECT data FROM events
WHERE ns = $1 AND entity = $2 AND checkname = $3;`

const writeEventQuery = `INSERT INTO events (ns, entity, checkname, data)
VALUES ($1, $2, $3, $4) ON CONFLICT (ns, entity, checkname) DO UPDATE SET data=$4;`

func initTest(db *sql.DB) error {
	_, err := db.Exec(ddl)
	if err != nil {
		return fmt.Errorf("couldn't initialize test: %s", err)
	}
	return nil
}

func deleteTest(db *sql.DB) error {
	_, err := db.Exec("DELETE FROM events;")
	if err != nil {
		return fmt.Errorf("error cleaning up test: %s", err)
	}
	return nil
}

func main() {
	flag.Parse()

	db, err := sql.Open("postgres", *pgURL)
	if err != nil {
		log.Fatal(err)
	}

	if err := initTest(db); err != nil {
		log.Fatal(err)
	}

	defer deleteTest(db)

	go eventCounter()

	if err := doBench(db); err != nil {
		log.Fatal(err)
	}
}

func countEvent() {
	atomic.AddInt64(&atomicCounter, 1)
}

func eventCounter() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		counted := atomic.SwapInt64(&atomicCounter, 0)
		fmt.Printf("%d events/sec\n", counted)
	}
}

func bencher(ctx context.Context, write, read *sql.Stmt, index int, eventNames []string, serialized []byte) {
	j := index
	for ctx.Err() == nil {
		eventName := eventNames[j]
		j = (j + index) % len(eventNames)
		var data []byte
		row := read.QueryRowContext(ctx, "default", eventName, eventName)
		if err := row.Scan(&data); err != nil && ctx.Err() == nil && err != sql.ErrNoRows {
			log.Printf("error executing read: %s", err)
			continue
		}
		_, err := write.ExecContext(ctx, "default", eventName, eventName, serialized)
		if err != nil && ctx.Err() == nil {
			log.Printf("error executing write: %s", err)
			continue
		}
		countEvent()
	}
}

func doBench(db *sql.DB) error {
	event := sensu.FixtureEvent("entity", "check")
	serialized, err := json.Marshal(event)
	if err != nil {
		return err
	}

	eventNames := make([]string, *numEvents)

	for i := 0; i < *numEvents; i++ {
		eventNames[i] = uuid.New().String()
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-sigc
		cancel()
	}()

	read, err := db.PrepareContext(ctx, readEventQuery)
	if err != nil && err != context.Canceled {
		return fmt.Errorf("error preparing read statement: %s", err)
	}
	defer read.Close()

	write, err := db.PrepareContext(ctx, writeEventQuery)
	if err != nil && err != context.Canceled {
		return fmt.Errorf("error preparing read statement: %s", err)
	}
	defer write.Close()

	for i := 0; i < *concurrency; i++ {
		go bencher(ctx, write, read, i, eventNames, serialized)
	}

	<-ctx.Done()

	return nil
}
