package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"time"
)

func main() {
	output := flag.String("output", "", "output file")

	flag.Parse()

	if *output == "" {
		panic("output is required")
	}

	var i uint32

	outFile, err := os.OpenFile(*output, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	for {
		time.Sleep(5 * time.Second)

		entry := logEntry{
			Message:   "Hello, world!",
			Timestamp: time.Now(),
		}

		entry.Message = fmt.Sprintf("Hello, Surreal Logs! %d", i)

		data, err := json.Marshal(entry)
		if err != nil {
			panic(err)
		}

		if _, err := outFile.Write(data); err != nil {
			panic(err)
		}

		if _, err := outFile.Write([]byte("\n")); err != nil {
			panic(err)
		}

		if err := outFile.Sync(); err != nil {
			panic(err)
		}

		if i == math.MaxUint32 {
			i = 0
		} else {
			i++
		}
	}
}

type logEntry struct {
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}
