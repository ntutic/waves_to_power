package main

import (
	"fmt"
	"log"
	"os"

	"github.com/nilsmagnus/grib/griblib"
)

func ReadGrib(file string) error {

	gribfile, err := os.Open(file)
	if err != nil {
		log.Fatalf("Could not open test-file %v", err)
	}
	messages, err := griblib.ReadMessages(gribfile)

	for _, message := range messages {
		fmt.Print(message)
	}

	return err
}
