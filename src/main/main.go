package main

import (
	"log"
	"operator"
)

func main() {
	log.Printf("Main program starts\n")
	aspace := operator.NewAirSpace()
	aspace.Start()
	log.Printf("Main program ends\n")
	for {

	}
}
