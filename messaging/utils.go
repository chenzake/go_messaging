package messaging

import (
	"log"
)

func FailOnErrExit(err error, msg string) {
	if err != nil {
		log.Println("msg", err, msg)
		panic("error")
	}
}

func FailOnLog(err error, msg string) {
	if err != nil {
		log.Println("msg", err, msg)
	}
}
