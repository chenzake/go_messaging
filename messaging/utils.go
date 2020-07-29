package messaging

import "log"

func FailOnErr(err error, msg string) {
	if err != nil {
		log.Println("msg", err, msg)
		//panic("error")
	}
}
