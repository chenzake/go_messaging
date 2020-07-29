package main

//import (
//	"go_messaging/messaging/message"
//)
//
//func main() {
//	msg := new(message.Message)
//	msg.Msg = "test"
//	msg.Version = '1'
//
//	msg.Serialize_msg(msg)
//}

import (
	"go_messaging/messaging"
	"log"
	"net/http"
	_ "net/http/pprof"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	conn := new(messaging.AMQPConnection)
	conn.URL = "amqp://admin:admin@10.200.2.187:5672/"

	target := messaging.Target{Exchange: "go_test", Topic: "new_go_test", Server: "server1"}
	conn.Listen(target)
}
