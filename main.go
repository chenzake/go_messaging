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

func Print(args ...interface{}) {
	log.Println("my test")
}

func main() {
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	endpoint := messaging.Endpoint{
		NameSpace: "my_test",
		Version:   "v1.0",
		Method:    Print,
	}
	endpoints := make(map[string]messaging.Endpoint)
	//var Method_mapping map[string]messaging.Endpoint
	endpoints["test"] = endpoint

	conn := new(messaging.AMQPConnection)
	conn.URL = "amqp://admin:admin@10.200.2.187:5672/"

	target := messaging.Target{Exchange: "go_test", Topic: "new_go_test", Server: "server1"}
	messaging.StartConsumer(target, endpoints, conn)
	conn.Listen(target)
}
