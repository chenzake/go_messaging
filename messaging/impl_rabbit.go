package messaging

import (
	"bytes"
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"strings"
	"time"
)

type Target struct {
	Exchange string
	Topic    string
	Server   string
	Fanout   string
}

type AMQPConnection struct {
	URL  string
	conn *amqp.Connection
	ch   *amqp.Channel
}

//type RabbitDriver struct {
//	Default_exchange string
//}

//func (ra_driver RabbitDriver) send(t target.Target, msg string) {
//
//}

type Endpoint struct {
	NameSpace, Version string
	Method             func(args ...interface{}) interface{}
}

var Method_mapping map[string]Endpoint

func (conn *AMQPConnection) ProcessMsg(d amqp.Delivery) {
	msg := *BytesToString(&(d.Body))
	//log.Println("Received a message:", *BytesToString(&(d.Body)))
	messbody := Message{}
	err := json.Unmarshal(d.Body, &messbody)
	FailOnLog(err, "解析失败")
	log.Println("Received a message:", msg)

	oslomessage := OsloMessage{}
	str := []byte(messbody.Message_body.(string))
	err = json.Unmarshal(str, &oslomessage)
	FailOnLog(err, "解析失败")

	endpoint, existed := Method_mapping[oslomessage.Method]
	if existed {
		//var params []reflect.Value
		//params = append(params, reflect.ValueOf())
		res := endpoint.Method()
		if oslomessage.Reply_queue != "" {
			oslomessage.Reply(res, conn)
		}

	} else {
		log.Println("No this method")
	}

}

func BytesToString(b *[]byte) *string {
	s := bytes.NewBuffer(*b)
	r := s.String()
	return &r
}

func (conn *AMQPConnection) AMQP_set_channel(connectionTimeout time.Duration) (success bool) {
	defer func() {
		if err := recover(); err != nil {
			success = false
		}
	}()

	var err error
	conn.conn, err = amqp.DialConfig(conn.URL, amqp.Config{Dial: amqp.DefaultDial(connectionTimeout)})
	//conn.conn, err = amqp.Dial(conn.URL)
	//defer conn.conn.Close()
	FailOnLog(err, "connect failed")

	conn.ch, err = conn.conn.Channel()
	FailOnLog(err, "channel failed")
	success = true
	log.Println("Connected rabbitmq with", conn.URL)
	return success
}

func (conn *AMQPConnection) DeclareDirectConsumer(topic string) {
	_, err := conn.ch.QueueDeclare(
		topic, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	FailOnErrExit(err, "QueueDeclare error")

	msgs, err := conn.ch.Consume(
		topic, // queue
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	go func() {
		for d := range msgs {
			//err = json.Unmarshal(d.Body, &res)
			//log.Println(res)
			go conn.ProcessMsg(d)
			log.Println("from", topic, "Received a message", *BytesToString(&(d.Body)))
		}
	}()
}

func (conn *AMQPConnection) DeclareTopicConsumer(exchange, topic string) {

	err := conn.ch.ExchangeDeclare(
		exchange,
		"topic",
		false,
		false,
		false, false,
		nil)
	FailOnErrExit(err, "Exchange Declare error")
	_, err = conn.ch.QueueDeclare(
		topic, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	FailOnErrExit(err, "QueueDeclare error")

	err = conn.ch.QueueBind(
		topic,
		topic,
		exchange,
		false,
		nil)
	FailOnErrExit(err, "QueueBind")
	msgs, err := conn.ch.Consume(
		topic, // queue
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	go func() {
		for d := range msgs {
			//err = json.Unmarshal(d.Body, &res)
			//log.Println(res)
			go conn.ProcessMsg(d)
			log.Println("from", topic, "Received a message", *BytesToString(&(d.Body)))
		}
	}()

}

func (conn *AMQPConnection) DeclareFanoutConsumer(topic string) {

	var fanout_exchange, fanout_queue strings.Builder
	fanout_exchange.WriteString(topic)
	fanout_exchange.WriteString("_fanout")
	fanout_queue.WriteString(topic)
	fanout_queue.WriteString("_fanout")
	err := conn.ch.ExchangeDeclare(
		fanout_exchange.String(),
		"fanout",
		false,
		true,
		false, false,
		nil)
	FailOnErrExit(err, "Exchange Declare error")
	_, err = conn.ch.QueueDeclare(
		fanout_queue.String(), // name
		true,                  // durable
		false,                 // delete when unused
		false,                 // exclusive
		false,                 // no-wait
		nil,                   // arguments
	)
	FailOnErrExit(err, "QueueDeclare error")

	err = conn.ch.QueueBind(
		fanout_queue.String(),
		topic,
		fanout_exchange.String(),
		false,
		nil)
	FailOnErrExit(err, "QueueBind")

	msgs, err := conn.ch.Consume(
		fanout_queue.String(), // queue
		"",                    // consumer
		true,                  // auto-ack
		false,                 // exclusive
		false,                 // no-local
		false,                 // no-wait
		nil,                   // args
	)
	go func() {
		for d := range msgs {
			//err = json.Unmarshal(d.Body, &res)
			//log.Println(res)
			go conn.ProcessMsg(d)
			log.Println("from", fanout_queue.String(), "Received a message", *BytesToString(&(d.Body)))
		}
	}()
}

func (conn *AMQPConnection) Listen(target Target) {
	connectionTimeout := 2 * time.Second
	sleepInterval := 2 * time.Second
	for {
		connected := conn.AMQP_set_channel(connectionTimeout)
		if !connected {
			if sleepInterval < 60*time.Second {
				sleepInterval = sleepInterval + connectionTimeout
			} else {
				sleepInterval = sleepInterval
			}
			log.Println("retrying in", sleepInterval, "seconds")
			time.Sleep(sleepInterval)
		} else {
			break
		}
	}
	//forever := make(chan bool)
	closeChan := make(chan *amqp.Error, 1)
	notifyClose := conn.ch.NotifyClose(closeChan)
	closeFlag := false

	conn.DeclareTopicConsumer(target.Exchange, target.Topic)
	if target.Server != "" {
		var topic_server strings.Builder
		topic_server.WriteString(target.Topic)
		topic_server.WriteString(".")
		topic_server.WriteString(target.Server)
		conn.DeclareTopicConsumer(target.Exchange, topic_server.String())
	}
	conn.DeclareFanoutConsumer(target.Topic)
	for {
		select {
		case e := <-notifyClose:
			log.Println("chan error, e:" + e.Error())
			//close(closeChan)
			//time.Sleep(sleepInterval)
			//conn.AMQP_set_channel()
			conn.Listen(target)
			closeFlag = true
		}
		//<-forever
		if closeFlag {
			log.Println("Stop service go messaging")
			break
		}
	}
}

func (conn *AMQPConnection) SendReply(reply_q string, msg []byte) {
	publishing := amqp.Publishing{
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		DeliveryMode:    2,
		Priority:        0,
		Body:            msg,
	}
	err := conn.ch.Publish("", reply_q, false, false, publishing)
	log.Println(reply_q, string(msg))
	FailOnLog(err, "Reply Failed")
}

func StartConsumer(target Target, endpoints map[string]Endpoint, conn *AMQPConnection) {

	//for index, _ := range endopints{
	//	endpoint := reflect.ValueOf(&endopints[index])
	//	typ := endpoint.Type()
	//	for i := 0; i < endpoint.NumMethod(); i++{
	//		//v := endpoint.Method(i)
	//		Method_mapping[typ.Method(i).Name] =  endpoint.Method(i)
	//	}
	//}
	Method_mapping = endpoints
	conn.Listen(target)
}

//var (
//	Trace   *log.Logger // 记录所有日志
//	Info    *log.Logger // 重要的信息
//	Warning *log.Logger // 需要注意的信息
//	Error   *log.Logger // 非常严重的问题
//)
//
//func init() {
//	//file, err := os.OpenFile("errors.txt",
//	//	os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
//	//if err != nil {
//	//	log.Fatalln("Failed to open error log file:", err)
//	//}
//
//	Trace = log.New(os.Stdout,
//		"TRACE: ",
//		log.Ldate|log.Ltime|log.Lshortfile)
//
//	Info = log.New(os.Stdout,
//		"INFO: ",
//		log.Ldate|log.Ltime|log.Lshortfile)
//
//	Warning = log.New(os.Stdout,
//		"WARNING: ",
//		log.Ldate|log.Ltime|log.Lshortfile)
//
//	Error = log.New(os.Stdout,
//		"ERROR: ",
//		log.Ldate|log.Ltime|log.Lshortfile)
//}
