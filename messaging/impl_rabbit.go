package messaging

import (
	"bytes"
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
func Process_msg(d amqp.Delivery) {
	log.Println("Received a message:", *BytesToString(&(d.Body)))
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
	FailOnErr(err, "connect failed")

	conn.ch, err = conn.conn.Channel()
	FailOnErr(err, "channel failed")
	success = true
	log.Println("Connected rabbitmq with", conn.URL)
	return success
}

func (conn *AMQPConnection) DeclareDirectConsumer(topic string, callback func(d amqp.Delivery)) {
	_, err := conn.ch.QueueDeclare(
		topic, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	FailOnErr(err, "QueueDeclare error")

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
			callback(d)
			log.Println("from", topic, "Received a message", *BytesToString(&(d.Body)))
		}
	}()
}

func (conn *AMQPConnection) DeclareTopicConsumer(exchange, topic string, callback func(d amqp.Delivery)) {

	err := conn.ch.ExchangeDeclare(
		exchange,
		"topic",
		false,
		false,
		false, false,
		nil)
	FailOnErr(err, "Exchange Declare error")
	_, err = conn.ch.QueueDeclare(
		topic, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	FailOnErr(err, "QueueDeclare error")

	err = conn.ch.QueueBind(
		topic,
		topic,
		exchange,
		false,
		nil)
	FailOnErr(err, "QueueBind")
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
			callback(d)
			log.Println("from", topic, "Received a message", *BytesToString(&(d.Body)))
		}
	}()

}

func (conn *AMQPConnection) DeclareFanoutConsumer(topic string, callback func(d amqp.Delivery)) {

	var fanout_exchange, fanout_queue strings.Builder
	fanout_exchange.WriteString(topic)
	fanout_exchange.WriteString("_fanout")
	fanout_queue.WriteString(topic)
	fanout_queue.WriteString("_fanout")
	err := conn.ch.ExchangeDeclare(
		fanout_exchange.String(),
		"fanout",
		false,
		false,
		false, false,
		nil)
	FailOnErr(err, "Exchange Declare error")
	_, err = conn.ch.QueueDeclare(
		fanout_queue.String(), // name
		false,                 // durable
		false,                 // delete when unused
		false,                 // exclusive
		false,                 // no-wait
		nil,                   // arguments
	)
	FailOnErr(err, "QueueDeclare error")

	err = conn.ch.QueueBind(
		fanout_queue.String(),
		topic,
		fanout_exchange.String(),
		false,
		nil)
	FailOnErr(err, "QueueBind")

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
			callback(d)
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
	defer conn.ch.Close()
	defer conn.conn.Close()
	//forever := make(chan bool)
	closeChan := make(chan *amqp.Error, 1)
	notifyClose := conn.ch.NotifyClose(closeChan)
	closeFlag := false

	conn.DeclareTopicConsumer(target.Exchange, target.Topic, Process_msg)
	if target.Server != "" {
		var topic_server strings.Builder
		topic_server.WriteString(target.Topic)
		topic_server.WriteString(target.Server)
		conn.DeclareTopicConsumer(target.Exchange, topic_server.String(), Process_msg)
	}
	conn.DeclareFanoutConsumer(target.Topic, Process_msg)
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
			break
		}
	}
}
