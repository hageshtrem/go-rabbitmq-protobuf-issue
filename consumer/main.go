package main

import (
	producer "consumer/pb/producer/pb"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"

	"github.com/codingconcepts/env"
	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"
)

const timeFormat = "02.01.2006"

type envConfig struct {
	RabbitURI string `env:"RABBIT_URI" default:"amqp://guest:guest@localhost:5672/"`
}

type eventBus struct {
	*rabbitmq.Connection
	*rabbitmq.Channel
	queueName string
}

// NewEventBus returns implementation of application.EventBus.
func NewEventBus(uri string) (*eventBus, error) {
	conn, err := rabbitmq.Dial(uri)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if err := channel.ExchangeDeclare(
		"issue",             // name
		amqp.ExchangeDirect, // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	); err != nil {
		return nil, err
	}

	q, err := channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	if err := channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	); err != nil {
		return nil, err
	}

	return &eventBus{conn, channel, q.Name}, nil
}

func (eb *eventBus) Subscribe(event proto.Message, handleEvent func(event proto.Message) error) error {
	routingKey := strings.Split(reflect.TypeOf(event).String(), ".")[1]
	fmt.Printf("Routing key: %s\n", routingKey)
	if err := eb.QueueBind(
		eb.queueName, // queue name
		routingKey,   // routing key
		"issue",      // exchange
		false,
		nil); err != nil {
		return err
	}

	msgs, err := eb.Channel.Consume(
		eb.queueName, // queue
		"",           // consumer
		false,        // auto ack
		false,        // exclusive
		false,        // no local
		false,        // no wait
		nil,          // args
	)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			message := reflect.ValueOf(event).Interface()
			unmarshalOptions := proto.UnmarshalOptions{
				DiscardUnknown: true,
				AllowPartial:   true,
			}
			if err := unmarshalOptions.Unmarshal(d.Body, message.(proto.Message)); err != nil {
				// log
				fmt.Printf("Unmarshal err: %v\n%v\n", err, len(d.Body))
				continue
			}

			if err := handleEvent(message.(proto.Message)); err != nil {
				// log
				fmt.Printf("EventHandler err: %v\n", err)
			}

			if err := d.Ack(true); err != nil {
				fmt.Printf("Acknolegement error: %v\n", err)
			}
		}
	}()

	return nil
}

func (eb *eventBus) Close() {
	eb.Channel.Close()
	eb.Connection.Close()
}

func main() {
	envCfg := envConfig{}
	checkErr(env.Set(&envCfg))

	bus, err := NewEventBus(envCfg.RabbitURI)
	checkErr(err)
	defer bus.Close()

	bus.Subscribe(&producer.NewCargoBooked{}, func(event proto.Message) error {
		newCargo := event.(*producer.NewCargoBooked)
		log.Printf("New Cargo booked: %v", newCargo)
		return nil
	})

	bus.Subscribe(&producer.CargoToRouteAssigned{}, func(event proto.Message) error {
		e := event.(*producer.CargoToRouteAssigned)
		log.Printf("Cargo assigned to route: %v", e.GetTrackingId())
		return nil
	})

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
