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

type envConfig struct {
	RabbitURI string `env:"RABBIT_URI" default:"amqp://guest:guest@localhost:5672/"`
}

type consumer struct {
	subscribersSync sync.RWMutex
	subscribers     map[string]chan<- []byte
}

func (cons *consumer) process(msgs <-chan amqp.Delivery) {
	for d := range msgs {
		cons.subscribersSync.Lock()
		ch, ok := cons.subscribers[d.Type]
		if ok {
			ch <- d.Body
		}
		cons.subscribersSync.Unlock()

		// TODO:
		// if err := d.Ack(false); err != nil {
		// 	fmt.Printf("Acknolegement error: %v\n", err)
		// }
	}
}

func (cons *consumer) addSubscriber(msgType string, out chan<- []byte) {
	cons.subscribersSync.Lock()
	defer cons.subscribersSync.Unlock()
	cons.subscribers[msgType] = out
}

type eventBus struct {
	*rabbitmq.Connection
	*rabbitmq.Channel
	queueName string
	consumer  consumer
}

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

	// if err := channel.Qos(
	// 	1,     // prefetch count
	// 	0,     // prefetch size
	// 	false, // global
	// ); err != nil {
	// 	return nil, err
	// }

	msgs, err := channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	if err != nil {
		return nil, err
	}

	consumer := consumer{
		subscribersSync: sync.RWMutex{},
		subscribers:     make(map[string]chan<- []byte),
	}
	go consumer.process(msgs)

	return &eventBus{conn, channel, q.Name, consumer}, nil
}

func (eb *eventBus) Subscribe(event proto.Message, handleEvent func(event proto.Message) error) error {
	routingKey := strings.Split(reflect.TypeOf(event).String(), ".")[1]

	if err := eb.QueueBind(
		eb.queueName, // queue name
		routingKey,   // routing key
		"issue",      // exchange
		false,
		nil); err != nil {
		return err
	}

	msgsChan := make(chan []byte)
	eb.consumer.addSubscriber(routingKey, msgsChan)

	go func() {
		for {
			body := <-msgsChan
			message := reflect.ValueOf(event).Interface()

			unmarshalOptions := proto.UnmarshalOptions{
				DiscardUnknown: true,
				AllowPartial:   true,
			}
			if err := unmarshalOptions.Unmarshal(body, message.(proto.Message)); err != nil {
				fmt.Printf("Message length: %d -- Unmarshal err: %v\n", len(body), err)
				continue
			}

			if err := handleEvent(message.(proto.Message)); err != nil {
				fmt.Printf("EventHandler err: %v\n", err)
			}
		}
	}()

	fmt.Printf("Registered handler for %s routing key\n", routingKey)
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

	checkErr(bus.Subscribe(&producer.NewCargoBooked{}, func(event proto.Message) error {
		newCargo := event.(*producer.NewCargoBooked)
		fmt.Printf("Successfully received NewCargoBooked message [%v]\n", newCargo.GetTrackingId())
		return nil
	}))

	checkErr(bus.Subscribe(&producer.CargoToRouteAssigned{}, func(event proto.Message) error {
		e := event.(*producer.CargoToRouteAssigned)
		fmt.Printf("Successfully received CargoToRouteAssigned message [%v]\n", e.GetTrackingId())
		return nil
	}))

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
