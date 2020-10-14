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

type handler func(body []byte) error

type consumer struct {
	handlersSync sync.RWMutex
	handlers     map[string]handler
	errChan      chan<- error
	enough       chan struct{}
}

func (con *consumer) process(msgs <-chan amqp.Delivery) {
	go func() {
		for {
			select {
			case d, ok := <-msgs:
				if !ok {
					break
				}

				con.handlersSync.Lock()
				h, ok := con.handlers[d.Type]
				con.handlersSync.Unlock()
				if !ok {
					fmt.Printf("No handler for received message: %s\n", d.Type)
					_ = d.Nack(false, true)
					continue
				}

				if err := h(d.Body); err != nil {
					_ = d.Nack(false, true)
					con.processErr(err)
				}

				if err := d.Ack(false); err != nil {
					con.processErr(err)
				}
			case <-con.enough:
				break
			}
		}
	}()
}

func (con *consumer) addHandler(msgType string, handler handler) {
	con.handlersSync.Lock()
	defer con.handlersSync.Unlock()
	con.handlers[msgType] = handler
}

func (con *consumer) processErr(err error) {
	if con.errChan == nil {
		panic(err)
	}

	con.errChan <- err
}

type eventBus struct {
	*rabbitmq.Connection
	*rabbitmq.Channel
	queueName string
	consumer  *consumer
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
		"consQueue", // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
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
		handlersSync: sync.RWMutex{},
		handlers:     make(map[string]handler),
		errChan:      nil,
		enough:       make(chan struct{}),
	}
	consumer.process(msgs)

	return &eventBus{conn, channel, q.Name, &consumer}, nil
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

	eb.consumer.addHandler(routingKey, func(body []byte) error {
		message := reflect.ValueOf(event).Interface()

		unmarshalOptions := proto.UnmarshalOptions{
			DiscardUnknown: true,
			AllowPartial:   true,
		}
		if err := unmarshalOptions.Unmarshal(body, message.(proto.Message)); err != nil {
			fmt.Printf("Message length: %d -- Unmarshal err: %v\n", len(body), err)
			return err
		}

		if err := handleEvent(message.(proto.Message)); err != nil {
			fmt.Printf("EventHandler err: %v\n", err)
			return err
		}

		return nil
	})

	fmt.Printf("Registered handler for %s routing key\n", routingKey)
	return nil
}

func (eb *eventBus) NotifyError(errChan chan<- error) {
	eb.consumer.errChan = errChan
}

func (eb *eventBus) Close() {
	fmt.Println("Shutting down...")
	eb.consumer.enough <- struct{}{}
	eb.Channel.Close()
	eb.Connection.Close()
}

func main() {
	envCfg := envConfig{}
	checkErr(env.Set(&envCfg))

	bus, err := NewEventBus(envCfg.RabbitURI)
	checkErr(err)
	defer bus.Close()

	errChan := make(chan error)
	bus.NotifyError(errChan)

	checkErr(bus.Subscribe(&producer.NewCargoBooked{}, func(event proto.Message) error {
		newCargo := event.(*producer.NewCargoBooked)
		fmt.Printf("Received NewCargoBooked message [%v]\n", newCargo.GetTrackingId())
		return nil
	}))
	checkErr(bus.Subscribe(&producer.CargoToRouteAssigned{}, func(event proto.Message) error {
		e := event.(*producer.CargoToRouteAssigned)
		fmt.Printf("Received CargoToRouteAssigned message [%v]\n", e.GetTrackingId())
		// if e.GetTrackingId() == "03" {
		// 	return fmt.Errorf("route error")
		// }
		return nil
	}))

	fmt.Println(<-errChan)
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
