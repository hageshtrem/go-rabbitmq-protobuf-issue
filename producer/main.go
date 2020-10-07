package main

import (
	"fmt"
	"log"
	"producer/pb"
	"reflect"
	"strings"

	"github.com/codingconcepts/env"
	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type envConfig struct {
	RabbitURI string `env:"RABBIT_URI" default:"amqp://guest:guest@localhost:5672/"`
}

type eventBus struct {
	*rabbitmq.Connection
	*rabbitmq.Channel
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

	return &eventBus{conn, channel}, nil
}

func (eb *eventBus) Publish(message proto.Message) error {
	routingKey := strings.Split(reflect.TypeOf(message).String(), ".")[1]
	messageContent, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	if err := eb.Channel.Publish(
		"issue",    // publish to an exchange
		routingKey, // routing to 0 or more queues
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType:  "application/x-protobuf; proto=" + routingKey, // TODO: change when standardized
			Body:         messageContent,
			DeliveryMode: amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:     0,              // 0-9
		},
	); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}

	fmt.Printf("Successfully published %d byte message for %s routing key\n", len(messageContent), routingKey)
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

	cargo := pb.NewCargoBooked{
		TrackingId:      "01",
		Origin:          "AUMEL",
		Destination:     "JNTKO",
		ArrivalDeadline: timestamppb.Now(),
	}

	checkErr(bus.Publish(&cargo))
	checkErr(bus.Publish(&cargo))
	checkErr(bus.Publish(&cargo))

	legs := []*pb.Leg{
		&pb.Leg{
			VoyageNumber:   "001",
			LoadLocation:   "AA",
			UnloadLocation: "BB",
			LoadTime:       timestamppb.Now(),
			UnloadTime:     timestamppb.Now(),
		},
		&pb.Leg{
			VoyageNumber:   "002",
			LoadLocation:   "BB",
			UnloadLocation: "CC",
			LoadTime:       timestamppb.Now(),
			UnloadTime:     timestamppb.Now(),
		},
	}
	routeAssigned := pb.CargoToRouteAssigned{
		TrackingId: "01",
		Itinerary:  &pb.Itinerary{Legs: legs},
		Eta:        timestamppb.Now(),
	}

	checkErr(bus.Publish(&routeAssigned))
	checkErr(bus.Publish(&routeAssigned))
	checkErr(bus.Publish(&routeAssigned))
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
