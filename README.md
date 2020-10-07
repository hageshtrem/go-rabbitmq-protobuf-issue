# Go RabbitMQ Protobuf Issue

Messages are defined as protobuf. These messages are then serialized and published to RabbitMQ by the producer. The consumer receives them and tries to deserialize them. And here I see that some messages are successfully deserialized and some are not.
I don’t understand what I’m doing wrong.

## Reproducing

Run RabbitMQ:
```
docker run -d --name some-rabbit --network host rabbitmq:3-management
```

Generate proto stub:
```
make gen
```

Run consumer and producer in separate terminals:
```
cd consumer
go run ./main.go
----------------
cd producer
go run ./main.go
```