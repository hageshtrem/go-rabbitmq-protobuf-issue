run: gen
	docker-compose up -d

gen: gen-producer gen-consumer

gen-producer:
	@protoc --proto_path=proto --go_out=:. proto/booking_events.proto proto/itinerary.proto

gen-consumer:
	@protoc --proto_path=proto --go_out=consumer/pb proto/booking_events.proto proto/itinerary.proto

.PHONY: gen gen-producer gen-consumer run