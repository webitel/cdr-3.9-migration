package rabbit

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

var (
	Channel    *amqp.Channel
	Connection *amqp.Connection
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func Connect(host string) {
	var err error
	Connection, err = amqp.Dial(host)
	failOnError(err, "Failed to connect to RabbitMQ")

	Channel, err = Connection.Channel()
	failOnError(err, "Failed to open a channel")

	err = Channel.ExchangeDeclare(
		"TAP.CDR-Events", // name
		"direct",         // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
	failOnError(err, "Failed to declare an exchange")
}

func Publish(body []byte) {
	err := Channel.Publish(
		"TAP.CDR-Events", // exchange
		"cdr-leg-a",      // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         body,
		})
	if err != nil {
		panic(err)
		log.Printf("ERROR [Rabbit]: %s", err)
	}
}
