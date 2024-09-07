package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	consumer, consumerError := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"group.id":          "ticket",
			"auto.offset.reset": "earliest",
	})

    if consumerError != nil {
        panic(consumerError)
    }

    subscribeTopicError := consumer.SubscribeTopics([]string{"teste"}, nil)

    if subscribeTopicError != nil {
        panic(subscribeTopicError)
    }

    run := true

    for run {
        msg, readMessageError := consumer.ReadMessage(time.Second)

        if readMessageError == nil {
            fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
        } else if !readMessageError.(kafka.Error).IsTimeout() {
            fmt.Printf("Consumer error: %v (%v)\n", readMessageError, msg)
        }
    }

    consumer.Close()
}
