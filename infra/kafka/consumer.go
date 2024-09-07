package infra_kafka

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type IConsumer interface {
    Listen() error
}

func CreateConsumer(GroupId string) IConsumer {
    consumer, consumerError := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
        "group.id":          GroupId,
        "auto.offset.reset": "earliest",
    })

    if consumerError != nil {
        return nil
    }

    subscribeTopicError := consumer.SubscribeTopics([]string{"teste"}, nil)

    if subscribeTopicError != nil {
        consumer.Close()
        return nil
    }

    return &ConsumerImpl{consumer: consumer}
}

type ConsumerImpl struct {
    consumer *kafka.Consumer
}

func (c *ConsumerImpl) Listen() error {
    run := true

    for run {
        msg, readMessageError := c.consumer.ReadMessage(time.Second)

        if readMessageError == nil {
            fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
        } else if !readMessageError.(kafka.Error).IsTimeout() {
            fmt.Printf("Consumer error: %v (%v)\n", readMessageError, msg)
        }
    }

    c.consumer.Close()
    return nil
}
