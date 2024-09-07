package main

import (
	infra_kafka "ticket-service/infra/kafka"
)

func main() {
	consumer1 := infra_kafka.CreateConsumer("groupid1")
	consumer2 := infra_kafka.CreateConsumer("groupid2")

    go consumer1.Listen()
    consumer2.Listen()
}
