package main

import (
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	kafkaCfg := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "test_group",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(kafkaCfg)
	if err != nil {
		panic(err)
	}

	defer consumer.Close()

	topic := "test_topic"

	consumer.Subscribe(topic, nil)

	for {
		msg, err := consumer.ReadMessage(10 * time.Second)
		if err == nil {
			log.Printf("Получено: %s", msg.Value)
		} else {
			log.Printf("Ошибка чтения сообщения: %v", err)
		}
	}
}
