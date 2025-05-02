package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := []string{"kafka:9092"}

	kafkaCfg := kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: "my_group",
		Topic:   "my_topic",
	}

	reader := kafka.NewReader(kafkaCfg)
	defer reader.Close()

	ctx := context.Background()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("could not read message: %v", err)
		}

		log.Printf("Received: %s inside %s\n", string(msg.Value), string(msg.Key))
		time.Sleep(10 * time.Second)
	}

}
