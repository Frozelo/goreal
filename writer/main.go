package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := []string{"kafka:9092"}

	kafkaCfg := kafka.WriterConfig{
		Brokers: brokers,
		Topic:   "my_topic",
	}

	writer := kafka.NewWriter(kafkaCfg)
	defer writer.Close()

	ctx := context.Background()

	for i := range 10 {
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%d", i)),
			Value: []byte(fmt.Sprintf("Hello %d", i)),
		}

		if err := writer.WriteMessages(ctx, msg); err != nil {
			log.Fatalf("could not write message %d: %v", i, err)
		}

		log.Printf("message %d written", i)
		time.Sleep(1 * time.Second)

	}
}
