package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	UserId    string `json:"user_id"`
	Action    string `json:"action"`
	Timestamp int64  `json:"timestamp"`
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	topic := os.Getenv("KAFKA_TOPIC")

	kafkaCfg := kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: "my_group",
	}

	reader := kafka.NewReader(kafkaCfg)
	defer reader.Close()

	ctx := context.Background()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println("read error: %w", err)
			time.Sleep(1 * time.Second)
			continue
		}

		var e Event
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			log.Println("unmarshal error:", err)
			continue
		}

		log.Println("Received:", e)
	}

}
