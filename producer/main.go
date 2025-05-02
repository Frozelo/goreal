package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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

	kafkaCfg := kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
	}

	writer := kafka.NewWriter(kafkaCfg)
	defer writer.Close()

	ctx := context.Background()

	http.HandleFunc("/event", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			log.Println("method not allowed")
			w.Write([]byte(`{"error": "method not allowed"}`))
			return
		}

		event := &Event{
			UserId:    r.Header.Get("user-id"),
			Action:    r.Header.Get("action"),
			Timestamp: time.Now().UnixNano(),
		}

		err := writeMessage(ctx, writer, event)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			log.Println("failed to write message:", err)
			w.Write([]byte(`{"error": "failed to write message"}`))
			return
		}

		w.WriteHeader(http.StatusAccepted)
		log.Println("event written successfully")
		w.Write([]byte(`{"message": "event written successfully"}`))
	})

	log.Println("producer service started at port 8081")
	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Fatalf("failed to start HTTP server: %v", err)
	}
}

func writeMessage(ctx context.Context, writer *kafka.Writer, event *Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("failed to marshal event data: %v", err)
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.UserId),
		Value: []byte(data),
		Time:  time.Now(),
	})
	if err != nil {
		log.Printf("failed to write message: %v", err)
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}
