package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
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
	actions := []string{"login", "logout", "click", "purchase", "view"}

	kafkaCfg := kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
	}

	writer := kafka.NewWriter(kafkaCfg)
	defer writer.Close()

	rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()

	for {
		e := Event{
			UserId:    fmt.Sprintf("user-%d", rand.Intn(5)),
			Action:    actions[rand.Intn(len(actions))],
			Timestamp: time.Now().Unix(),
		}

		buf, err := json.Marshal(e)
		if err != nil {
			log.Println("marshal error:", err)
			continue
		}

		err = writer.WriteMessages(ctx,
			kafka.Message{
				Key:   []byte(e.UserId),
				Value: buf,
			})

		if err != nil {
			log.Println("write error:", err)
		} else {
			log.Printf("sent %+v\n", e)
		}

		time.Sleep(1 * time.Second)
	}
}
