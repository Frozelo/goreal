package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Event struct {
	UserId    string `json:"user_id"`
	Action    string `json:"action"`
	Timestamp int64  `json:"timestamp"`
}

func processMessage(ctx context.Context, pool *pgxpool.Pool, message *kafka.Message) error {
	var e Event
	if err := json.Unmarshal(message.Value, &e); err != nil {
		return fmt.Errorf("processMessage: json unmarshal error: %w", err)
	}

	log.Println("Recieved", e)

	if err := writeEvent(ctx, pool, &e); err != nil {
		return err
	}

	return nil
}

func writeEvent(ctx context.Context, pool *pgxpool.Pool, e *Event) error {
	_, err := pool.Exec(ctx,
		`
		INSERT INTO event (user_id, action, timestamp)
	 	VALUES ($1, $2, $3)
		`,
		e.UserId, e.Action, e.Timestamp)
	if err != nil {
		return fmt.Errorf("writeEvent: failed to write: %w", err)
	}

	return nil
}

func kafkaReader(ctx context.Context, reader *kafka.Reader, msgCh chan<- *kafka.Message) {
	defer close(msgCh)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Context cancelled in kafkaReader (on error)")
				return
			}
			log.Println("read error: %w", err)
			time.Sleep(1 * time.Second)
			continue
		}

		select {
		case msgCh <- &msg:
		case <-ctx.Done():
			log.Println("Context cancelled in kafkaReaderRoutine")
			return
		}
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	dbUrl := os.Getenv("DATABASE_URL")
	pool, err := pgxpool.New(ctx, dbUrl)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}

	defer pool.Close()

	broker := os.Getenv("KAFKA_BROKER")
	topic := os.Getenv("KAFKA_TOPIC")

	kafkaCfg := kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: "my_group",
	}

	reader := kafka.NewReader(kafkaCfg)
	defer reader.Close()

	log.Println("Consumer started, waiting for messages...")

	msgCh := make(chan *kafka.Message)
	go kafkaReader(ctx, reader, msgCh)

	log.Println("Consumer started, waiting for messages...")

	for msg := range msgCh {
		if err := processMessage(ctx, pool, msg); err != nil {
			log.Println(err)
			continue
		}
	}

	log.Println("Performing final cleanup before exit...")
	log.Println("Consumer stopped")
}
