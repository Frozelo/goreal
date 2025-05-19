package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	UserId    string `json:"user_id"`
	Action    string `json:"action"`
	Timestamp int64  `json:"timestamp"`
}

var genMode bool

func init() {
	flag.BoolVar(&genMode, "gen-mode", false, "generate events in a loop")
}

func main() {
	mux := http.NewServeMux()

	broker := os.Getenv("KAFKA_BROKER")
	topic := os.Getenv("KAFKA_TOPIC")

	kafkaCfg := kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
	}

	writer := kafka.NewWriter(kafkaCfg)
	defer writer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	flag.Parse()

	if genMode {
		genDuration := 30 * time.Second
		log.Println("starting event generator")
		go startGenerator(ctx, writer, genDuration)
	}

	mux.HandleFunc("/event", func(w http.ResponseWriter, r *http.Request) {
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

	srv := &http.Server{
		Addr:    ":8081",
		Handler: mux,
	}

	log.Println("producer service started at port 8081")
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("failed to start HTTP server: %v", err)
	}

	go func() {
		<-ctx.Done()
		log.Println("got signal to stop, shutting down HTTP server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("error shutting down HTTP server: %v", err)
		}

		longShutdown := make(chan struct{}, 1)

		go func() {
			time.Sleep(3 * time.Second)
			longShutdown <- struct{}{}
		}()

		select {
		case <-shutdownCtx.Done():
			log.Println("server shutdown error: %w", ctx.Err())
		case <-longShutdown:
			log.Println("finished")
		}
	}()
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

func startGenerator(ctx context.Context, writer *kafka.Writer, duration time.Duration) error {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	actions := []string{"click", "view", "purchase", "signup"}

	log.Println("the duration is", duration)
	var endTime time.Time
	if duration > 0 {
		endTime = time.Now().Add(duration)
	}

	for {
		if !endTime.IsZero() && time.Now().After(endTime) {
			log.Println("generator stopped")
			return nil
		}

		evt := &Event{
			UserId:    fmt.Sprintf("user-%d", rand.Intn(1000)),
			Action:    actions[rand.Intn(len(actions))],
			Timestamp: time.Now().UnixNano(),
		}

		if err := writeMessage(ctx, writer, evt); err != nil {
			log.Printf("error writing generated event: %v", err)
		} else {
			log.Printf("generated event: %v", evt)
		}
	}
}
