package main

import (
	"log"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	kafkaConf := &kafka.ConfigMap{"bootstrap.servers": "localhost:9092"}

	producer, err := kafka.NewProducer(kafkaConf)
	if err != nil {
		panic(err)
	}

	defer producer.Close()

	topic := "sample_topic"

	for i := range 10 {
		msg := []byte("the msg number " + strconv.Itoa(i))
		kafkaMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          msg,
		}

		producer.Produce(kafkaMsg, nil)

		log.Printf("Message %s sent", msg)
		time.Sleep(1 * time.Second)
	}

	producer.Flush(1000)
}
