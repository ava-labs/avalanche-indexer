package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	fmt.Println("hello world")

	// Create a Kafka consumer instance
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "hello-world-consumer",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		fmt.Printf("Note: Failed to create consumer (Kafka may not be running): %v\n", err)
		fmt.Println("Consumer will exit - this is expected if Kafka is not configured yet")
		return
	}
	defer consumer.Close()

	fmt.Println("Kafka consumer created successfully")

	// Set up signal handling for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)

	// Wait for interrupt signal
	<-sigchan
	fmt.Println("\nShutting down consumer...")
}
