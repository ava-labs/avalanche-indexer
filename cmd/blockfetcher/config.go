package main

import (
	confluentKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// buildKafkaProducerConfig builds a Kafka producer ConfigMap from CLI context flags
func buildKafkaProducerConfig(kafkaBrokers, kafkaClientID string, kafkaEnableLogs bool) *confluentKafka.ConfigMap {
	return &confluentKafka.ConfigMap{
		// Required
		"bootstrap.servers": kafkaBrokers,
		"client.id":         kafkaClientID,

		// Reliability: wait for all replicas to acknowledge
		"acks": "all",

		// Performance tuning
		"linger.ms":        5,     // Batch messages for 5ms
		"batch.size":       16384, // 16KB batch size
		"compression.type": "lz4", // Fast compression

		// Idempotence for exactly-once semantics
		"enable.idempotence": true,

		// Go channel for logs (optional, enable for debugging)
		"go.logs.channel.enable": kafkaEnableLogs,
	}
}
