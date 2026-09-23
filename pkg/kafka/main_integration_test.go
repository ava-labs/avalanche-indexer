//go:build integration
// +build integration

package kafka

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/docker/docker/api/types/container"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// sharedBrokers is the bootstrap address of the one Kafka broker used by every
// integration test in this package. Tests read it through their existing
// setup helpers rather than directly.
var sharedBrokers string

// TestMain starts a single Kafka broker for the whole package.
//
// Each test previously started and tore down its own container. A broker boot
// is 10-20s, so thirty of them dominated the suite's runtime. The container
// also binds a fixed host port (9093), which means two of them can never run
// concurrently — so the cost could not be recovered with parallelism either.
//
// Sharing is safe here because no test depends on an empty broker: topic names
// do not collide across tests, each consumer test generates a unique group id,
// and every count assertion is GreaterOrEqual rather than exact, so records
// left by an earlier test cannot fail a later one.
func TestMain(m *testing.M) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:7.5.0",
		ExposedPorts: []string{"9093/tcp"},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = map[nat.Port][]nat.PortBinding{
				"9093/tcp": {{HostIP: "127.0.0.1", HostPort: "9093"}},
			}
		},
		Env: map[string]string{
			"KAFKA_LISTENERS": "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094",
			// Advertise IPv4 explicitly. Clients follow the advertised address after
			// bootstrap, and "localhost" prefers ::1 on hosts with IPv6 while the
			// broker is published on 127.0.0.1 only.
			"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://127.0.0.1:9093,BROKER://127.0.0.1:9092",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9094",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "true",
			"CLUSTER_ID":                                     "MkU3OEVBNTcwNTJENDM2Qk",
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(3 * time.Minute),
	}

	kafka, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("failed to start shared Kafka container: %v", err)
	}

	// The container publishes 9093 on 127.0.0.1 only. Resolving the host name
	// instead would yield "localhost", which prefers ::1 on hosts with IPv6 and
	// is refused, so address the binding directly.
	sharedBrokers = "127.0.0.1:9093"
	waitForSharedBroker(sharedBrokers)

	code := m.Run()

	if err := kafka.Terminate(ctx); err != nil {
		log.Printf("failed to terminate shared Kafka container: %v", err)
	}

	os.Exit(code)
}

// waitForSharedBroker blocks until the broker answers a metadata request. The
// container's log wait only proves the process started; the per-test helpers
// used to repeat this check, which is unnecessary once the broker is warm.
func waitForSharedBroker(brokers string) {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		admin, err := ckafka.NewAdminClient(&ckafka.ConfigMap{"bootstrap.servers": brokers})
		if err == nil {
			md, mdErr := admin.GetMetadata(nil, false, 5000)
			admin.Close()
			if mdErr == nil && len(md.Brokers) > 0 {
				return
			}
		}
		if time.Now().After(deadline) {
			log.Fatalf("shared Kafka broker never became ready at %s", brokers)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// sharedKafka reports the shared broker, failing the test if TestMain did not
// bring one up.
func sharedKafka(t *testing.T) string {
	t.Helper()
	if sharedBrokers == "" {
		t.Fatal("shared Kafka broker is not running; TestMain did not start it")
	}
	return sharedBrokers
}
