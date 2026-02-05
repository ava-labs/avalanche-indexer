#!/bin/bash
# Script to create SASL/SCRAM users in local Kafka
# Run this after starting Kafka with SASL enabled

set -e

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVER="localhost:9092"
ADMIN_USER="admin"
ADMIN_PASS="admin-secret"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Setting up SASL users in Kafka...${NC}"

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
until docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9093 > /dev/null 2>&1; do
  echo "Waiting for Kafka..."
  sleep 2
done

echo -e "${GREEN}Kafka is ready!${NC}"

# Create users using kafka-configs.sh
# Note: We use the internal PLAINTEXT listener (9093) for admin operations
# since the SASL listener is for external clients

create_user() {
    local username=$1
    local password=$2
    
    echo -e "${YELLOW}Creating user: $username${NC}"
    
    # Create SCRAM-SHA-512 credentials
    docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-configs.sh \
        --bootstrap-server localhost:9093 \
        --alter \
        --add-config "SCRAM-SHA-512=[password=$password]" \
        --entity-type users \
        --entity-name "$username" || {
        echo "User $username might already exist, continuing..."
    }
    
    echo -e "${GREEN}User $username created successfully${NC}"
}

# Create test users
create_user "testuser" "testpass"
create_user "producer" "producer-secret"
create_user "consumer" "consumer-secret"

echo -e "${GREEN}All users created!${NC}"
echo ""
echo "Available users:"
echo "  - testuser / testpass"
echo "  - producer / producer-secret"
echo "  - consumer / consumer-secret"
echo "  - admin / admin-secret (for broker operations)"
echo ""
echo "To test with blockfetcher:"
echo "  bin/blockfetcher run \\"
echo "    --kafka-brokers localhost:9092 \\"
echo "    --kafka-sasl-username testuser \\"
echo "    --kafka-sasl-password testpass \\"
echo "    --kafka-sasl-mechanism SCRAM-SHA-512 \\"
echo "    --kafka-security-protocol SASL_PLAINTEXT \\"
echo "    # ... other flags"

