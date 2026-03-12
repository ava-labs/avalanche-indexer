package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

// testLogger creates a test logger for use in tests
func testLogger(t *testing.T) *zap.SugaredLogger {
	t.Helper()
	logger, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	return logger
}

func TestNew_InvalidConfig_EmptyRegion(t *testing.T) {
	cfg := Config{
		Region: "",
	}

	client, err := New(cfg, testLogger(t))

	require.Error(t, err)
	assert.Equal(t, "dynamodb region is required", err.Error())
	assert.Nil(t, client)
}

func TestNew_InvalidConfig_WhitespaceRegion(t *testing.T) {
	cfg := Config{
		Region: "   ",
	}

	client, err := New(cfg, testLogger(t))

	require.Error(t, err)
	assert.Equal(t, "dynamodb region is required", err.Error())
	assert.Nil(t, client)
}

func TestNew_SuccessfulCreation(t *testing.T) {
	cfg := Config{
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	client, err := New(cfg, testLogger(t))

	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestNew_WithEndpointURL(t *testing.T) {
	cfg := Config{
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
		EndpointURL:     "http://localhost:8000",
	}

	client, err := New(cfg, testLogger(t))

	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestDynamoDBConfig_Defaults(t *testing.T) {
	cfg := Config{}

	assert.NotNil(t, cfg)
}

func TestNew_AllConfigFields(t *testing.T) {
	cfg := Config{
		Region:          "eu-west-1",
		AccessKeyID:     "custom-key",
		SecretAccessKey: "custom-secret",
		EndpointURL:     "https://dynamodb.eu-west-1.amazonaws.com",
	}

	client, err := New(cfg, testLogger(t))

	require.NoError(t, err)
	require.NotNil(t, client)
}
