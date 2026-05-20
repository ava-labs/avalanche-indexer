package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_InvalidConfig_EmptyRegion(t *testing.T) {
	cfg := Config{
		Region: "",
	}

	client, err := New(cfg)

	require.ErrorIs(t, err, errRegionRequired)
	assert.Nil(t, client)
}

func TestNew_InvalidConfig_WhitespaceRegion(t *testing.T) {
	cfg := Config{
		Region: "   ",
	}

	client, err := New(cfg)

	require.ErrorIs(t, err, errRegionRequired)
	assert.Nil(t, client)
}

func TestNew_SuccessfulCreation(t *testing.T) {
	cfg := Config{
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	client, err := New(cfg)

	require.NoError(t, err)
	require.NotNil(t, client)
}

// Region-only config must construct successfully so the AWS default chain
// can supply credentials at call time.
func TestNew_NoStaticCredentials_DefersToDefaultChain(t *testing.T) {
	cfg := Config{
		Region: "us-east-1",
	}

	client, err := New(cfg)

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

	client, err := New(cfg)

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

	client, err := New(cfg)

	require.NoError(t, err)
	require.NotNil(t, client)
}
