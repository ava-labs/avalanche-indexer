package kafka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerConfig_WithDefaults_EmptyConfig(t *testing.T) {
	// Empty config should get all default values
	cfg := ConsumerConfig{}.WithDefaults()

	require.NotNil(t, cfg.SessionTimeout, "SessionTimeout should not be nil")
	assert.Equal(t, DefaultSessionTimeout, *cfg.SessionTimeout)

	require.NotNil(t, cfg.MaxPollInterval, "MaxPollInterval should not be nil")
	assert.Equal(t, DefaultMaxPollInterval, *cfg.MaxPollInterval)

	require.NotNil(t, cfg.FlushTimeout, "FlushTimeout should not be nil")
	assert.Equal(t, DefaultFlushTimeout, *cfg.FlushTimeout)

	require.NotNil(t, cfg.GoroutineWaitTimeout, "GoroutineWaitTimeout should not be nil")
	assert.Equal(t, DefaultGoroutineWaitTimeout, *cfg.GoroutineWaitTimeout)

	require.NotNil(t, cfg.PollInterval, "PollInterval should not be nil")
	assert.Equal(t, DefaultPollInterval, *cfg.PollInterval)
}

func TestConsumerConfig_WithDefaults_PartialConfig(t *testing.T) {
	customSession := 5 * time.Minute
	customFlush := 30 * time.Second

	cfg := ConsumerConfig{
		SessionTimeout: &customSession,
		FlushTimeout:   &customFlush,
		// Other timeout fields left nil
	}.WithDefaults()

	// Custom values should be preserved
	require.NotNil(t, cfg.SessionTimeout)
	assert.Equal(t, customSession, *cfg.SessionTimeout, "SessionTimeout should keep custom value")

	require.NotNil(t, cfg.FlushTimeout)
	assert.Equal(t, customFlush, *cfg.FlushTimeout, "FlushTimeout should keep custom value")

	// Missing fields should get defaults
	require.NotNil(t, cfg.MaxPollInterval)
	assert.Equal(t, DefaultMaxPollInterval, *cfg.MaxPollInterval, "MaxPollInterval should get default")

	require.NotNil(t, cfg.GoroutineWaitTimeout)
	assert.Equal(t, DefaultGoroutineWaitTimeout, *cfg.GoroutineWaitTimeout, "GoroutineWaitTimeout should get default")

	require.NotNil(t, cfg.PollInterval)
	assert.Equal(t, DefaultPollInterval, *cfg.PollInterval, "PollInterval should get default")
}

func TestConsumerConfig_WithDefaults_FullCustomConfig(t *testing.T) {
	customSession := 1 * time.Minute
	customMaxPoll := 10 * time.Minute
	customFlush := 5 * time.Second
	customGoroutineWait := 45 * time.Second
	customPoll := 200 * time.Millisecond

	cfg := ConsumerConfig{
		SessionTimeout:       &customSession,
		MaxPollInterval:      &customMaxPoll,
		FlushTimeout:         &customFlush,
		GoroutineWaitTimeout: &customGoroutineWait,
		PollInterval:         &customPoll,
	}.WithDefaults()

	// All custom values should be preserved
	require.NotNil(t, cfg.SessionTimeout)
	assert.Equal(t, customSession, *cfg.SessionTimeout)

	require.NotNil(t, cfg.MaxPollInterval)
	assert.Equal(t, customMaxPoll, *cfg.MaxPollInterval)

	require.NotNil(t, cfg.FlushTimeout)
	assert.Equal(t, customFlush, *cfg.FlushTimeout)

	require.NotNil(t, cfg.GoroutineWaitTimeout)
	assert.Equal(t, customGoroutineWait, *cfg.GoroutineWaitTimeout)

	require.NotNil(t, cfg.PollInterval)
	assert.Equal(t, customPoll, *cfg.PollInterval)
}

func TestConsumerConfig_WithDefaults_NonPointerFieldsUnaffected(t *testing.T) {
	cfg := ConsumerConfig{
		DLQTopic:                    "custom-dlq",
		Topic:                       "custom-topic",
		BootstrapServers:            "broker1:9092,broker2:9092",
		GroupID:                     "custom-group",
		AutoOffsetReset:             "latest",
		Concurrency:                 20,
		OffsetManagerCommitInterval: 15 * time.Second,
		EnableLogs:                  true,
		PublishToDLQ:                true,
	}.WithDefaults()

	// Non-pointer fields should be preserved
	assert.Equal(t, "custom-dlq", cfg.DLQTopic)
	assert.Equal(t, "custom-topic", cfg.Topic)
	assert.Equal(t, "broker1:9092,broker2:9092", cfg.BootstrapServers)
	assert.Equal(t, "custom-group", cfg.GroupID)
	assert.Equal(t, "latest", cfg.AutoOffsetReset)
	assert.Equal(t, int64(20), cfg.Concurrency)
	assert.Equal(t, 15*time.Second, cfg.OffsetManagerCommitInterval)
	assert.True(t, cfg.EnableLogs)
	assert.True(t, cfg.PublishToDLQ)

	// Pointer fields should get defaults
	require.NotNil(t, cfg.SessionTimeout)
	assert.Equal(t, DefaultSessionTimeout, *cfg.SessionTimeout)
}

func TestConsumerConfig_WithDefaults_ZeroValueTimeouts(t *testing.T) {
	// Zero-value durations (0s) are valid and should NOT be overridden
	zeroTimeout := time.Duration(0)

	cfg := ConsumerConfig{
		SessionTimeout: &zeroTimeout,
		// Other fields nil
	}.WithDefaults()

	// Zero value should be preserved (not nil, so not overridden)
	require.NotNil(t, cfg.SessionTimeout)
	assert.Equal(t, time.Duration(0), *cfg.SessionTimeout, "Zero-value timeout should be preserved")

	// Nil fields should get defaults
	require.NotNil(t, cfg.MaxPollInterval)
	assert.Equal(t, DefaultMaxPollInterval, *cfg.MaxPollInterval)
}

func TestConsumerConfig_WithDefaults_DoesNotMutateOriginal(t *testing.T) {
	original := ConsumerConfig{
		Topic: "original-topic",
		// All timeout fields nil
	}

	// Call WithDefaults
	modified := original.WithDefaults()

	// Original should remain unchanged (all timeout fields still nil)
	assert.Nil(t, original.SessionTimeout, "Original SessionTimeout should remain nil")
	assert.Nil(t, original.MaxPollInterval, "Original MaxPollInterval should remain nil")
	assert.Nil(t, original.FlushTimeout, "Original FlushTimeout should remain nil")
	assert.Nil(t, original.GoroutineWaitTimeout, "Original GoroutineWaitTimeout should remain nil")
	assert.Nil(t, original.PollInterval, "Original PollInterval should remain nil")
	assert.Equal(t, "original-topic", original.Topic)

	// Modified should have defaults
	require.NotNil(t, modified.SessionTimeout)
	assert.Equal(t, DefaultSessionTimeout, *modified.SessionTimeout)
	assert.Equal(t, "original-topic", modified.Topic)
}

func TestConsumerConfig_WithDefaults_CanBeChained(t *testing.T) {
	cfg := ConsumerConfig{
		Topic: "test-topic",
	}.WithDefaults()

	// Calling WithDefaults again should be idempotent
	cfg2 := cfg.WithDefaults()

	require.NotNil(t, cfg2.SessionTimeout)
	assert.Equal(t, *cfg.SessionTimeout, *cfg2.SessionTimeout)

	require.NotNil(t, cfg2.MaxPollInterval)
	assert.Equal(t, *cfg.MaxPollInterval, *cfg2.MaxPollInterval)

	require.NotNil(t, cfg2.FlushTimeout)
	assert.Equal(t, *cfg.FlushTimeout, *cfg2.FlushTimeout)

	require.NotNil(t, cfg2.GoroutineWaitTimeout)
	assert.Equal(t, *cfg.GoroutineWaitTimeout, *cfg2.GoroutineWaitTimeout)

	require.NotNil(t, cfg2.PollInterval)
	assert.Equal(t, *cfg.PollInterval, *cfg2.PollInterval)
}

func TestDefaultConstants(t *testing.T) {
	// Verify default constants have expected values
	assert.Equal(t, 240*time.Second, DefaultSessionTimeout, "DefaultSessionTimeout should be 240s")
	assert.Equal(t, 3400*time.Second, DefaultMaxPollInterval, "DefaultMaxPollInterval should be 3400s")
	assert.Equal(t, 15*time.Second, DefaultFlushTimeout, "DefaultFlushTimeout should be 15s")
	assert.Equal(t, 30*time.Second, DefaultGoroutineWaitTimeout, "DefaultGoroutineWaitTimeout should be 30s")
	assert.Equal(t, 100*time.Millisecond, DefaultPollInterval, "DefaultPollInterval should be 100ms")
}

func TestConsumerConfig_WithDefaults_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		input    ConsumerConfig
		validate func(t *testing.T, cfg ConsumerConfig)
	}{
		{
			name:  "empty config",
			input: ConsumerConfig{},
			validate: func(t *testing.T, cfg ConsumerConfig) {
				assert.Equal(t, DefaultSessionTimeout, *cfg.SessionTimeout)
				assert.Equal(t, DefaultMaxPollInterval, *cfg.MaxPollInterval)
				assert.Equal(t, DefaultFlushTimeout, *cfg.FlushTimeout)
				assert.Equal(t, DefaultGoroutineWaitTimeout, *cfg.GoroutineWaitTimeout)
				assert.Equal(t, DefaultPollInterval, *cfg.PollInterval)
			},
		},
		{
			name: "only session timeout set",
			input: ConsumerConfig{
				SessionTimeout: func() *time.Duration { d := 1 * time.Minute; return &d }(),
			},
			validate: func(t *testing.T, cfg ConsumerConfig) {
				assert.Equal(t, 1*time.Minute, *cfg.SessionTimeout)
				assert.Equal(t, DefaultMaxPollInterval, *cfg.MaxPollInterval)
				assert.Equal(t, DefaultFlushTimeout, *cfg.FlushTimeout)
				assert.Equal(t, DefaultGoroutineWaitTimeout, *cfg.GoroutineWaitTimeout)
				assert.Equal(t, DefaultPollInterval, *cfg.PollInterval)
			},
		},
		{
			name: "all fields custom",
			input: ConsumerConfig{
				SessionTimeout:       func() *time.Duration { d := 1 * time.Minute; return &d }(),
				MaxPollInterval:      func() *time.Duration { d := 5 * time.Minute; return &d }(),
				FlushTimeout:         func() *time.Duration { d := 20 * time.Second; return &d }(),
				GoroutineWaitTimeout: func() *time.Duration { d := 60 * time.Second; return &d }(),
				PollInterval:         func() *time.Duration { d := 50 * time.Millisecond; return &d }(),
			},
			validate: func(t *testing.T, cfg ConsumerConfig) {
				assert.Equal(t, 1*time.Minute, *cfg.SessionTimeout)
				assert.Equal(t, 5*time.Minute, *cfg.MaxPollInterval)
				assert.Equal(t, 20*time.Second, *cfg.FlushTimeout)
				assert.Equal(t, 60*time.Second, *cfg.GoroutineWaitTimeout)
				assert.Equal(t, 50*time.Millisecond, *cfg.PollInterval)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.input.WithDefaults()
			tt.validate(t, result)
		})
	}
}
