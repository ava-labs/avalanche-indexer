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

	assert.Equal(t, DefaultRetryBaseDelay, cfg.Retry.BaseDelay)
	assert.Equal(t, DefaultRetryMaxDelay, cfg.Retry.MaxDelay)
	assert.Equal(t, DefaultMessageMaxBytes, cfg.MessageMaxBytes)
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
	assert.Equal(t, 500*time.Millisecond, DefaultRetryBaseDelay, "DefaultRetryBaseDelay should be 500ms")
	assert.Equal(t, 2*time.Second, DefaultRetryMaxDelay, "DefaultRetryMaxDelay should be 2s")
	assert.Equal(t, -1, InfiniteRetries, "InfiniteRetries should be -1")
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
				assert.Equal(t, DefaultRetryBaseDelay, cfg.Retry.BaseDelay)
				assert.Equal(t, DefaultRetryMaxDelay, cfg.Retry.MaxDelay)
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
				assert.Equal(t, DefaultRetryBaseDelay, cfg.Retry.BaseDelay)
				assert.Equal(t, DefaultRetryMaxDelay, cfg.Retry.MaxDelay)
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
				Retry: RetryPolicy{
					MaxRetries: 5,
					BaseDelay:  1 * time.Second,
					MaxDelay:   30 * time.Second,
				},
			},
			validate: func(t *testing.T, cfg ConsumerConfig) {
				assert.Equal(t, 1*time.Minute, *cfg.SessionTimeout)
				assert.Equal(t, 5*time.Minute, *cfg.MaxPollInterval)
				assert.Equal(t, 20*time.Second, *cfg.FlushTimeout)
				assert.Equal(t, 60*time.Second, *cfg.GoroutineWaitTimeout)
				assert.Equal(t, 50*time.Millisecond, *cfg.PollInterval)
				assert.Equal(t, 5, cfg.Retry.MaxRetries)
				assert.Equal(t, 1*time.Second, cfg.Retry.BaseDelay)
				assert.Equal(t, 30*time.Second, cfg.Retry.MaxDelay)
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

func TestRetryPolicy_ShouldRetry(t *testing.T) {
	tests := []struct {
		name       string
		policy     RetryPolicy
		attempts   []int
		wantResult []bool
	}{
		{
			name:       "disabled (MaxRetries=0) never allows retry",
			policy:     RetryPolicy{MaxRetries: 0},
			attempts:   []int{0, 1, 5},
			wantResult: []bool{false, false, false},
		},
		{
			name:       "finite retries allows up to MaxRetries",
			policy:     RetryPolicy{MaxRetries: 3},
			attempts:   []int{0, 1, 2, 3, 4, 100},
			wantResult: []bool{true, true, true, false, false, false},
		},
		{
			name:       "single retry",
			policy:     RetryPolicy{MaxRetries: 1},
			attempts:   []int{0, 1, 2},
			wantResult: []bool{true, false, false},
		},
		{
			name:       "infinite retries always allows retry",
			policy:     RetryPolicy{MaxRetries: InfiniteRetries},
			attempts:   []int{0, 1, 100, 1000, 1_000_000},
			wantResult: []bool{true, true, true, true, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, len(tt.attempts), len(tt.wantResult), "test setup: attempts and wantResult must have same length")
			for i, attempt := range tt.attempts {
				assert.Equal(t, tt.wantResult[i], tt.policy.ShouldRetry(attempt),
					"ShouldRetry(%d)", attempt)
			}
		})
	}
}

func TestRetryPolicy_Backoff(t *testing.T) {
	tests := []struct {
		name     string
		policy   RetryPolicy
		attempt  int
		expected time.Duration
	}{
		{
			name:     "attempt 0 returns BaseDelay",
			policy:   RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: 10 * time.Second},
			attempt:  0,
			expected: 100 * time.Millisecond,
		},
		{
			name:     "attempt 1 doubles BaseDelay",
			policy:   RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: 10 * time.Second},
			attempt:  1,
			expected: 200 * time.Millisecond,
		},
		{
			name:     "attempt 2 quadruples BaseDelay",
			policy:   RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: 10 * time.Second},
			attempt:  2,
			expected: 400 * time.Millisecond,
		},
		{
			name:     "attempt 3 is 8x BaseDelay",
			policy:   RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: 10 * time.Second},
			attempt:  3,
			expected: 800 * time.Millisecond,
		},
		{
			name:     "clamped at MaxDelay",
			policy:   RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: 500 * time.Millisecond},
			attempt:  5, // 2^5 * 100ms = 3200ms > 500ms
			expected: 500 * time.Millisecond,
		},
		{
			name:     "exactly at MaxDelay boundary",
			policy:   RetryPolicy{BaseDelay: 1 * time.Second, MaxDelay: 8 * time.Second},
			attempt:  3, // 2^3 * 1s = 8s == MaxDelay
			expected: 8 * time.Second,
		},
		{
			name:     "zero BaseDelay returns zero",
			policy:   RetryPolicy{BaseDelay: 0, MaxDelay: 10 * time.Second},
			attempt:  5,
			expected: 0,
		},
		{
			name:     "negative BaseDelay returns zero",
			policy:   RetryPolicy{BaseDelay: -1 * time.Second, MaxDelay: 10 * time.Second},
			attempt:  0,
			expected: 0,
		},
		{
			name:     "large attempt does not overflow",
			policy:   RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: 5 * time.Minute},
			attempt:  50, // capped at shift=30 internally
			expected: 5 * time.Minute,
		},
		{
			name:     "attempt at shift cap (30) unclamped",
			policy:   RetryPolicy{BaseDelay: 1 * time.Nanosecond, MaxDelay: 2 * time.Second},
			attempt:  30, // 2^30 * 1ns = 1073741824ns ≈ 1.07s < 2s
			expected: 1073741824 * time.Nanosecond,
		},
		{
			name:     "attempt beyond shift cap (31) same as 30",
			policy:   RetryPolicy{BaseDelay: 1 * time.Nanosecond, MaxDelay: 2 * time.Second},
			attempt:  31, // shift capped at 30: 2^30 * 1ns
			expected: 1073741824 * time.Nanosecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.policy.Backoff(tt.attempt)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestRetryPolicy_Backoff_MonotonicallyIncreasing(t *testing.T) {
	policy := RetryPolicy{BaseDelay: 50 * time.Millisecond, MaxDelay: 30 * time.Second}

	var prev time.Duration
	for attempt := 0; attempt < 20; attempt++ {
		got := policy.Backoff(attempt)
		assert.GreaterOrEqual(t, got, prev,
			"backoff should be monotonically non-decreasing: attempt %d gave %v, previous was %v", attempt, got, prev)
		assert.LessOrEqual(t, got, policy.MaxDelay,
			"backoff should never exceed MaxDelay: attempt %d gave %v", attempt, got)
		prev = got
	}
}

func TestConsumerConfig_WithDefaults_RetryDefaults(t *testing.T) {
	cfg := ConsumerConfig{}.WithDefaults()

	assert.Equal(t, DefaultRetryBaseDelay, cfg.Retry.BaseDelay, "empty Retry.BaseDelay should get default")
	assert.Equal(t, DefaultRetryMaxDelay, cfg.Retry.MaxDelay, "empty Retry.MaxDelay should get default")
	assert.Equal(t, 0, cfg.Retry.MaxRetries, "MaxRetries zero-value means disabled")
}

func TestConsumerConfig_WithDefaults_RetryCustomPreserved(t *testing.T) {
	cfg := ConsumerConfig{
		Retry: RetryPolicy{
			MaxRetries: InfiniteRetries,
			BaseDelay:  1 * time.Second,
			MaxDelay:   5 * time.Minute,
		},
	}.WithDefaults()

	assert.Equal(t, InfiniteRetries, cfg.Retry.MaxRetries, "custom MaxRetries should be preserved")
	assert.Equal(t, 1*time.Second, cfg.Retry.BaseDelay, "custom BaseDelay should be preserved")
	assert.Equal(t, 5*time.Minute, cfg.Retry.MaxDelay, "custom MaxDelay should be preserved")
}

func TestConsumerConfig_WithDefaults_RetryPartialCustom(t *testing.T) {
	cfg := ConsumerConfig{
		Retry: RetryPolicy{
			MaxRetries: 3,
			BaseDelay:  2 * time.Second,
			// MaxDelay left as zero → should get default
		},
	}.WithDefaults()

	assert.Equal(t, 3, cfg.Retry.MaxRetries)
	assert.Equal(t, 2*time.Second, cfg.Retry.BaseDelay, "custom BaseDelay should be preserved")
	assert.Equal(t, DefaultRetryMaxDelay, cfg.Retry.MaxDelay, "zero MaxDelay should get default")
}

func TestConsumerConfig_WithDefaults_DoesNotMutateOriginalRetry(t *testing.T) {
	original := ConsumerConfig{
		Retry: RetryPolicy{MaxRetries: 5},
	}

	modified := original.WithDefaults()

	assert.Equal(t, time.Duration(0), original.Retry.BaseDelay, "original BaseDelay should remain zero")
	assert.Equal(t, time.Duration(0), original.Retry.MaxDelay, "original MaxDelay should remain zero")
	assert.Equal(t, DefaultRetryBaseDelay, modified.Retry.BaseDelay, "modified should have default BaseDelay")
	assert.Equal(t, DefaultRetryMaxDelay, modified.Retry.MaxDelay, "modified should have default MaxDelay")
	assert.Equal(t, 5, modified.Retry.MaxRetries, "MaxRetries should be preserved")
}
