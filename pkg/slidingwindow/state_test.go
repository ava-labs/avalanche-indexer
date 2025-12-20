package slidingwindow

import (
	"testing"
)

func TestNewInMemorySlidingWindowRepository(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		initialLowest  uint64
		initialHighest uint64
		wantLowest     uint64
		wantHighest    uint64
		wantErr        bool
	}{
		{
			name:          "Highest>=Lowest keeps values",
			initialLowest: 5, initialHighest: 10,
			wantLowest: 5, wantHighest: 10,
			wantErr: false,
		},
		{
			name:          "Highest<Lowest coerces Highest to Lowest",
			initialLowest: 5, initialHighest: 3,
			wantLowest: 5, wantHighest: 5,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := NewState(tt.initialLowest, tt.initialHighest)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("NewState(%d, %d) expected error", tt.initialLowest, tt.initialHighest)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewState(%d, %d) unexpected error: %v", tt.initialLowest, tt.initialHighest, err)
			}
			if got := c.GetLowest(); got != tt.wantLowest {
				t.Fatalf("GetLowest()=%d, want %d", got, tt.wantLowest)
			}
			if got := c.GetHighest(); got != tt.wantHighest {
				t.Fatalf("GetHighest()=%d, want %d", got, tt.wantHighest)
			}
		})
	}
}

func TestWindowAndGetters(t *testing.T) {
	t.Parallel()
	c, err := NewState(7, 12)
	if err != nil {
		t.Fatalf("NewState(7, 12) unexpected error: %v", err)
	}
	lowest, highest := c.Window()
	if lowest != 7 || highest != 12 {
		t.Fatalf("Window()=(%d,%d), want (7,12)", lowest, highest)
	}
	if c.GetLowest() != 7 {
		t.Fatalf("GetLowest()=%d, want 7", c.GetLowest())
	}
	if c.GetHighest() != 12 {
		t.Fatalf("GetHighest()=%d, want 12", c.GetHighest())
	}
}

func TestSetHighest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		initialLowest  uint64
		initialHighest uint64
		newHighest     uint64
		wantErr        bool
		wantHighest    uint64
	}{
		{
			name:          "valid increase",
			initialLowest: 5, initialHighest: 5, newHighest: 8,
			wantErr: false, wantHighest: 8,
		},
		{
			name:          "invalid below Lowest",
			initialLowest: 5, initialHighest: 7, newHighest: 3,
			wantErr: true, wantHighest: 7,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := NewState(tt.initialLowest, tt.initialHighest)
			if err != nil {
				t.Fatalf("NewState(%d, %d) unexpected error: %v", tt.initialLowest, tt.initialHighest, err)
			}
			err = c.SetHighest(tt.newHighest)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("SetHighest(%d) expected error", tt.newHighest)
				}
			} else if err != nil {
				t.Fatalf("SetHighest(%d) unexpected error: %v", tt.newHighest, err)
			}
			if got := c.GetHighest(); got != tt.wantHighest {
				t.Fatalf("GetHighest()=%d, want %d", got, tt.wantHighest)
			}
		})
	}
}

func TestResetLowest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		initialLowest  uint64
		initialHighest uint64
		newLowest      uint64
		mark           []uint64
		wantErr        bool
		wantLowest     uint64
	}{
		{
			name:          "move forward within Highest",
			initialLowest: 5, initialHighest: 10, newLowest: 7,
			mark:    []uint64{5, 6, 7, 8},
			wantErr: false, wantLowest: 7,
		},
		{
			name:          "move backward allowed",
			initialLowest: 5, initialHighest: 10, newLowest: 3,
			mark:    []uint64{5, 6},
			wantErr: false, wantLowest: 3,
		},
		{
			name:          "invalid above Highest",
			initialLowest: 5, initialHighest: 10, newLowest: 11,
			wantErr: true, wantLowest: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := NewState(tt.initialLowest, tt.initialHighest)
			if err != nil {
				t.Fatalf("NewState(%d, %d) unexpected error: %v", tt.initialLowest, tt.initialHighest, err)
			}
			for _, h := range tt.mark {
				if err := c.MarkProcessed(h); err != nil {
					t.Fatalf("MarkProcessed(%d) unexpected error: %v", h, err)
				}
			}
			err = c.ResetLowest(tt.newLowest)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ResetLowest(%d) expected error", tt.newLowest)
				}
				if c.GetLowest() != tt.initialLowest {
					t.Fatalf("Lowest changed on error: got %d, want %d", c.GetLowest(), tt.initialLowest)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResetLowest(%d) unexpected error: %v", tt.newLowest, err)
			}
			if got := c.GetLowest(); got != tt.wantLowest {
				t.Fatalf("GetLowest()=%d, want %d", got, tt.wantLowest)
			}
			// Spot-check semantics after moving forward: values below Lowest are implicitly processed.
			if tt.newLowest > tt.initialLowest && tt.newLowest > 0 {
				if !c.IsProcessed(tt.newLowest - 1) {
					t.Fatalf("IsProcessed(%d) expected true for < Lowest", tt.newLowest-1)
				}
			}
		})
	}
}

func TestMarkProcessed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		initialLowest  uint64
		initialHighest uint64
		h              uint64
		wantErr        bool
	}{
		{
			name:          "below Lowest no-op",
			initialLowest: 5, initialHighest: 10, h: 4,
			wantErr: false,
		},
		{
			name:          "within window ok",
			initialLowest: 5, initialHighest: 10, h: 7,
			wantErr: false,
		},
		{
			name:          "above Highest error",
			initialLowest: 5, initialHighest: 10, h: 11,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := NewState(tt.initialLowest, tt.initialHighest)
			if err != nil {
				t.Fatalf("NewState(%d, %d) unexpected error: %v", tt.initialLowest, tt.initialHighest, err)
			}
			err = c.MarkProcessed(tt.h)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("MarkProcessed(%d) expected error", tt.h)
				}
				return
			}
			if err != nil {
				t.Fatalf("MarkProcessed(%d) unexpected error: %v", tt.h, err)
			}
			if tt.h >= tt.initialLowest && tt.h <= tt.initialHighest {
				if !c.IsProcessed(tt.h) {
					t.Fatalf("IsProcessed(%d)=false, want true after mark", tt.h)
				}
			}
		})
	}
}

func TestAdvanceLowest(t *testing.T) {
	t.Parallel()
	type step struct {
		marks      []uint64
		wantLowest uint64
		changed    bool
	}
	tests := []struct {
		name           string
		initialLowest  uint64
		initialHighest uint64
		steps          []step
	}{
		{
			name:          "no contiguous processed at Lowest",
			initialLowest: 5, initialHighest: 10,
			steps: []step{
				{marks: nil, wantLowest: 5, changed: false},
			},
		},
		{
			name:          "advance through contiguous processed",
			initialLowest: 5, initialHighest: 10,
			steps: []step{
				{marks: []uint64{5, 6, 7}, wantLowest: 8, changed: true},
				{marks: []uint64{8}, wantLowest: 9, changed: true},
				{marks: nil, wantLowest: 9, changed: false},
			},
		},
		{
			name:          "gap stops advancement",
			initialLowest: 5, initialHighest: 10,
			steps: []step{
				{marks: []uint64{5, 7}, wantLowest: 6, changed: true},
				{marks: []uint64{6}, wantLowest: 8, changed: true},
			},
		},
		{
			name:          "advance beyond Highest yields no work",
			initialLowest: 5, initialHighest: 5,
			steps: []step{
				{marks: []uint64{5}, wantLowest: 6, changed: true},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := NewState(tt.initialLowest, tt.initialHighest)
			if err != nil {
				t.Fatalf("NewState(%d, %d) unexpected error: %v", tt.initialLowest, tt.initialHighest, err)
			}
			for _, s := range tt.steps {
				for _, h := range s.marks {
					if err := c.MarkProcessed(h); err != nil {
						t.Fatalf("MarkProcessed(%d) unexpected error: %v", h, err)
					}
				}
				gotLowest, changed := c.AdvanceLowest()
				if gotLowest != s.wantLowest || changed != s.changed {
					t.Fatalf("AdvanceLowest()=(%d,%t), want (%d,%t)", gotLowest, changed, s.wantLowest, s.changed)
				}
			}
		})
	}
}

func TestHasWork(t *testing.T) {
	t.Parallel()
	c, err := NewState(5, 5)
	if err != nil {
		t.Fatalf("NewState(5, 5) unexpected error: %v", err)
	}
	if !c.HasWork() {
		t.Fatalf("HasWork()=false, want true when Lowest==Highest")
	}
	if err := c.MarkProcessed(5); err != nil {
		t.Fatalf("MarkProcessed unexpected error: %v", err)
	}
	if _, changed := c.AdvanceLowest(); !changed {
		t.Fatalf("AdvanceLowest expected to change when marking Lowest")
	}
	if c.HasWork() {
		t.Fatalf("HasWork()=true, want false when Lowest>Highest")
	}
}
