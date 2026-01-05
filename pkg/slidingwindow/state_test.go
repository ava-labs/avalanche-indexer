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

func TestGetters(t *testing.T) {
	t.Parallel()
	c, err := NewState(7, 12)
	if err != nil {
		t.Fatalf("NewState(7, 12) unexpected error: %v", err)
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
		initialHighest uint64
		newHighest     uint64
		heightSet      bool
		wantHighest    uint64
	}{
		{
			name:           "valid increase",
			initialHighest: 5, newHighest: 8,
			heightSet: true, wantHighest: 8,
		},
		{
			name:           "invalid below highest",
			initialHighest: 7, newHighest: 3,
			heightSet: false, wantHighest: 7,
		},
		{
			name:           "invalid equal to highest",
			initialHighest: 7, newHighest: 7,
			heightSet: false, wantHighest: 7,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := NewState(0, tt.initialHighest)
			if err != nil {
				t.Fatalf("NewState(0, %d) unexpected error: %v", tt.initialHighest, err)
			}
			ok := c.SetHighest(tt.newHighest)
			if tt.heightSet {
				if !ok {
					t.Fatalf("SetHighest(%d) expected true, got false", tt.newHighest)
				}
			} else {
				if ok {
					t.Fatalf("SetHighest(%d) expected false, got true", tt.newHighest)
				}
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

func TestFindNextUnclaimedBlock(t *testing.T) {
	t.Parallel()
	type fields struct {
		lowest    uint64
		highest   uint64
		processed []uint64
		inflight  []uint64
	}
	type want struct {
		height uint64
		ok     bool
	}
	tests := []struct {
		name   string
		fields fields
		want   want
	}{
		{
			name: "returns first unprocessed and not inflight",
			fields: fields{
				lowest: 5, highest: 7,
				processed: []uint64{5},
			},
			want: want{height: 6, ok: true},
		},
		{
			name: "skips inflight heights",
			fields: fields{
				lowest: 5, highest: 7,
				inflight: []uint64{5},
			},
			want: want{height: 6, ok: true},
		},
		{
			name: "all processed returns none",
			fields: fields{
				lowest: 5, highest: 7,
				processed: []uint64{5, 6, 7},
			},
			want: want{height: 0, ok: false},
		},
		{
			name: "single height available",
			fields: fields{
				lowest: 10, highest: 10,
			},
			want: want{height: 10, ok: true},
		},
		{
			name: "single height inflight returns none",
			fields: fields{
				lowest: 10, highest: 10,
				inflight: []uint64{10},
			},
			want: want{height: 0, ok: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, err := NewState(tt.fields.lowest, tt.fields.highest)
			if err != nil {
				t.Fatalf("New state error: %v", err)
			}
			for _, h := range tt.fields.processed {
				if err := state.MarkProcessed(h); err != nil {
					t.Fatalf("MarkProcessed(%d) error: %v", h, err)
				}
			}
			for _, h := range tt.fields.inflight {
				_ = state.TrySetInflight(h)
			}

			gotH, gotOK := state.FindNextUnclaimedHeight()
			if gotH != tt.want.height || gotOK != tt.want.ok {
				t.Fatalf("FindNextUnclaimedBlock()=(%d,%t), want (%d,%t)", gotH, gotOK, tt.want.height, tt.want.ok)
			}
		})
	}
}

func TestFindAndSetNextInflight(t *testing.T) {
	t.Parallel()
	type fields struct {
		lowest    uint64
		highest   uint64
		processed []uint64
		inflight  []uint64
	}
	type want struct {
		height   uint64
		ok       bool
		inflight []uint64 // expected inflight after the call
	}
	tests := []struct {
		name   string
		fields fields
		want   want
	}{
		{
			name: "single available height claims and marks inflight",
			fields: fields{
				lowest: 10, highest: 10,
			},
			want: want{height: 10, ok: true, inflight: []uint64{10}},
		},
		{
			name: "single processed height returns none",
			fields: fields{
				lowest: 10, highest: 10,
				processed: []uint64{10},
			},
			want: want{height: 0, ok: false, inflight: nil},
		},
		{
			name: "single inflight height returns none",
			fields: fields{
				lowest: 10, highest: 10,
				inflight: []uint64{10},
			},
			want: want{height: 0, ok: false, inflight: []uint64{10}},
		},
		{
			name: "skips processed at lowest and claims next",
			fields: fields{
				lowest: 5, highest: 7,
				processed: []uint64{5},
			},
			want: want{height: 6, ok: true, inflight: []uint64{6}},
		},
		{
			name: "skips inflight at start and claims next",
			fields: fields{
				lowest: 5, highest: 7,
				inflight: []uint64{5},
			},
			want: want{height: 6, ok: true, inflight: []uint64{5, 6}},
		},
		{
			name: "all heights blocked (processed) returns none",
			fields: fields{
				lowest: 5, highest: 7,
				processed: []uint64{5, 6, 7},
			},
			want: want{height: 0, ok: false, inflight: nil},
		},
		{
			name: "all heights blocked (inflight) returns none",
			fields: fields{
				lowest: 5, highest: 7,
				inflight: []uint64{5, 6, 7},
			},
			want: want{height: 0, ok: false, inflight: []uint64{5, 6, 7}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state, err := NewState(tt.fields.lowest, tt.fields.highest)
			if err != nil {
				t.Fatalf("New state error: %v", err)
			}
			for _, h := range tt.fields.processed {
				if err := state.MarkProcessed(h); err != nil {
					t.Fatalf("MarkProcessed(%d) error: %v", h, err)
				}
			}
			for _, h := range tt.fields.inflight {
				if ok := state.TrySetInflight(h); !ok {
					t.Fatalf("failed to seed inflight for %d", h)
				}
			}

			origLowest := state.GetLowest()
			origHighest := state.GetHighest()

			gotH, gotOK := state.FindAndSetNextInflight()
			if gotH != tt.want.height || gotOK != tt.want.ok {
				t.Fatalf("FindAndSetNextInflight()=(%d,%t), want (%d,%t)", gotH, gotOK, tt.want.height, tt.want.ok)
			}

			// Verify inflight set membership after the call
			for _, h := range tt.want.inflight {
				if !state.IsInflight(h) {
					t.Fatalf("IsInflight(%d)=false, want true", h)
				}
			}
			// Ensure no unexpected inflight when none expected
			if tt.want.inflight == nil && gotOK {
				if !state.IsInflight(gotH) {
					t.Fatalf("claimed height %d should be inflight", gotH)
				}
			}
			// Lowest/Highest should not change
			if state.GetLowest() != origLowest || state.GetHighest() != origHighest {
				t.Fatalf("watermarks changed: got (lowest=%d, highest=%d), want (lowest=%d, highest=%d)",
					state.GetLowest(), state.GetHighest(), origLowest, origHighest)
			}
		})
	}

	t.Run("sequential claims across window", func(t *testing.T) {
		t.Parallel()
		state, err := NewState(5, 7)
		if err != nil {
			t.Fatalf("New state error: %v", err)
		}
		// First claim: 5
		if h, ok := state.FindAndSetNextInflight(); !ok || h != 5 {
			t.Fatalf("first claim=(%d,%t), want (5,true)", h, ok)
		}
		// Second claim: 6
		if h, ok := state.FindAndSetNextInflight(); !ok || h != 6 {
			t.Fatalf("second claim=(%d,%t), want (6,true)", h, ok)
		}
		// Third claim: 7
		if h, ok := state.FindAndSetNextInflight(); !ok || h != 7 {
			t.Fatalf("third claim=(%d,%t), want (7,true)", h, ok)
		}
		// Fourth claim: none left
		if h, ok := state.FindAndSetNextInflight(); ok || h != 0 {
			t.Fatalf("fourth claim=(%d,%t), want (0,false)", h, ok)
		}
		// Watermarks unchanged
		if state.GetLowest() != 5 || state.GetHighest() != 7 {
			t.Fatalf("watermarks changed unexpectedly: lowest=%d highest=%d", state.GetLowest(), state.GetHighest())
		}
	})
}

func TestTrySetInflight(t *testing.T) {
	t.Parallel()

	type step struct {
		height       uint64
		value        bool
		expectOk     bool
		wantInFlight bool
	}
	tests := []struct {
		name      string
		initial   map[uint64]bool
		processed []uint64
		steps     []step
	}{
		{
			name:      "set inflight height < lowest is no-op",
			initial:   map[uint64]bool{},
			processed: []uint64{},
			steps: []step{
				{height: 3, value: true, expectOk: false, wantInFlight: false},
			},
		},
		{
			name:      "set inflight height > highest is no-op",
			initial:   map[uint64]bool{},
			processed: []uint64{},
			steps: []step{
				{height: 110, value: true, expectOk: false, wantInFlight: false},
			},
		},
		{
			name: "set inflight height already inflight is no-op",
			initial: map[uint64]bool{
				10: true,
			},
			processed: []uint64{},
			steps: []step{
				{height: 10, value: true, expectOk: false, wantInFlight: true},
			},
		},
		{
			name:      "set inflight height already processed is no-op",
			initial:   map[uint64]bool{},
			processed: []uint64{10},
			steps: []step{
				{height: 10, value: true, expectOk: false, wantInFlight: false},
			},
		},
		{
			name:    "set inflight correct height (mid-window)",
			initial: map[uint64]bool{},
			steps: []step{
				{height: 10, value: true, expectOk: true, wantInFlight: true},
			},
		},
		{
			name:    "set inflight at lowest boundary",
			initial: map[uint64]bool{},
			steps: []step{
				{height: 5, value: true, expectOk: true, wantInFlight: true},
			},
		},
		{
			name:    "set inflight at highest boundary",
			initial: map[uint64]bool{},
			steps: []step{
				{height: 100, value: true, expectOk: true, wantInFlight: true},
			},
		},
		{
			name:    "toggle inflight height",
			initial: map[uint64]bool{},
			steps: []step{
				{height: 12, value: true, expectOk: true, wantInFlight: true},
				{height: 12, value: false, wantInFlight: false},
				{height: 12, value: true, expectOk: true, wantInFlight: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use a wide window so all tested heights are in-range for TrySetInflight.
			state, err := NewState(5, 100)
			if err != nil {
				t.Fatalf("New state error: %v", err)
			}
			// Seed initial inflight map
			for h, v := range tt.initial {
				if v {
					if ok := state.TrySetInflight(h); !ok {
						t.Fatalf("failed to seed inflight for height %d", h)
					}
				} else {
					state.UnsetInflight(h)
				}
			}

			for _, h := range tt.processed {
				if err := state.MarkProcessed(h); err != nil {
					t.Fatalf("MarkProcessed(%d) error: %v", h, err)
				}
			}

			// Execute steps
			for _, s := range tt.steps {
				if s.value {
					ok := state.TrySetInflight(s.height)
					if ok != s.expectOk {
						t.Fatalf("TrySetInflight(%d) ok=%t, want %t", s.height, ok, s.expectOk)
					}
				} else {
					state.UnsetInflight(s.height)
				}
				got := state.IsInflight(s.height)
				if got != s.wantInFlight {
					t.Fatalf("after TrySetInflight/UnsetInflight(%d,%t): isInflight=%t, want %t", s.height, s.value, got, s.wantInFlight)
				}
			}
		})
	}
}
