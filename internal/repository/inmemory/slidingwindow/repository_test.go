package slidingwindow

import (
	"testing"
)

func TestNewInMemorySlidingWindowRepository(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		initialLUB uint64
		initialHIB uint64
		wantLUB    uint64
		wantHIB    uint64
		wantErr    bool
	}{
		{
			name:       "HIB>=LUB keeps values",
			initialLUB: 5, initialHIB: 10,
			wantLUB: 5, wantHIB: 10,
			wantErr: false,
		},
		{
			name:       "HIB<LUB coerces HIB to LUB",
			initialLUB: 5, initialHIB: 3,
			wantLUB: 5, wantHIB: 5,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, err := New(tt.initialLUB, tt.initialHIB)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("New(%d, %d) expected error", tt.initialLUB, tt.initialHIB)
				}
				return
			}
			if err != nil {
				t.Fatalf("New(%d, %d) unexpected error: %v", tt.initialLUB, tt.initialHIB, err)
			}
			if got := r.GetLUB(); got != tt.wantLUB {
				t.Fatalf("GetLUB()=%d, want %d", got, tt.wantLUB)
			}
			if got := r.GetHIB(); got != tt.wantHIB {
				t.Fatalf("GetHIB()=%d, want %d", got, tt.wantHIB)
			}
		})
	}
}

func TestWindowAndGetters(t *testing.T) {
	t.Parallel()
	r, err := New(7, 12)
	if err != nil {
		t.Fatalf("New(7, 12) unexpected error: %v", err)
	}
	lub, hib := r.Window()
	if lub != 7 || hib != 12 {
		t.Fatalf("Window()=(%d,%d), want (7,12)", lub, hib)
	}
	if r.GetLUB() != 7 {
		t.Fatalf("GetLUB()=%d, want 7", r.GetLUB())
	}
	if r.GetHIB() != 12 {
		t.Fatalf("GetHIB()=%d, want 12", r.GetHIB())
	}
}

func TestSetHIB(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		initialLUB uint64
		initialHIB uint64
		newHIB     uint64
		wantErr    bool
		wantHIB    uint64
	}{
		{
			name:       "valid increase",
			initialLUB: 5, initialHIB: 5, newHIB: 8,
			wantErr: false, wantHIB: 8,
		},
		{
			name:       "invalid below LUB",
			initialLUB: 5, initialHIB: 7, newHIB: 3,
			wantErr: true, wantHIB: 7,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, err := New(tt.initialLUB, tt.initialHIB)
			if err != nil {
				t.Fatalf("New(%d, %d) unexpected error: %v", tt.initialLUB, tt.initialHIB, err)
			}
			err = r.SetHIB(tt.newHIB)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("SetHIB(%d) expected error", tt.newHIB)
				}
			} else if err != nil {
				t.Fatalf("SetHIB(%d) unexpected error: %v", tt.newHIB, err)
			}
			if got := r.GetHIB(); got != tt.wantHIB {
				t.Fatalf("GetHIB()=%d, want %d", got, tt.wantHIB)
			}
		})
	}
}

func TestResetLUB(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		initialLUB uint64
		initialHIB uint64
		newLUB     uint64
		mark       []uint64
		wantErr    bool
		wantLUB    uint64
	}{
		{
			name:       "move forward within HIB",
			initialLUB: 5, initialHIB: 10, newLUB: 7,
			mark:    []uint64{5, 6, 7, 8},
			wantErr: false, wantLUB: 7,
		},
		{
			name:       "move backward allowed",
			initialLUB: 5, initialHIB: 10, newLUB: 3,
			mark:    []uint64{5, 6},
			wantErr: false, wantLUB: 3,
		},
		{
			name:       "invalid above HIB",
			initialLUB: 5, initialHIB: 10, newLUB: 11,
			wantErr: true, wantLUB: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, err := New(tt.initialLUB, tt.initialHIB)
			if err != nil {
				t.Fatalf("New(%d, %d) unexpected error: %v", tt.initialLUB, tt.initialHIB, err)
			}
			for _, h := range tt.mark {
				if err := r.MarkProcessed(h); err != nil {
					t.Fatalf("MarkProcessed(%d) unexpected error: %v", h, err)
				}
			}
			err = r.ResetLUB(tt.newLUB)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ResetLUB(%d) expected error", tt.newLUB)
				}
				if r.GetLUB() != tt.initialLUB {
					t.Fatalf("LUB changed on error: got %d, want %d", r.GetLUB(), tt.initialLUB)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResetLUB(%d) unexpected error: %v", tt.newLUB, err)
			}
			if got := r.GetLUB(); got != tt.wantLUB {
				t.Fatalf("GetLUB()=%d, want %d", got, tt.wantLUB)
			}
			// Spot-check semantics after moving forward: values below LUB are implicitly processed.
			if tt.newLUB > tt.initialLUB && tt.newLUB > 0 {
				if !r.IsProcessed(tt.newLUB - 1) {
					t.Fatalf("IsProcessed(%d) expected true for < LUB", tt.newLUB-1)
				}
			}
		})
	}
}

func TestMarkProcessed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		initialLUB uint64
		initialHIB uint64
		h          uint64
		wantErr    bool
	}{
		{
			name:       "below LUB no-op",
			initialLUB: 5, initialHIB: 10, h: 4,
			wantErr: false,
		},
		{
			name:       "within window ok",
			initialLUB: 5, initialHIB: 10, h: 7,
			wantErr: false,
		},
		{
			name:       "above HIB error",
			initialLUB: 5, initialHIB: 10, h: 11,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, err := New(tt.initialLUB, tt.initialHIB)
			if err != nil {
				t.Fatalf("New(%d, %d) unexpected error: %v", tt.initialLUB, tt.initialHIB, err)
			}
			err = r.MarkProcessed(tt.h)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("MarkProcessed(%d) expected error", tt.h)
				}
				return
			}
			if err != nil {
				t.Fatalf("MarkProcessed(%d) unexpected error: %v", tt.h, err)
			}
			if tt.h >= tt.initialLUB && tt.h <= tt.initialHIB {
				if !r.IsProcessed(tt.h) {
					t.Fatalf("IsProcessed(%d)=false, want true after mark", tt.h)
				}
			}
		})
	}
}

func TestAdvanceLUB(t *testing.T) {
	t.Parallel()
	type step struct {
		marks   []uint64
		wantLUB uint64
		changed bool
	}
	tests := []struct {
		name       string
		initialLUB uint64
		initialHIB uint64
		steps      []step
	}{
		{
			name:       "no contiguous processed at LUB",
			initialLUB: 5, initialHIB: 10,
			steps: []step{
				{marks: nil, wantLUB: 5, changed: false},
			},
		},
		{
			name:       "advance through contiguous processed",
			initialLUB: 5, initialHIB: 10,
			steps: []step{
				{marks: []uint64{5, 6, 7}, wantLUB: 8, changed: true},
				{marks: []uint64{8}, wantLUB: 9, changed: true},
				{marks: nil, wantLUB: 9, changed: false},
			},
		},
		{
			name:       "gap stops advancement",
			initialLUB: 5, initialHIB: 10,
			steps: []step{
				{marks: []uint64{5, 7}, wantLUB: 6, changed: true},
				{marks: []uint64{6}, wantLUB: 8, changed: true},
			},
		},
		{
			name:       "advance beyond HIB yields no work",
			initialLUB: 5, initialHIB: 5,
			steps: []step{
				{marks: []uint64{5}, wantLUB: 6, changed: true},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, err := New(tt.initialLUB, tt.initialHIB)
			if err != nil {
				t.Fatalf("New(%d, %d) unexpected error: %v", tt.initialLUB, tt.initialHIB, err)
			}
			for _, s := range tt.steps {
				for _, h := range s.marks {
					if err := r.MarkProcessed(h); err != nil {
						t.Fatalf("MarkProcessed(%d) unexpected error: %v", h, err)
					}
				}
				gotLUB, changed := r.AdvanceLUB()
				if gotLUB != s.wantLUB || changed != s.changed {
					t.Fatalf("AdvanceLUB()=(%d,%t), want (%d,%t)", gotLUB, changed, s.wantLUB, s.changed)
				}
			}
		})
	}
}

func TestHasWork(t *testing.T) {
	t.Parallel()
	r, err := New(5, 5)
	if err != nil {
		t.Fatalf("New(5, 5) unexpected error: %v", err)
	}
	if !r.HasWork() {
		t.Fatalf("HasWork()=false, want true when LUB==HIB")
	}
	if err := r.MarkProcessed(5); err != nil {
		t.Fatalf("MarkProcessed unexpected error: %v", err)
	}
	if _, changed := r.AdvanceLUB(); !changed {
		t.Fatalf("AdvanceLUB expected to change when marking LUB")
	}
	if r.HasWork() {
		t.Fatalf("HasWork()=true, want false when LUB>HIB")
	}
}
