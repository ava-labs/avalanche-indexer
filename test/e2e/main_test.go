//go:build e2e

package e2e

import (
	"os"
	"testing"

	"github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
)

// TestMain registers the coreth libevm type extras for the whole e2e binary.
//
// Registration mutates process-global state in libevm and must happen exactly once
// before any block is decoded. cmd/blockfetcher does this for the real binary, but these
// tests construct workers directly, so they must register it themselves; without it
// ethclient panics while attaching block extras.
//
// This must live in a _test.go file: the go tool only recognises TestMain in test files,
// and would silently ignore it anywhere else.
//
// Only coreth is registered because the e2e suite does not exercise Subnet-EVM, and the
// two register incompatible payload types.
func TestMain(m *testing.M) {
	customtypes.Register()
	os.Exit(m.Run())
}
