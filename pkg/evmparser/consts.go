package evmparser

// ERC event topic0 hashes used to identify transfer types from receipt logs.
const (
	// TransferMethodHash is keccak256("Transfer(address,address,uint256)")
	// Used by both ERC-20 (3 topics) and ERC-721 (4 topics).
	TransferMethodHash = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

	// TransferSingleMethodHash is keccak256("TransferSingle(address,address,address,uint256,uint256)")
	TransferSingleMethodHash = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"

	// TransferBatchMethodHash is keccak256("TransferBatch(address,address,address,uint256[],uint256[])")
	TransferBatchMethodHash = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"

	// WrappedDepositMethodHash is keccak256("Deposit(address,uint256)")
	WrappedDepositMethodHash = "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"

	// WrappedWithdrawalMethodHash is keccak256("Withdrawal(address,uint256)")
	WrappedWithdrawalMethodHash = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
)

// ERCType represents the type of ERC token standard.
type ERCType string

const (
	ERC20   ERCType = "ERC20"
	ERC721  ERCType = "ERC721"
	ERC1155 ERCType = "ERC1155"
)

// Avalanche-specific chain IDs and gas price constants.
const (
	MainnetChainID = uint64(43114)
	TestnetChainID = uint64(43113)
	DevnetChainID  = uint64(43117)

	// Historical gas price values for pre-ApricotPhase3 baseFee defaults.
	LaunchMinGasPrice        = int64(470_000_000_000) // 470 gwei
	ApricotPhase1MinGasPrice = int64(25_000_000_000)  // 25 gwei

	FujiApricotPhase1BlockTimestamp    = uint64(1630348800)
	MainnetApricotPhase1BlockTimestamp = uint64(1630348800)
	DevnetApricotPhase1BlockTimestamp  = uint64(1630348800)
)
