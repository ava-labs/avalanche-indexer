package icmrepo

import (
	"math/big"
	"time"
)

// SendEventRow represents a row in the icm_send_events table.
type SendEventRow struct {
	BlockchainID             string
	EVMChainID               *big.Int // UInt256 in ClickHouse
	BlockNumber              uint64
	BlockTime                time.Time
	TxHash                   string // hex, stored as FixedString(32)
	TxIndex                  uint32
	LogIndex                 uint32
	ContractAddress          string // hex, stored as FixedString(20)
	MessageID                string // hex, stored as FixedString(32)
	DestinationBlockchainID  string
	SenderAddress            string   // hex, stored as FixedString(20)
	DestinationAddress       string   // hex, stored as FixedString(20)
	RequiredGasLimit         *big.Int // UInt256 in ClickHouse
	AllowedRelayerAddresses  []string // hex addresses, stored as Array(String)
	FeeTokenAddress          string   // hex, stored as FixedString(20)
	FeeAmount                *big.Int // UInt256 in ClickHouse
	MessageNonce             *big.Int // UInt256 in ClickHouse
	MessageData              []byte   // stored as String
	ReceiptsMessageNonces    []*big.Int
	ReceiptsRelayerAddresses []string // hex addresses, stored as Array(FixedString(20))
}

// ReceiveEventRow represents a row in the icm_receive_events table.
type ReceiveEventRow struct {
	BlockchainID             string
	EVMChainID               *big.Int
	BlockNumber              uint64
	BlockTime                time.Time
	TxHash                   string
	TxIndex                  uint32
	LogIndex                 uint32
	ContractAddress          string
	MessageID                string
	SourceBlockchainID       string
	DelivererAddress         string
	RewardRedeemerAddress    string
	MessageNonce             *big.Int
	OriginSenderAddress      string
	DestinationBlockchainID  string
	DestinationAddress       string
	RequiredGasLimit         *big.Int
	AllowedRelayerAddresses  []string
	MessageData              []byte
	ReceiptsMessageNonces    []*big.Int
	ReceiptsRelayerAddresses []string
}

// MessageExecutedEventRow represents a row in the icm_message_executed_events table.
type MessageExecutedEventRow struct {
	BlockchainID       string
	EVMChainID         *big.Int
	BlockNumber        uint64
	BlockTime          time.Time
	TxHash             string
	TxIndex            uint32
	LogIndex           uint32
	ContractAddress    string
	MessageID          string
	SourceBlockchainID string
}

// MessageExecutionFailedEventRow represents a row in the icm_message_execution_failed_events table.
type MessageExecutionFailedEventRow struct {
	BlockchainID             string
	EVMChainID               *big.Int
	BlockNumber              uint64
	BlockTime                time.Time
	TxHash                   string
	TxIndex                  uint32
	LogIndex                 uint32
	ContractAddress          string
	MessageID                string
	SourceBlockchainID       string
	MessageNonce             *big.Int
	OriginSenderAddress      string
	DestinationBlockchainID  string
	DestinationAddress       string
	RequiredGasLimit         *big.Int
	AllowedRelayerAddresses  []string
	MessageData              []byte
	ReceiptsMessageNonces    []*big.Int
	ReceiptsRelayerAddresses []string
}

// ReceiptEventRow represents a row in the icm_receipt_events table.
type ReceiptEventRow struct {
	BlockchainID            string
	EVMChainID              *big.Int
	BlockNumber             uint64
	BlockTime               time.Time
	TxHash                  string
	TxIndex                 uint32
	LogIndex                uint32
	ContractAddress         string
	MessageID               string
	DestinationBlockchainID string
	RelayerRewardAddress    string
	FeeTokenAddress         string
	FeeAmount               *big.Int
}

// AddFeeEventRow represents a row in the icm_add_fee_events table.
type AddFeeEventRow struct {
	BlockchainID            string
	EVMChainID              *big.Int
	BlockNumber             uint64
	BlockTime               time.Time
	TxHash                  string
	TxIndex                 uint32
	LogIndex                uint32
	ContractAddress         string
	MessageID               string
	DestinationBlockchainID string
	FeeTokenAddress         string
	AdditionalFeeAmount     *big.Int
}

// RelayerRewardRedeemedEventRow represents a row in the icm_relayer_reward_redeemed_events table.
type RelayerRewardRedeemedEventRow struct {
	BlockchainID    string
	EVMChainID      *big.Int
	BlockNumber     uint64
	BlockTime       time.Time
	TxHash          string
	TxIndex         uint32
	LogIndex        uint32
	ContractAddress string
	RedeemerAddress string
	FeeTokenAddress string
	Amount          *big.Int
}

// MessagePartialSendRow carries the fields written to messages by the
// SendCrossChainMessage handler. Only send-side columns are populated; all
// receive/execute fields are left at their zero value so AggregatingMergeTree
// merges them correctly when the destination-chain partial row arrives.
type MessagePartialSendRow struct {
	SourceBlockchainID      string
	DestinationBlockchainID string
	MessageID               string
	SourceBlockTime         time.Time
	SourceTxHash            string
	EVMChainID              *big.Int
	ContractAddress         string
	MessageNonce            *big.Int
	SenderAddress           string
	DestinationAddress      string
	RequiredGasLimit        *big.Int
	AllowedRelayerAddresses []string
	FeeTokenAddress         string
	FeeAmount               *big.Int
	MessageData             string // serialised; stored as String in ClickHouse
	// SourceGasSpent is the full transaction gas cost (effectiveGasPrice × gasUsed), not
	// the cost attributable to this send alone. A single transaction can emit multiple
	// SendCrossChainMessage events (e.g. via a batch-dispatch contract), so any aggregation
	// of this field must deduplicate on TxHash first to avoid overcounting.
	SourceGasSpent  *big.Int // nullable UInt256
	MessageReceipts string   // serialised; stored as String in ClickHouse
}

// MessagePartialReceiveRow carries the fields written to messages by the
// ReceiveCrossChainMessage handler.
type MessagePartialReceiveRow struct {
	SourceBlockchainID      string
	DestinationBlockchainID string
	MessageID               string
	ReceiveBlockTime        time.Time
	ReceiveTxHash           string
	DelivererAddress        string
	RewardRedeemerAddress   string
	DestinationEVMChainID   *big.Int
	// DestinationGasSpent is the full transaction gas cost, not per-message. A single
	// transaction can deliver multiple messages, so aggregations must deduplicate on
	// ReceiveTxHash first to avoid overcounting.
	DestinationGasSpent *big.Int
}

// MessagePartialExecutedRow carries the fields written to messages by the
// MessageExecuted handler.
type MessagePartialExecutedRow struct {
	SourceBlockchainID      string
	DestinationBlockchainID string
	MessageID               string
	ExecutedBlockTime       time.Time
	ExecutedTxHash          string
}

// MessagePartialExecutionFailedRow carries the fields written to messages
// by the MessageExecutionFailed handler.
type MessagePartialExecutionFailedRow struct {
	SourceBlockchainID      string
	DestinationBlockchainID string
	MessageID               string
	LastExecutionFailedTime time.Time
}

// MessagePartialReceiptRow carries the fields written to messages by the
// ReceiptReceived handler.
type MessagePartialReceiptRow struct {
	SourceBlockchainID      string
	DestinationBlockchainID string
	MessageID               string
	ReceiptDelivered        uint8
}
