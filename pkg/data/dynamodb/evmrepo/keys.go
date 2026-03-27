package evmrepo

import (
	"fmt"
	"math/big"
)

// Key padding functions — must match analytics/recorder/utils.go exactly
// to produce DynamoDB keys compatible with glacier-api queries.

// PadBlockNumber formats a block number as a 16-character lowercase hex string.
func PadBlockNumber(blockNumber uint64) string {
	return fmt.Sprintf("%016x", blockNumber)
}

// PadTxIndex formats a transaction index as an 8-character lowercase hex string.
func PadTxIndex(txIndex uint) string {
	return fmt.Sprintf("%08x", txIndex)
}

// PadLogIndex formats a log index as an 8-character lowercase hex string.
func PadLogIndex(logIndex uint) string {
	return fmt.Sprintf("%08x", logIndex)
}

// PadTransferIndex formats a transfer index as an 8-character lowercase hex string.
func PadTransferIndex(transferIndex uint) string {
	return fmt.Sprintf("%08x", transferIndex)
}

// PadProcessIndex formats an internal tx process index as an 8-character lowercase hex string.
func PadProcessIndex(processIndex uint) string {
	return fmt.Sprintf("%08x", processIndex)
}

// PadTokenID formats a token ID as a 64-character hex string for DynamoDB sort keys.
func PadTokenID(tokenID *big.Int) string {
	if tokenID == nil {
		return fmt.Sprintf("%064s", "0")
	}
	return fmt.Sprintf("%064s", tokenID.Text(16))
}

// Block key prefixes — match glacier-api's query patterns.
const (
	BlockPKPrefix = "B#"
	TxPKPrefix    = "T#"

	NativeTxTypePrefix  = "N#"
	ERC20TxTypePrefix   = "20#"
	ERC721TxTypePrefix  = "721#"
	ERC1155TxTypePrefix = "1155#"
	InternalTxPrefix    = "I#"

	AddressInteractionPrefix = "I#"
	AddressReceivablePrefix  = "A#"

	BlockSKPrefix = "B#"
	NativeSKPrefix = "N#"
)

// BlockPK returns the partition key for a block history record: B#{blockHash}
func BlockPK(blockHash string) string {
	return BlockPKPrefix + blockHash
}

// BlockSK returns the sort key for a block history record: {paddedBlockNumber}
func BlockSK(blockNumber uint64) string {
	return PadBlockNumber(blockNumber)
}

// BlockSKValue returns the blockSk GSI value: B#{paddedBlockNumber}
func BlockSKValue(blockNumber uint64) string {
	return BlockSKPrefix + PadBlockNumber(blockNumber)
}

// NativeTxPK returns the partition key for a native transaction: T#N#{txHash}
func NativeTxPK(txHash string) string {
	return TxPKPrefix + NativeTxTypePrefix + txHash
}

// NativeTxSK returns the sort key for a native transaction: {paddedBlock}#{paddedTxIndex}
func NativeTxSK(blockNumber uint64, txIndex uint) string {
	return PadBlockNumber(blockNumber) + "#" + PadTxIndex(txIndex)
}

// NativeTxBlockSK returns the blockSk GSI value for a native tx: N#{paddedBlock}#{paddedTxIndex}
func NativeTxBlockSK(blockNumber uint64, txIndex uint) string {
	return NativeSKPrefix + PadBlockNumber(blockNumber) + "#" + PadTxIndex(txIndex)
}

// NativeReceivablePK returns the pk for a native receivable: A#N#{address}
func NativeReceivablePK(address string) string {
	return AddressReceivablePrefix + NativeTxTypePrefix + address
}

// NativeReceivableSK returns the sk: {paddedBlock}#{paddedTxIndex}#{isSender}
func NativeReceivableSK(blockNumber uint64, txIndex uint, isSender bool) string {
	return PadBlockNumber(blockNumber) + "#" + PadTxIndex(txIndex) + "#" + fmt.Sprintf("%t", isSender)
}

// InteractionPK returns the pk for an address interaction: I#{address}
func InteractionPK(address string) string {
	return AddressInteractionPrefix + address
}

// InteractionSK returns the sk for an address interaction: {paddedBlock}#{paddedTxIndex}
func InteractionSK(blockNumber uint64, txIndex uint) string {
	return PadBlockNumber(blockNumber) + "#" + PadTxIndex(txIndex)
}

// ERC20InsertPK returns the pk for an ERC-20 transfer insert: T#20#{txHash}
func ERC20InsertPK(txHash string) string {
	return TxPKPrefix + ERC20TxTypePrefix + txHash
}

// ERC20InsertSK returns the sk: {paddedBlock}#{paddedTxIndex}#L#{paddedLogIndex}
func ERC20InsertSK(blockNumber uint64, txIndex uint, logIndex uint) string {
	return PadBlockNumber(blockNumber) + "#" + PadTxIndex(txIndex) + "#L#" + PadLogIndex(logIndex)
}

// ERC20ReceivablePK returns the pk: A#20#{address}
func ERC20ReceivablePK(address string) string {
	return AddressReceivablePrefix + ERC20TxTypePrefix + address
}

// ERC20ReceivableSK returns the sk: {paddedBlock}#{paddedTxIndex}#L#{paddedLogIndex}#{isSender}
func ERC20ReceivableSK(blockNumber uint64, txIndex uint, logIndex uint, isSender bool) string {
	return PadBlockNumber(blockNumber) + "#" + PadTxIndex(txIndex) + "#L#" + PadLogIndex(logIndex) + "#" + fmt.Sprintf("%t", isSender)
}

// ERC721InsertPK returns the pk for an ERC-721 transfer insert: T#721#{txHash}
func ERC721InsertPK(txHash string) string {
	return TxPKPrefix + ERC721TxTypePrefix + txHash
}

// ERC721InsertSK returns the sk: {paddedBlock}#{paddedTxIndex}#L#{paddedLogIndex}
func ERC721InsertSK(blockNumber uint64, txIndex uint, logIndex uint) string {
	return PadBlockNumber(blockNumber) + "#" + PadTxIndex(txIndex) + "#L#" + PadLogIndex(logIndex)
}

// ERC721ReceivablePK returns the pk: A#721#{address}
func ERC721ReceivablePK(address string) string {
	return AddressReceivablePrefix + ERC721TxTypePrefix + address
}

// ERC721ReceivableSK returns the sk: {paddedBlock}#{paddedTxIndex}#L#{paddedLogIndex}#{isSender}
func ERC721ReceivableSK(blockNumber uint64, txIndex uint, logIndex uint, isSender bool) string {
	return PadBlockNumber(blockNumber) + "#" + PadTxIndex(txIndex) + "#L#" + PadLogIndex(logIndex) + "#" + fmt.Sprintf("%t", isSender)
}

// ERC1155InsertPK returns the pk for an ERC-1155 transfer insert: T#1155#{txHash}
func ERC1155InsertPK(txHash string) string {
	return TxPKPrefix + ERC1155TxTypePrefix + txHash
}

// ERC1155InsertSK returns the sk: {paddedBlock}#{paddedTxIndex}#L#{paddedLogIndex}#{paddedTransferIndex}
func ERC1155InsertSK(blockNumber uint64, txIndex uint, logIndex uint, transferIndex uint) string {
	return PadBlockNumber(blockNumber) + "#" + PadTxIndex(txIndex) + "#L#" + PadLogIndex(logIndex) + "#" + PadTransferIndex(transferIndex)
}

// ERC1155ReceivablePK returns the pk: A#1155#{address}
func ERC1155ReceivablePK(address string) string {
	return AddressReceivablePrefix + ERC1155TxTypePrefix + address
}

// ERC1155ReceivableSK returns the sk: {paddedBlock}#{paddedTxIndex}#L#{paddedLogIndex}#{paddedTransferIndex}#{isSender}
func ERC1155ReceivableSK(blockNumber uint64, txIndex uint, logIndex uint, transferIndex uint, isSender bool) string {
	return PadBlockNumber(blockNumber) + "#" + PadTxIndex(txIndex) + "#L#" + PadLogIndex(logIndex) + "#" + PadTransferIndex(transferIndex) + "#" + fmt.Sprintf("%t", isSender)
}
