package evmrepo

import (
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/evmparser"
	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	"github.com/ava-labs/libevm/common"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// S creates a DynamoDB string attribute value.
func S(val string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: val}
}

// N creates a DynamoDB number attribute value from a string.
func N(val string) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: val}
}

// BOOL creates a DynamoDB boolean attribute value.
func BOOL(val bool) types.AttributeValue {
	return &types.AttributeValueMemberBOOL{Value: val}
}

// nowUnixMs returns the current time as milliseconds since epoch.
func nowUnixMs() string {
	return strconv.FormatInt(time.Now().UnixMilli(), 10)
}

// bigIntStr returns the string representation of a big.Int, defaulting to "0" for nil.
func bigIntStr(v *big.Int) string {
	if v == nil {
		return "0"
	}
	return v.String()
}

// methodHashFromInput extracts the 4-byte method selector from tx input data.
func methodHashFromInput(input string) string {
	// input is hex-encoded, e.g. "0xabcdef12..."
	if len(input) >= 10 {
		return input[:10] // "0x" + 4 bytes = 10 chars
	}
	return ""
}

// isContractDeployment returns true if the transaction deployed a contract.
func isContractDeployment(tx *kafkamsg.EVMTransaction) bool {
	return tx.To == "" && tx.Receipt != nil && tx.Receipt.ContractAddress != (common.Address{})
}

// effectiveToAddress returns the contract address if deployed, else the To field.
func effectiveToAddress(tx *kafkamsg.EVMTransaction) string {
	if isContractDeployment(tx) {
		return tx.Receipt.ContractAddress.Hex()
	}
	return tx.To
}

// isToContract determines if the transaction targets a contract.
// Logic matches legacy analytics: if the tx has a method selector (input >= 4 bytes / 10 hex chars
// with "0x" prefix) and is not a contract deployment, it's a contract call.
func isToContract(tx *kafkamsg.EVMTransaction) bool {
	if isContractDeployment(tx) {
		return false
	}
	// tx.Input is hex-encoded: "0x" prefix + 2 hex chars per byte.
	// A 4-byte method selector = "0x" + 8 hex chars = 10 chars minimum.
	return len(tx.Input) >= 10
}

// CreateBlockWriteRequest creates the DynamoDB PutItem request for a block.
func CreateBlockWriteRequest(block *kafkamsg.EVMBlock, cumulativeTxs uint64) types.WriteRequest {
	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	item := map[string]types.AttributeValue{
		"pk":                          S(BlockPK(block.Hash)),
		"sk":                          S(BlockSK(blockNumber)),
		"blockSk":                     S(BlockSKValue(blockNumber)),
		"blockNumber":                 N(strconv.FormatUint(blockNumber, 10)),
		"blockNumberKey":              N(strconv.FormatUint(blockNumber, 10)),
		"blockHashKey":                S(block.Hash),
		"txCount":                     N(strconv.Itoa(len(block.Transactions))),
		"baseFee":                     N(bigIntStr(block.BaseFee)),
		"gasUsed":                     N(strconv.FormatUint(block.GasUsed, 10)),
		"gasLimit":                    N(strconv.FormatUint(block.GasLimit, 10)),
		"blockGasCost":                N("0"), // Not available in Kafka message; default to 0
		"blockHash":                   S(block.Hash),
		"blockTimestamp":              N(strconv.FormatUint(block.Timestamp, 10)),
		"parentHash":                  S(block.ParentHash),
		"feesSpent":                   N(calculateFeesSpent(block)),
		"cumulativeTransactions":      N(strconv.FormatUint(cumulativeTxs, 10)),
		"lastUpdated":                 N(nowUnixMs()),
		"isBlock":                     N("1"),
		"blockTimestampMilliseconds":  S(strconv.FormatUint(block.TimestampMs, 10)),
		"minDelayExcess":              S(strconv.FormatUint(block.MinDelayExcess, 10)),
	}

	return types.WriteRequest{
		PutRequest: &types.PutRequest{Item: item},
	}
}

// calculateFeesSpent computes gasUsed * baseFee for a block.
func calculateFeesSpent(block *kafkamsg.EVMBlock) string {
	if block.BaseFee == nil {
		return "0"
	}
	fees := new(big.Int).Mul(new(big.Int).SetUint64(block.GasUsed), block.BaseFee)
	return fees.String()
}

// CreateNativeTxWriteRequest creates the DynamoDB PutItem request for a native transaction.
func CreateNativeTxWriteRequest(
	tx *kafkamsg.EVMTransaction,
	block *kafkamsg.EVMBlock,
	txIndex uint,
) types.WriteRequest {
	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	toAddr := effectiveToAddress(tx)
	contractDeployed := isContractDeployment(tx)
	methodHash := methodHashFromInput(tx.Input)

	gasUsed := uint64(0)
	status := uint64(0)
	if tx.Receipt != nil {
		gasUsed = tx.Receipt.GasUsed
		status = tx.Receipt.Status
	}

	item := map[string]types.AttributeValue{
		"pk":                          S(NativeTxPK(tx.Hash)),
		"sk":                          S(NativeTxSK(blockNumber, txIndex)),
		"blockNumber":                 N(strconv.FormatUint(blockNumber, 10)),
		"blockIndex":                  N(strconv.FormatUint(uint64(txIndex), 10)),
		"blockSk":                     S(NativeTxBlockSK(blockNumber, txIndex)),
		"txHash":                      S(tx.Hash),
		"from":                        S(tx.From),
		"to":                          S(toAddr),
		"blockHash":                   S(block.Hash),
		"gasPrice":                    N(bigIntStr(tx.GasPrice)),
		"gasLimit":                    N(strconv.FormatUint(tx.Gas, 10)),
		"gasUsed":                     N(strconv.FormatUint(gasUsed, 10)),
		"maxPriorityFeePerGas":        N(bigIntStr(tx.MaxPriorityFee)),
		"maxFeePerGas":                N(bigIntStr(tx.MaxFeePerGas)),
		"nonce":                       N(strconv.FormatUint(tx.Nonce, 10)),
		"txStatus":                    N(strconv.FormatUint(status, 10)),
		"blockTimestamp":              N(strconv.FormatUint(block.Timestamp, 10)),
		"blockTimestampMilliseconds":  S(strconv.FormatUint(block.TimestampMs, 10)),
		"toContract":                  BOOL(isToContract(tx)),
		"txType":                      N(strconv.FormatUint(uint64(tx.Type), 10)),
		"value":                       N(bigIntStr(tx.Value)),
		"lastUpdated":                 N(nowUnixMs()),
		"methodHash":                  S(methodHash),
		"insertType":                  S("native"),
		"blockNumberKey":              N(strconv.FormatUint(blockNumber, 10)),
		"blockHashKey":                S(block.Hash),
	}

	if contractDeployed {
		item["contractDeployed"] = BOOL(true)
		item["deployedContractAddressKey"] = S(toAddr)
		item["contractDeployerAddress"] = S(tx.From)
	}

	return types.WriteRequest{
		PutRequest: &types.PutRequest{Item: item},
	}
}

// CreateNativeReceivableWriteRequests creates sender and receiver receivable items for a native tx.
func CreateNativeReceivableWriteRequests(
	tx *kafkamsg.EVMTransaction,
	block *kafkamsg.EVMBlock,
	txIndex uint,
) []types.WriteRequest {
	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	toAddr := effectiveToAddress(tx)
	contractDeployed := isContractDeployment(tx)
	methodHash := methodHashFromInput(tx.Input)
	gasUsed := uint64(0)
	status := uint64(0)
	if tx.Receipt != nil {
		gasUsed = tx.Receipt.GasUsed
		status = tx.Receipt.Status
	}

	makeItem := func(address string, isSender bool, otherAddress string) types.WriteRequest {
		item := map[string]types.AttributeValue{
			"pk":                          S(NativeReceivablePK(address)),
			"sk":                          S(NativeReceivableSK(blockNumber, txIndex, isSender)),
			"address":                     S(address),
			"insertType":                  S("native"),
			"blockNumber":                 N(strconv.FormatUint(blockNumber, 10)),
			"blockIndex":                  N(strconv.FormatUint(uint64(txIndex), 10)),
			"txHash":                      S(tx.Hash),
			"isSender":                    BOOL(isSender),
			"otherAddress":                S(otherAddress),
			"blockHash":                   S(block.Hash),
			"gasPrice":                    N(bigIntStr(tx.GasPrice)),
			"gasLimit":                    N(strconv.FormatUint(tx.Gas, 10)),
			"gasUsed":                     N(strconv.FormatUint(gasUsed, 10)),
			"nonce":                       N(strconv.FormatUint(tx.Nonce, 10)),
			"txStatus":                    N(strconv.FormatUint(status, 10)),
			"blockTimestamp":              N(strconv.FormatUint(block.Timestamp, 10)),
			"blockTimestampMilliseconds":  S(strconv.FormatUint(block.TimestampMs, 10)),
			"toContract":                  BOOL(isToContract(tx)),
			"txType":                      N(strconv.FormatUint(uint64(tx.Type), 10)),
			"value":                       N(bigIntStr(tx.Value)),
			"lastUpdated":                 N(nowUnixMs()),
			"methodHash":                  S(methodHash),
		}
		if contractDeployed {
			item["contractDeployed"] = BOOL(true)
		}
		return types.WriteRequest{
			PutRequest: &types.PutRequest{Item: item},
		}
	}

	return []types.WriteRequest{
		makeItem(tx.From, true, toAddr),
		makeItem(toAddr, false, tx.From),
	}
}

// CreateInteractionWriteRequests creates address interaction items for all addresses
// involved in a transaction (from, to, ERC addresses).
func CreateInteractionWriteRequests(
	tx *kafkamsg.EVMTransaction,
	block *kafkamsg.EVMBlock,
	txIndex uint,
	erc20s []*evmparser.ERC20Transfer,
	erc721s []*evmparser.ERC721Transfer,
	erc1155s []*evmparser.ERC1155Transfer,
) []types.WriteRequest {
	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	toAddr := effectiveToAddress(tx)
	contractDeployed := isContractDeployment(tx)
	methodHash := methodHashFromInput(tx.Input)
	gasUsed := uint64(0)
	status := uint64(0)
	if tx.Receipt != nil {
		gasUsed = tx.Receipt.GasUsed
		status = tx.Receipt.Status
	}

	// Collect unique addresses
	addresses := map[string]struct{}{}
	addresses[tx.From] = struct{}{}
	if toAddr != "" {
		addresses[toAddr] = struct{}{}
	}
	for _, t := range erc20s {
		addresses[t.From] = struct{}{}
		addresses[t.To] = struct{}{}
		addresses[t.ContractAddress] = struct{}{}
	}
	for _, t := range erc721s {
		addresses[t.From] = struct{}{}
		addresses[t.To] = struct{}{}
		addresses[t.ContractAddress] = struct{}{}
	}
	for _, t := range erc1155s {
		addresses[t.From] = struct{}{}
		addresses[t.To] = struct{}{}
		addresses[t.ContractAddress] = struct{}{}
	}

	var requests []types.WriteRequest
	for address := range addresses {
		if address == "" {
			continue
		}
		item := map[string]types.AttributeValue{
			"pk":                          S(InteractionPK(address)),
			"sk":                          S(InteractionSK(blockNumber, txIndex)),
			"insertType":                  S("interaction"),
			"blockNumber":                 N(strconv.FormatUint(blockNumber, 10)),
			"blockIndex":                  N(strconv.FormatUint(uint64(txIndex), 10)),
			"blockHash":                   S(block.Hash),
			"txHash":                      S(tx.Hash),
			"gasPrice":                    N(bigIntStr(tx.GasPrice)),
			"gasLimit":                    N(strconv.FormatUint(tx.Gas, 10)),
			"gasUsed":                     N(strconv.FormatUint(gasUsed, 10)),
			"nonce":                       N(strconv.FormatUint(tx.Nonce, 10)),
			"txStatus":                    N(strconv.FormatUint(status, 10)),
			"blockTimestamp":              N(strconv.FormatUint(block.Timestamp, 10)),
			"blockTimestampMilliseconds":  S(strconv.FormatUint(block.TimestampMs, 10)),
			"toContract":                  BOOL(isToContract(tx)),
			"txType":                      N(strconv.FormatUint(uint64(tx.Type), 10)),
			"value":                       N(bigIntStr(tx.Value)),
			"from":                        S(tx.From),
			"to":                          S(toAddr),
			"methodHash":                  S(methodHash),
			"lastUpdated":                 N(nowUnixMs()),
		}
		if contractDeployed {
			item["contractDeployed"] = BOOL(true)
		}
		requests = append(requests, types.WriteRequest{
			PutRequest: &types.PutRequest{Item: item},
		})
	}
	return requests
}

// CreateERC20InsertWriteRequest creates the DynamoDB PutItem for an ERC-20 transfer insert.
func CreateERC20InsertWriteRequest(
	transfer *evmparser.ERC20Transfer,
	block *kafkamsg.EVMBlock,
	txIndex uint,
) types.WriteRequest {
	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: map[string]types.AttributeValue{
				"pk":                          S(ERC20InsertPK(transfer.TxHash)),
				"sk":                          S(ERC20InsertSK(blockNumber, txIndex, transfer.LogIndex)),
				"insertType":                  S("erc20"),
				"blockNumber":                 N(strconv.FormatUint(blockNumber, 10)),
				"blockIndex":                  N(strconv.FormatUint(uint64(txIndex), 10)),
				"logIndex":                    N(strconv.FormatUint(uint64(transfer.LogIndex), 10)),
				"txHash":                      S(transfer.TxHash),
				"from":                        S(transfer.From),
				"to":                          S(transfer.To),
				"blockHash":                   S(block.Hash),
				"blockTimestamp":              N(strconv.FormatUint(block.Timestamp, 10)),
				"blockTimestampMilliseconds":  S(strconv.FormatUint(block.TimestampMs, 10)),
				"contractAddressKey":          S(transfer.ContractAddress),
				"value":                       S(bigIntStr(transfer.Value)),
				"lastUpdated":                 N(nowUnixMs()),
			},
		},
	}
}

// CreateERC20ReceivableWriteRequests creates sender and receiver receivable items for an ERC-20 transfer.
func CreateERC20ReceivableWriteRequests(
	transfer *evmparser.ERC20Transfer,
	block *kafkamsg.EVMBlock,
	txIndex uint,
) []types.WriteRequest {
	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	makeItem := func(address string, isSender bool, otherAddress string) types.WriteRequest {
		return types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"pk":                          S(ERC20ReceivablePK(address)),
					"sk":                          S(ERC20ReceivableSK(blockNumber, txIndex, transfer.LogIndex, isSender)),
					"address":                     S(address),
					"insertType":                  S("erc20"),
					"blockNumber":                 N(strconv.FormatUint(blockNumber, 10)),
					"blockIndex":                  N(strconv.FormatUint(uint64(txIndex), 10)),
					"blockHash":                   S(block.Hash),
					"blockTimestamp":              N(strconv.FormatUint(block.Timestamp, 10)),
					"blockTimestampMilliseconds":  S(strconv.FormatUint(block.TimestampMs, 10)),
					"logIndex":                    N(strconv.FormatUint(uint64(transfer.LogIndex), 10)),
					"txHash":                      S(transfer.TxHash),
					"isSender":                    BOOL(isSender),
					"otherAddress":                S(otherAddress),
					"contractAddress":             S(transfer.ContractAddress),
					"value":                       S(bigIntStr(transfer.Value)),
					"lastUpdated":                 N(nowUnixMs()),
				},
			},
		}
	}

	return []types.WriteRequest{
		makeItem(transfer.From, true, transfer.To),
		makeItem(transfer.To, false, transfer.From),
	}
}

// CreateERC721InsertWriteRequest creates the DynamoDB PutItem for an ERC-721 transfer insert.
func CreateERC721InsertWriteRequest(
	transfer *evmparser.ERC721Transfer,
	block *kafkamsg.EVMBlock,
	txIndex uint,
) types.WriteRequest {
	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: map[string]types.AttributeValue{
				"pk":                          S(ERC721InsertPK(transfer.TxHash)),
				"sk":                          S(ERC721InsertSK(blockNumber, txIndex, transfer.LogIndex)),
				"insertType":                  S("erc721"),
				"blockNumber":                 N(strconv.FormatUint(blockNumber, 10)),
				"blockIndex":                  N(strconv.FormatUint(uint64(txIndex), 10)),
				"logIndex":                    N(strconv.FormatUint(uint64(transfer.LogIndex), 10)),
				"txHash":                      S(transfer.TxHash),
				"from":                        S(transfer.From),
				"to":                          S(transfer.To),
				"blockHash":                   S(block.Hash),
				"blockTimestamp":              N(strconv.FormatUint(block.Timestamp, 10)),
				"blockTimestampMilliseconds":  S(strconv.FormatUint(block.TimestampMs, 10)),
				"contractAddressKey":          S(transfer.ContractAddress),
				"tokenId":                     S(transfer.TokenID.String()),
				"contractAddress#tokenId":     S(transfer.ContractAddress + "#" + transfer.TokenID.String()),
				"lastUpdated":                 N(nowUnixMs()),
			},
		},
	}
}

// CreateERC721ReceivableWriteRequests creates sender and receiver receivable items for an ERC-721 transfer.
func CreateERC721ReceivableWriteRequests(
	transfer *evmparser.ERC721Transfer,
	block *kafkamsg.EVMBlock,
	txIndex uint,
) []types.WriteRequest {
	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	makeItem := func(address string, isSender bool, otherAddress string) types.WriteRequest {
		return types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"pk":                          S(ERC721ReceivablePK(address)),
					"sk":                          S(ERC721ReceivableSK(blockNumber, txIndex, transfer.LogIndex, isSender)),
					"address":                     S(address),
					"insertType":                  S("erc721"),
					"blockNumber":                 N(strconv.FormatUint(blockNumber, 10)),
					"blockIndex":                  N(strconv.FormatUint(uint64(txIndex), 10)),
					"blockHash":                   S(block.Hash),
					"blockTimestamp":              N(strconv.FormatUint(block.Timestamp, 10)),
					"blockTimestampMilliseconds":  S(strconv.FormatUint(block.TimestampMs, 10)),
					"logIndex":                    N(strconv.FormatUint(uint64(transfer.LogIndex), 10)),
					"txHash":                      S(transfer.TxHash),
					"isSender":                    BOOL(isSender),
					"otherAddress":                S(otherAddress),
					"contractAddress":             S(transfer.ContractAddress),
					"tokenId":                     S(transfer.TokenID.String()),
					"lastUpdated":                 N(nowUnixMs()),
				},
			},
		}
	}

	return []types.WriteRequest{
		makeItem(transfer.From, true, transfer.To),
		makeItem(transfer.To, false, transfer.From),
	}
}

// CreateERC1155InsertWriteRequest creates the DynamoDB PutItem for an ERC-1155 transfer insert.
func CreateERC1155InsertWriteRequest(
	transfer *evmparser.ERC1155Transfer,
	block *kafkamsg.EVMBlock,
	txIndex uint,
) types.WriteRequest {
	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: map[string]types.AttributeValue{
				"pk":                          S(ERC1155InsertPK(transfer.TxHash)),
				"sk":                          S(ERC1155InsertSK(blockNumber, txIndex, transfer.LogIndex, transfer.TransferIndex)),
				"insertType":                  S("erc1155"),
				"blockNumber":                 N(strconv.FormatUint(blockNumber, 10)),
				"blockIndex":                  N(strconv.FormatUint(uint64(txIndex), 10)),
				"logIndex":                    N(strconv.FormatUint(uint64(transfer.LogIndex), 10)),
				"transferIndex":               N(strconv.FormatUint(uint64(transfer.TransferIndex), 10)),
				"txHash":                      S(transfer.TxHash),
				"from":                        S(transfer.From),
				"to":                          S(transfer.To),
				"blockHash":                   S(block.Hash),
				"blockTimestamp":              N(strconv.FormatUint(block.Timestamp, 10)),
				"blockTimestampMilliseconds":  S(strconv.FormatUint(block.TimestampMs, 10)),
				"contractAddressKey":          S(transfer.ContractAddress),
				"tokenId":                     S(transfer.TokenID.String()),
				"contractAddress#tokenId":     S(transfer.ContractAddress + "#" + transfer.TokenID.String()),
				"value":                       S(bigIntStr(transfer.Value)),
				"lastUpdated":                 N(nowUnixMs()),
			},
		},
	}
}

// CreateERC1155ReceivableWriteRequests creates sender and receiver receivable items for an ERC-1155 transfer.
func CreateERC1155ReceivableWriteRequests(
	transfer *evmparser.ERC1155Transfer,
	block *kafkamsg.EVMBlock,
	txIndex uint,
) []types.WriteRequest {
	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	makeItem := func(address string, isSender bool, otherAddress string) types.WriteRequest {
		return types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"pk":                          S(ERC1155ReceivablePK(address)),
					"sk":                          S(ERC1155ReceivableSK(blockNumber, txIndex, transfer.LogIndex, transfer.TransferIndex, isSender)),
					"address":                     S(address),
					"insertType":                  S("erc1155"),
					"blockNumber":                 N(strconv.FormatUint(blockNumber, 10)),
					"blockIndex":                  N(strconv.FormatUint(uint64(txIndex), 10)),
					"blockHash":                   S(block.Hash),
					"blockTimestamp":              N(strconv.FormatUint(block.Timestamp, 10)),
					"blockTimestampMilliseconds":  S(strconv.FormatUint(block.TimestampMs, 10)),
					"logIndex":                    N(strconv.FormatUint(uint64(transfer.LogIndex), 10)),
					"transferIndex":               N(strconv.FormatUint(uint64(transfer.TransferIndex), 10)),
					"txHash":                      S(transfer.TxHash),
					"isSender":                    BOOL(isSender),
					"otherAddress":                S(otherAddress),
					"contractAddress":             S(transfer.ContractAddress),
					"tokenId":                     S(transfer.TokenID.String()),
					"value":                       S(bigIntStr(transfer.Value)),
					"lastUpdated":                 N(nowUnixMs()),
				},
			},
		}
	}

	return []types.WriteRequest{
		makeItem(transfer.From, true, transfer.To),
		makeItem(transfer.To, false, transfer.From),
	}
}

// CreateLogWriteRequest creates a log DynamoDB item.
func CreateLogWriteRequest(
	log *kafkamsg.EVMLog,
	block *kafkamsg.EVMBlock,
) types.WriteRequest {
	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: map[string]types.AttributeValue{
				"pk":          S(fmt.Sprintf("%d#%s", blockNumber, log.Address.Hex())),
				"sk":          S(fmt.Sprintf("log#%s#%s#%s", PadBlockNumber(blockNumber), PadLogIndex(log.Index), PadTxIndex(log.TxIndex))),
				"insertType":  S("log"),
				"address":     S(log.Address.Hex()),
				"data":        S(common.Bytes2Hex(log.Data)),
				"blockNumber": N(strconv.FormatUint(blockNumber, 10)),
				"txHash":      S(log.TxHash.Hex()),
				"txIndex":     N(strconv.FormatUint(uint64(log.TxIndex), 10)),
				"blockHash":   S(log.BlockHash.Hex()),
				"logIndex":    N(strconv.FormatUint(uint64(log.Index), 10)),
				"removed":     BOOL(log.Removed),
			},
		},
	}
}

// CreateTopicWriteRequest creates a topic DynamoDB item.
func CreateTopicWriteRequest(
	log *kafkamsg.EVMLog,
	topicIndex int,
	block *kafkamsg.EVMBlock,
) types.WriteRequest {
	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: map[string]types.AttributeValue{
				"pk":          S(fmt.Sprintf("%d#%s", blockNumber, log.Topics[topicIndex].Hex())),
				"sk":          S(fmt.Sprintf("topic#%s#%s#%d", PadBlockNumber(blockNumber), PadLogIndex(log.Index), topicIndex)),
				"insertType":  S("topic"),
				"topic":       S(log.Topics[topicIndex].Hex()),
				"topicIndex":  N(strconv.Itoa(topicIndex)),
				"txHash":      S(log.TxHash.Hex()),
				"blockNumber": N(strconv.FormatUint(blockNumber, 10)),
				"blockHash":   S(log.BlockHash.Hex()),
				"logIndex":    N(strconv.FormatUint(uint64(log.Index), 10)),
			},
		},
	}
}

// CreateERCContractUpdate builds an UpdateItem for tracking contract existence in the ERC table.
func CreateERCContractUpdate(tableName string, ercType evmparser.ERCType, contractAddress string) *ERCUpdateItem {
	addressKey := ""
	typeStr := ""
	switch ercType {
	case evmparser.ERC20:
		addressKey = "erc20ContractAddressMetadataKey"
		typeStr = "20"
	case evmparser.ERC721:
		addressKey = "erc721ContractAddressMetadataKey"
		typeStr = "721"
	case evmparser.ERC1155:
		addressKey = "erc1155ContractAddressMetadataKey"
		typeStr = "1155"
	}

	return &ERCUpdateItem{
		TableName: tableName,
		Key: map[string]types.AttributeValue{
			"pk": S(contractAddress),
			"sk": S("metadata"),
		},
		UpdateExpression: "SET #et = :et, #ca = :ca, #lu = :lu, #addressKey = :addressValue",
		ExpressionAttributeNames: map[string]string{
			"#et":         "ercType",
			"#ca":         "contractAddress",
			"#lu":         "lastUpdated",
			"#addressKey": addressKey,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":et":           S(typeStr),
			":ca":           S(contractAddress),
			":lu":           N(nowUnixMs()),
			":addressValue": S(contractAddress),
		},
	}
}

// CreateERC20InteractionUpdate builds an UpdateItem for tracking ERC-20 interactions per address.
func CreateERC20InteractionUpdate(tableName string, address string, contractAddress string) *ERCUpdateItem {
	return &ERCUpdateItem{
		TableName: tableName,
		Key: map[string]types.AttributeValue{
			"pk": S(string(evmparser.ERC20) + "#" + address),
			"sk": S(contractAddress),
		},
		UpdateExpression: "SET #et = :et, #ca = :ca, #lu = :lu",
		ExpressionAttributeNames: map[string]string{
			"#et": "ercType",
			"#ca": "contractAddress",
			"#lu": "lastUpdated",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":et": S("20"),
			":ca": S(contractAddress),
			":lu": N(nowUnixMs()),
		},
	}
}

// CreateERC721OwnerUpdate builds an UpdateItem for tracking ERC-721 ownership.
func CreateERC721OwnerUpdate(tableName string, contractAddress string, tokenID *big.Int, toAddress string) *ERCUpdateItem {
	return &ERCUpdateItem{
		TableName: tableName,
		Key: map[string]types.AttributeValue{
			"pk": S(string(evmparser.ERC721) + "#" + contractAddress),
			"sk": S(contractAddress + "#" + PadTokenID(tokenID)),
		},
		UpdateExpression: "SET #et = :et, #ca = :ca, #ti = :ti, #lu = :lu, #coa = :coa, #oa = :oa",
		ExpressionAttributeNames: map[string]string{
			"#et":  "ercType",
			"#ca":  "contractAddress",
			"#ti":  "tokenId",
			"#lu":  "lastUpdated",
			"#coa": "collectibleOwnerAddress",
			"#oa":  "erc721OwnerAddress",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":et":  S("721"),
			":ca":  S(contractAddress),
			":ti":  S(tokenID.String()),
			":lu":  N(nowUnixMs()),
			":coa": S(toAddress),
			":oa":  S(toAddress),
		},
	}
}

// CreateERC1155InteractionUpdate builds an UpdateItem for tracking ERC-1155 interactions.
func CreateERC1155InteractionUpdate(tableName string, address string, contractAddress string, tokenID *big.Int) *ERCUpdateItem {
	return &ERCUpdateItem{
		TableName: tableName,
		Key: map[string]types.AttributeValue{
			"pk": S(string(evmparser.ERC1155) + "#" + address),
			"sk": S(contractAddress + "#" + PadTokenID(tokenID)),
		},
		UpdateExpression: "SET #et = :et, #ca = :ca, #ti = :ti, #lu = :lu, #coa = :coa",
		ExpressionAttributeNames: map[string]string{
			"#et":  "ercType",
			"#ca":  "contractAddress",
			"#ti":  "tokenId",
			"#lu":  "lastUpdated",
			"#coa": "collectibleOwnerAddress",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":et":  S("1155"),
			":ca":  S(contractAddress),
			":ti":  S(tokenID.String()),
			":lu":  N(nowUnixMs()),
			":coa": S(address),
		},
	}
}

// CreateERCTokenExistenceUpdate builds an UpdateItem for tracking token existence (721/1155).
func CreateERCTokenExistenceUpdate(tableName string, ercType evmparser.ERCType, contractAddress string, tokenID *big.Int) *ERCUpdateItem {
	addressKey := ""
	typeStr := ""
	switch ercType {
	case evmparser.ERC721:
		addressKey = "erc721ContractAddress"
		typeStr = "721"
	case evmparser.ERC1155:
		addressKey = "erc1155ContractAddress"
		typeStr = "1155"
	}

	return &ERCUpdateItem{
		TableName: tableName,
		Key: map[string]types.AttributeValue{
			"pk": S(contractAddress),
			"sk": S(contractAddress + "#" + PadTokenID(tokenID)),
		},
		UpdateExpression: "SET #et = :et, #ca = :ca, #ti = :ti, #lu = :lu, #addressKey = :addressValue",
		ExpressionAttributeNames: map[string]string{
			"#et":         "ercType",
			"#ca":         "contractAddress",
			"#ti":         "tokenId",
			"#lu":         "lastUpdated",
			"#addressKey": addressKey,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":et":           S(typeStr),
			":ca":           S(contractAddress),
			":ti":           S(tokenID.String()),
			":lu":           N(nowUnixMs()),
			":addressValue": S(contractAddress),
		},
	}
}

// ERCUpdateItem holds the parameters for a DynamoDB UpdateItem call.
type ERCUpdateItem struct {
	TableName                 string
	Key                       map[string]types.AttributeValue
	UpdateExpression          string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
}
