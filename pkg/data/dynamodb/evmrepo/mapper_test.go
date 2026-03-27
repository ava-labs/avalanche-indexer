package evmrepo

import (
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanche-indexer/pkg/evmparser"
	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func strPtr(s string) *string { return &s }

func getStringAttr(item map[string]types.AttributeValue, key string) string {
	if v, ok := item[key].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

func getNumberAttr(item map[string]types.AttributeValue, key string) string {
	if v, ok := item[key].(*types.AttributeValueMemberN); ok {
		return v.Value
	}
	return ""
}

func getBoolAttr(item map[string]types.AttributeValue, key string) bool {
	if v, ok := item[key].(*types.AttributeValueMemberBOOL); ok {
		return v.Value
	}
	return false
}

func testBlock() *kafkamsg.EVMBlock {
	return &kafkamsg.EVMBlock{
		Number:       big.NewInt(100),
		Hash:         "0xblockhash",
		ParentHash:   "0xparenthash",
		EVMChainID:   big.NewInt(43114),
		BlockchainID: strPtr("blockchain-id"),
		BaseFee:      big.NewInt(25000000000),
		GasUsed:      21000,
		GasLimit:     30000000,
		Timestamp:    1700000000,
		TimestampMs:  1700000000000,
		Transactions: []*kafkamsg.EVMTransaction{},
	}
}

func testTx() *kafkamsg.EVMTransaction {
	return &kafkamsg.EVMTransaction{
		Hash:     "0xtxhash",
		From:     "0xFromAddr",
		To:       "0xToAddr",
		Value:    big.NewInt(1000000000000000000),
		Gas:      21000,
		GasPrice: big.NewInt(25000000000),
		Nonce:    5,
		Type:     2,
		Input:    "0xabcdef12",
		Receipt: &kafkamsg.EVMTxReceipt{
			Status:  1,
			GasUsed: 21000,
		},
	}
}

func TestCreateBlockWriteRequest(t *testing.T) {
	block := testBlock()
	wr := CreateBlockWriteRequest(block, 500)
	item := wr.PutRequest.Item

	assert.Equal(t, "B#0xblockhash", getStringAttr(item, "pk"))
	assert.Equal(t, "0000000000000064", getStringAttr(item, "sk"))
	assert.Equal(t, "B#0000000000000064", getStringAttr(item, "blockSk"))
	assert.Equal(t, "100", getNumberAttr(item, "blockNumber"))
	assert.Equal(t, "100", getNumberAttr(item, "blockNumberKey"))
	assert.Equal(t, "0xblockhash", getStringAttr(item, "blockHashKey"))
	assert.Equal(t, "0", getNumberAttr(item, "txCount"))
	assert.Equal(t, "25000000000", getNumberAttr(item, "baseFee"))
	assert.Equal(t, "21000", getNumberAttr(item, "gasUsed"))
	assert.Equal(t, "30000000", getNumberAttr(item, "gasLimit"))
	assert.Equal(t, "1", getNumberAttr(item, "isBlock"))
	assert.Equal(t, "1700000000", getNumberAttr(item, "blockTimestamp"))
	assert.Equal(t, "0xparenthash", getStringAttr(item, "parentHash"))
	assert.Equal(t, "500", getNumberAttr(item, "cumulativeTransactions"))

	// feesSpent = gasUsed * baseFee = 21000 * 25000000000
	assert.Equal(t, "525000000000000", getNumberAttr(item, "feesSpent"))
}

func TestCreateNativeTxWriteRequest(t *testing.T) {
	block := testBlock()
	tx := testTx()
	wr := CreateNativeTxWriteRequest(tx, block, 3)
	item := wr.PutRequest.Item

	assert.Equal(t, "T#N#0xtxhash", getStringAttr(item, "pk"))
	assert.Equal(t, "0000000000000064#00000003", getStringAttr(item, "sk"))
	assert.Equal(t, "N#0000000000000064#00000003", getStringAttr(item, "blockSk"))
	assert.Equal(t, "0xFromAddr", getStringAttr(item, "from"))
	assert.Equal(t, "0xToAddr", getStringAttr(item, "to"))
	assert.Equal(t, "native", getStringAttr(item, "insertType"))
	assert.Equal(t, "1000000000000000000", getNumberAttr(item, "value"))
	assert.Equal(t, "1", getNumberAttr(item, "txStatus"))
	assert.Equal(t, "0xabcdef12", getStringAttr(item, "methodHash"))

	// toContract should be true because Input has a 4-byte method selector
	assert.True(t, getBoolAttr(item, "toContract"))

	// Should not have deployed contract keys for non-deployment tx
	_, hasDeployed := item["deployedContractAddressKey"]
	assert.False(t, hasDeployed)
}

func TestCreateNativeTxWriteRequest_NativeTransfer(t *testing.T) {
	block := testBlock()
	tx := &kafkamsg.EVMTransaction{
		Hash:     "0xtxhash",
		From:     "0xFromAddr",
		To:       "0xToAddr",
		Value:    big.NewInt(1000000000000000000),
		Gas:      21000,
		GasPrice: big.NewInt(25000000000),
		Input:    "0x", // no method selector = native transfer
		Receipt: &kafkamsg.EVMTxReceipt{
			Status:  1,
			GasUsed: 21000,
		},
	}

	wr := CreateNativeTxWriteRequest(tx, block, 0)
	item := wr.PutRequest.Item

	// toContract should be false for a native transfer (no method selector)
	assert.False(t, getBoolAttr(item, "toContract"))
}

func TestCreateNativeTxWriteRequest_ContractDeployment(t *testing.T) {
	block := testBlock()
	contractAddr := common.HexToAddress("0xABCDEF1234567890ABCDEF1234567890ABCDEF12")
	tx := &kafkamsg.EVMTransaction{
		Hash:     "0xtxhash",
		From:     "0xDeployer",
		To:       "", // empty To = contract deployment
		Value:    big.NewInt(0),
		Gas:      500000,
		GasPrice: big.NewInt(25000000000),
		Input:    "0x60806040",
		Receipt: &kafkamsg.EVMTxReceipt{
			Status:          1,
			GasUsed:         200000,
			ContractAddress: contractAddr,
		},
	}

	wr := CreateNativeTxWriteRequest(tx, block, 0)
	item := wr.PutRequest.Item

	assert.Equal(t, contractAddr.Hex(), getStringAttr(item, "to"))
	assert.Equal(t, contractAddr.Hex(), getStringAttr(item, "deployedContractAddressKey"))
	assert.Equal(t, "0xDeployer", getStringAttr(item, "contractDeployerAddress"))

	// toContract should be false for contract deployments
	assert.False(t, getBoolAttr(item, "toContract"))

	// contractDeployed should be true
	assert.True(t, getBoolAttr(item, "contractDeployed"))
}

func TestCreateNativeTxWriteRequest_NonDeployment_NoContractDeployed(t *testing.T) {
	block := testBlock()
	tx := testTx() // normal tx, not a deployment
	wr := CreateNativeTxWriteRequest(tx, block, 0)
	item := wr.PutRequest.Item

	// contractDeployed should not be present for non-deployment txs
	_, hasContractDeployed := item["contractDeployed"]
	assert.False(t, hasContractDeployed)
}

func TestCreateNativeReceivableWriteRequests(t *testing.T) {
	block := testBlock()
	tx := testTx()
	requests := CreateNativeReceivableWriteRequests(tx, block, 0)

	require.Len(t, requests, 2)

	// Sender
	senderItem := requests[0].PutRequest.Item
	assert.Equal(t, "A#N#0xFromAddr", getStringAttr(senderItem, "pk"))
	assert.True(t, getBoolAttr(senderItem, "isSender"))
	assert.Equal(t, "0xToAddr", getStringAttr(senderItem, "otherAddress"))

	// Receiver
	receiverItem := requests[1].PutRequest.Item
	assert.Equal(t, "A#N#0xToAddr", getStringAttr(receiverItem, "pk"))
	assert.False(t, getBoolAttr(receiverItem, "isSender"))
	assert.Equal(t, "0xFromAddr", getStringAttr(receiverItem, "otherAddress"))
}

func TestCreateInteractionWriteRequests(t *testing.T) {
	block := testBlock()
	tx := testTx()

	erc20s := []*evmparser.ERC20Transfer{
		{From: "0xERC20From", To: "0xERC20To", ContractAddress: "0xERC20Contract"},
	}

	requests := CreateInteractionWriteRequests(tx, block, 0, erc20s, nil, nil)

	// Should have interactions for: FromAddr, ToAddr, ERC20From, ERC20To, ERC20Contract
	// = 5 unique addresses
	assert.Len(t, requests, 5)

	// Verify all pks start with "I#"
	for _, req := range requests {
		pk := getStringAttr(req.PutRequest.Item, "pk")
		assert.Contains(t, pk, "I#")
	}
}

func TestCreateERC20InsertWriteRequest(t *testing.T) {
	block := testBlock()
	transfer := &evmparser.ERC20Transfer{
		TxHash:          "0xtxhash",
		LogIndex:        5,
		From:            "0xFrom",
		To:              "0xTo",
		ContractAddress: "0xContract",
		Value:           big.NewInt(1000),
	}

	wr := CreateERC20InsertWriteRequest(transfer, block, 2)
	item := wr.PutRequest.Item

	assert.Equal(t, "T#20#0xtxhash", getStringAttr(item, "pk"))
	assert.Equal(t, "0000000000000064#00000002#L#00000005", getStringAttr(item, "sk"))
	assert.Equal(t, "erc20", getStringAttr(item, "insertType"))
	assert.Equal(t, "0xContract", getStringAttr(item, "contractAddressKey"))
	assert.Equal(t, "1000", getStringAttr(item, "value"))
}

func TestCreateERC721InsertWriteRequest(t *testing.T) {
	block := testBlock()
	transfer := &evmparser.ERC721Transfer{
		TxHash:          "0xtxhash",
		LogIndex:        1,
		From:            "0xFrom",
		To:              "0xTo",
		ContractAddress: "0xNFT",
		TokenID:         big.NewInt(42),
	}

	wr := CreateERC721InsertWriteRequest(transfer, block, 0)
	item := wr.PutRequest.Item

	assert.Equal(t, "T#721#0xtxhash", getStringAttr(item, "pk"))
	assert.Equal(t, "erc721", getStringAttr(item, "insertType"))
	assert.Equal(t, "42", getStringAttr(item, "tokenId"))
	assert.Equal(t, "0xNFT#42", getStringAttr(item, "contractAddress#tokenId"))
}

func TestCreateERC1155InsertWriteRequest(t *testing.T) {
	block := testBlock()
	transfer := &evmparser.ERC1155Transfer{
		TxHash:          "0xtxhash",
		LogIndex:        2,
		From:            "0xFrom",
		To:              "0xTo",
		ContractAddress: "0x1155",
		TokenID:         big.NewInt(10),
		Value:           big.NewInt(50),
		TransferIndex:   1,
	}

	wr := CreateERC1155InsertWriteRequest(transfer, block, 0)
	item := wr.PutRequest.Item

	assert.Equal(t, "T#1155#0xtxhash", getStringAttr(item, "pk"))
	assert.Equal(t, "erc1155", getStringAttr(item, "insertType"))
	assert.Equal(t, "10", getStringAttr(item, "tokenId"))
	assert.Equal(t, "50", getStringAttr(item, "value"))
	assert.Equal(t, "0x1155#10", getStringAttr(item, "contractAddress#tokenId"))
}

func TestCreateERCContractUpdate(t *testing.T) {
	tests := []struct {
		ercType     evmparser.ERCType
		expectedKey string
		expectedVal string
	}{
		{evmparser.ERC20, "erc20ContractAddressMetadataKey", "20"},
		{evmparser.ERC721, "erc721ContractAddressMetadataKey", "721"},
		{evmparser.ERC1155, "erc1155ContractAddressMetadataKey", "1155"},
	}

	for _, tt := range tests {
		update := CreateERCContractUpdate("erc-table", tt.ercType, "0xContract")
		assert.Equal(t, "erc-table", update.TableName)
		assert.Equal(t, "0xContract", getStringAttr(update.Key, "pk"))
		assert.Equal(t, "metadata", getStringAttr(update.Key, "sk"))
		assert.Contains(t, update.ExpressionAttributeNames, "#addressKey")
		assert.Equal(t, tt.expectedKey, update.ExpressionAttributeNames["#addressKey"])
	}
}

func TestCreateERC721OwnerUpdate(t *testing.T) {
	update := CreateERC721OwnerUpdate("erc-table", "0xNFT", big.NewInt(42), "0xNewOwner")
	assert.Equal(t, "ERC721#0xNFT", getStringAttr(update.Key, "pk"))
	paddedID := PadTokenID(big.NewInt(42))
	assert.Equal(t, "0xNFT#"+paddedID, getStringAttr(update.Key, "sk"))
}

func TestCalculateFeesSpent(t *testing.T) {
	block := &kafkamsg.EVMBlock{
		GasUsed: 21000,
		BaseFee: big.NewInt(25000000000),
	}
	assert.Equal(t, "525000000000000", calculateFeesSpent(block))
}

func TestCalculateFeesSpent_NilBaseFee(t *testing.T) {
	block := &kafkamsg.EVMBlock{GasUsed: 21000}
	assert.Equal(t, "0", calculateFeesSpent(block))
}

func TestMethodHashFromInput(t *testing.T) {
	assert.Equal(t, "0xabcdef12", methodHashFromInput("0xabcdef1234567890"))
	assert.Equal(t, "", methodHashFromInput("0x"))
	assert.Equal(t, "", methodHashFromInput(""))
}

func TestCreateLogWriteRequest(t *testing.T) {
	block := testBlock()
	log := &kafkamsg.EVMLog{
		Address:     common.HexToAddress("0xLogAddr"),
		Data:        []byte{0xab, 0xcd},
		BlockNumber: 100,
		TxHash:      common.HexToHash("0xtxhash"),
		TxIndex:     2,
		BlockHash:   common.HexToHash("0xblockhash"),
		Index:       5,
		Removed:     false,
	}

	wr := CreateLogWriteRequest(log, block)
	item := wr.PutRequest.Item

	assert.Equal(t, "log", getStringAttr(item, "insertType"))
	assert.Equal(t, "100", getNumberAttr(item, "blockNumber"))
	assert.Equal(t, "5", getNumberAttr(item, "logIndex"))
}

func TestCreateTopicWriteRequest(t *testing.T) {
	block := testBlock()
	topicHash := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	log := &kafkamsg.EVMLog{
		Topics:      []common.Hash{topicHash},
		BlockNumber: 100,
		TxHash:      common.HexToHash("0xtxhash"),
		BlockHash:   common.HexToHash("0xblockhash"),
		Index:       3,
	}

	wr := CreateTopicWriteRequest(log, 0, block)
	item := wr.PutRequest.Item

	assert.Equal(t, "topic", getStringAttr(item, "insertType"))
	assert.Equal(t, topicHash.Hex(), getStringAttr(item, "topic"))
	assert.Equal(t, "0", getNumberAttr(item, "topicIndex"))
}
