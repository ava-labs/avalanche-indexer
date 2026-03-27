package evmparser

import (
	"math/big"

	"github.com/ava-labs/libevm/common"

	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
)

// ParseERC20Transfers extracts ERC-20 Transfer events from a transaction's receipt logs.
// ERC-20 Transfer: topic0 = TransferMethodHash, exactly 3 topics, 32-byte data = value.
func ParseERC20Transfers(tx *kafkamsg.EVMTransaction) []*ERC20Transfer {
	if tx.Receipt == nil {
		return nil
	}

	var transfers []*ERC20Transfer
	for _, log := range tx.Receipt.Logs {
		if len(log.Topics) != 3 {
			continue
		}
		if log.Topics[0].Hex() != TransferMethodHash {
			continue
		}

		from := common.HexToAddress(log.Topics[1].Hex()).Hex()
		to := common.HexToAddress(log.Topics[2].Hex()).Hex()
		value := common.BytesToHash(log.Data).Big()

		transfers = append(transfers, &ERC20Transfer{
			TxHash:          tx.Hash,
			LogIndex:        log.Index,
			From:            from,
			To:              to,
			ContractAddress: log.Address.Hex(),
			Value:           value,
		})
	}
	return transfers
}

// ParseERC721Transfers extracts ERC-721 Transfer events from a transaction's receipt logs.
// ERC-721 Transfer: topic0 = TransferMethodHash, exactly 4 topics (from, to, tokenId).
func ParseERC721Transfers(tx *kafkamsg.EVMTransaction) []*ERC721Transfer {
	if tx.Receipt == nil {
		return nil
	}

	var transfers []*ERC721Transfer
	for _, log := range tx.Receipt.Logs {
		if len(log.Topics) != 4 {
			continue
		}
		if log.Topics[0].Hex() != TransferMethodHash {
			continue
		}

		from := common.HexToAddress(log.Topics[1].Hex()).Hex()
		to := common.HexToAddress(log.Topics[2].Hex()).Hex()
		tokenID := log.Topics[3].Big()

		transfers = append(transfers, &ERC721Transfer{
			TxHash:          tx.Hash,
			LogIndex:        log.Index,
			From:            from,
			To:              to,
			ContractAddress: log.Address.Hex(),
			TokenID:         tokenID,
		})
	}
	return transfers
}

// ParseERC1155Transfers extracts ERC-1155 TransferSingle and TransferBatch events.
// TransferSingle: topic0 = TransferSingleMethodHash, 4 topics, data = (tokenId, value).
// TransferBatch: topic0 = TransferBatchMethodHash, 4 topics, data = (tokenIds[], values[]).
func ParseERC1155Transfers(tx *kafkamsg.EVMTransaction) []*ERC1155Transfer {
	if tx.Receipt == nil {
		return nil
	}

	var transfers []*ERC1155Transfer
	for _, log := range tx.Receipt.Logs {
		if len(log.Topics) < 4 {
			continue
		}

		topic0 := log.Topics[0].Hex()
		from := common.HexToAddress(log.Topics[2].Hex()).Hex()
		to := common.HexToAddress(log.Topics[3].Hex()).Hex()
		contractAddr := log.Address.Hex()

		switch topic0 {
		case TransferSingleMethodHash:
			tokenID, value, err := parseTransferSingleData(log.Data)
			if err != nil {
				continue
			}
			transfers = append(transfers, &ERC1155Transfer{
				TxHash:          tx.Hash,
				LogIndex:        log.Index,
				From:            from,
				To:              to,
				ContractAddress: contractAddr,
				TokenID:         tokenID,
				Value:           value,
				TransferIndex:   0,
			})

		case TransferBatchMethodHash:
			pairs := parseTransferBatchData(log.Data)
			for i, pair := range pairs {
				transfers = append(transfers, &ERC1155Transfer{
					TxHash:          tx.Hash,
					LogIndex:        log.Index,
					From:            from,
					To:              to,
					ContractAddress: contractAddr,
					TokenID:         pair.tokenID,
					Value:           pair.value,
					TransferIndex:   uint(i),
				})
			}
		}
	}
	return transfers
}

type tokenIDValue struct {
	tokenID *big.Int
	value   *big.Int
}

// parseTransferSingleData parses the data field of a TransferSingle event.
// Data layout: [tokenId (32 bytes)][value (32 bytes)]
func parseTransferSingleData(data []byte) (*big.Int, *big.Int, error) {
	if len(data) < 64 {
		return nil, nil, ErrInvalidLogData
	}
	tokenID := new(big.Int).SetBytes(data[:32])
	value := new(big.Int).SetBytes(data[32:64])
	return tokenID, value, nil
}

// parseTransferBatchData parses the data field of a TransferBatch event.
// Data layout: [offset1 (32)][offset2 (32)][length (32)][tokenId1...tokenIdN][value1...valueN]
func parseTransferBatchData(data []byte) []tokenIDValue {
	const chunkSize = 32
	if len(data) < chunkSize*3 {
		return nil
	}

	// Skip two offset words, read array length
	length := new(big.Int).SetBytes(data[chunkSize*2 : chunkSize*3]).Int64()
	if length <= 0 {
		return nil
	}

	// Ensure we have enough data: 3 header words + length tokenIDs + length values
	expectedSize := chunkSize * (3 + int(length)*2)
	if len(data) < expectedSize {
		return nil
	}

	results := make([]tokenIDValue, length)
	tokenIDStart := chunkSize * 3
	valueStart := tokenIDStart + chunkSize*int(length)

	for i := int64(0); i < length; i++ {
		offset := int(i) * chunkSize
		results[i] = tokenIDValue{
			tokenID: new(big.Int).SetBytes(data[tokenIDStart+offset : tokenIDStart+offset+chunkSize]),
			value:   new(big.Int).SetBytes(data[valueStart+offset : valueStart+offset+chunkSize]),
		}
	}
	return results
}
