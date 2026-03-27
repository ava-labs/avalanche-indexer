package evmparser

import "math/big"

// ERC20Transfer represents a parsed ERC-20 Transfer event.
type ERC20Transfer struct {
	TxHash          string
	LogIndex        uint
	From            string
	To              string
	ContractAddress string
	Value           *big.Int
}

// ERC721Transfer represents a parsed ERC-721 Transfer event.
type ERC721Transfer struct {
	TxHash          string
	LogIndex        uint
	From            string
	To              string
	ContractAddress string
	TokenID         *big.Int
}

// ERC1155Transfer represents a parsed ERC-1155 TransferSingle or TransferBatch event.
type ERC1155Transfer struct {
	TxHash          string
	LogIndex        uint
	From            string
	To              string
	ContractAddress string
	TokenID         *big.Int
	Value           *big.Int
	TransferIndex   uint
}
