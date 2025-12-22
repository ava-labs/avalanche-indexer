package types

import "math/big"


type Block struct {
	Number       big.Int       `json:"number"`
	Hash         string        `json:"hash"`
	ParentHash   string        `json:"parent_hash"`
	Transactions []Transaction `json:"transactions"`
}

type Transaction struct {
	Hash  string `json:"hash"`
	From  string `json:"from"`
	To    string `json:"to"`
	Value big.Int `json:"value"`
}

