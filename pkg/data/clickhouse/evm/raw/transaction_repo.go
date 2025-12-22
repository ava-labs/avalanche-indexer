package raw

import (
	"math/big"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ava-labs/libevm/common"
)

type Transaction struct {
	Hash common.Hash
	BlockNumber big.Int
	To common.Address
	From common.Address
}

type Repo struct {
	conn clickhouse.Conn
}

func NewRepo(conn clickhouse.Conn) *Repo {
	return &Repo{conn: conn}
}

func (t *Repo) TableName() string {
	return "transactions"
}

func (t *Repo) Insert(tx Transaction) error {
	// Implementation for inserting the transaction into ClickHouse
	return nil
}

func (t *Repo) GetByHash(hash common.Hash) (*Transaction, error) {
	// Implementation for retrieving a transaction by its hash from ClickHouse
	return nil, nil
}
