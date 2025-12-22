package erc20

import (
	"context"
	"math/big"
)

type Token struct {
	PK                              string `dynamodbav:"pk"`
	SK                              string `dynamodbav:"sk"`
	ContractAddress                 string `dynamodbav:"contractAddress"`
	Erc20ContractAddressMetadataKey string `dynamodbav:"erc20ContractAddressMetadataKey"`
	ErcType                         string `dynamodbav:"ercType"`
	TokenStatus                     string `dynamodbav:"tokenStatus"`
	LastUpdated                     int64  `dynamodbav:"lastUpdated"`
	StatusUpdateTimestamp           int64  `dynamodbav:"statusUpdateTimestamp"`
}

func (t * Token) GetToken(ctx context.Context, addr string, evmChainID *big.Int) (*Token, error) {
	return &Token{}, nil
}
