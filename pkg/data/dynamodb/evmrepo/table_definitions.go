package evmrepo

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// HistoryTableDefinition returns the CreateTableInput for the history table.
// This must match the legacy analytics table schema exactly for glacier-api compatibility.
func HistoryTableDefinition(tableName string) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("isBlock"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("blockNumberKey"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("blockHashKey"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("blockSk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("contractAddressKey"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("deployedContractAddressKey"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("contractDeployerAddress"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("contractAddress#tokenId"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("latest-blocks-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("isBlock"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("blockSk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String("block-hash-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("blockHashKey"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("blockSk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String("block-number-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("blockNumberKey"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("blockSk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String("deployed-contract-address-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("deployedContractAddressKey"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String("contract-address-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("contractAddressKey"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String("contract-address-token-id-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("contractAddress#tokenId"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String("contract-deployer-address-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("contractDeployerAddress"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	}
}

// ERCTableDefinition returns the CreateTableInput for the ERC metadata table.
func ERCTableDefinition(tableName string) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("erc20ContractAddressMetadataKey"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("erc721ContractAddressMetadataKey"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("erc1155ContractAddressMetadataKey"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("erc721OwnerAddress"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("collectibleOwnerAddress"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("erc20s-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("erc20ContractAddressMetadataKey"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String("erc721s-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("erc721ContractAddressMetadataKey"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String("erc1155s-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("erc1155ContractAddressMetadataKey"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String("erc721-owners-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("erc721OwnerAddress"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String("collectible-owners-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("collectibleOwnerAddress"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	}
}
