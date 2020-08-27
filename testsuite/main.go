package main

import (
	"log"
	"time"

	"dtpc"
	"dtpc/testsuite/example"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"golang.org/x/net/context"
)

const LocalEndpoint = "http://localhost:8000"
const AWSRegion = "ap-southeast-2"

func main() {
	// Initialise DynamoDB Instance
	dynamodbCli, err := getLocalDynamoDBInstance()
	if err != nil {
		panic(err.Error())
	}

	// initialize database tables
	err = setup(dynamodbCli)
	defer teardown(dynamodbCli)
	if err != nil {
		panic(err.Error())
	}

	// Setup Account Handler
	accountHandler := example.NewHandlerImpl(dynamodbCli, "accounts", "ID")
	if err := setupAccounts(accountHandler); err != nil {
		panic(err.Error())
	}

	// Setup Transaction Store
	transactionStore := dtpc.NewTransactionStore(dynamodbCli, "transactions")

	// Setup Transaction Service
	srv := dtpc.NewService(transactionStore, accountHandler)
	ctx := context.Background()
	if err := testSingleTransaction(ctx, srv); err != nil {
		panic(err.Error())
	}

	if err := testRecoverTransactions(ctx, srv); err != nil {
		panic(err.Error())
	}

	log.Println("All tests passed")
}

func setup(db *dynamodb.DynamoDB) error {
	for _, table := range tables {
		if _, err := db.CreateTable(createTableInput(table)); err != nil {
			return err
		}
	}
	return nil
}

func teardown(db *dynamodb.DynamoDB) error {
	for _, table := range tables {
		if _, err := db.DeleteTable(deleteTableInput(table.TableName)); err != nil {
			return err
		}
	}
	return nil
}

func setupAccounts(ah dtpc.AccountHandler) error {
	resources := make(map[string]example.Item)
	resources["item1"] = example.Item{
		ID:     "item1",
		Amount: 100,
	}
	resources["item2"] = example.Item{
		ID:     "item2",
		Amount: 100,
	}

	accounts := []*example.AccountDoc{
		getAccountDoc("account1", resources),
		getAccountDoc("account2", resources),
		getAccountDoc("account3", resources),
		getAccountDoc("account4", resources),
	}

	for _, account := range accounts {
		if err := ah.Put(context.Background(), account); err != nil {
			return err
		}
	}

	return nil
}

func testSingleTransaction(ctx context.Context, srv *dtpc.Service) error {
	req := getTransactionRequest("account1", "account2", "item1", 10)

	if _, err := srv.StartTransaction(ctx, req); err != nil {
		return err
	}
	return nil
}

func testRecoverTransactions(ctx context.Context, srv *dtpc.Service) error {
	t := time.Now().Add(-10000 * time.Millisecond)
	return srv.RecoverTransactions(ctx, t)
}

func getLocalDynamoDBInstance() (*dynamodb.DynamoDB, error) {
	creds, err := getStaticAwsCredentials()
	if err != nil {
		return nil, err
	}

	awsConf := &aws.Config{
		Endpoint:    aws.String(LocalEndpoint),
		Region:      aws.String(AWSRegion),
		Credentials: creds,
	}

	sess, err := session.NewSession(awsConf)
	if err != nil {
		return nil, err
	}

	return dynamodb.New(sess), nil
}

func getStaticAwsCredentials() (*credentials.Credentials, error) {
	awsCreds := credentials.NewStaticCredentials("test", "test", "")
	awsCreds.Expire()
	// The returning value should not be logged as it contains credential information
	_, err := awsCreds.Get()
	if err != nil {
		return nil, err
	}
	return awsCreds, nil
}

func getTransactionRequest(source, destination, itemID string, itemQuantity int) dtpc.Request {
	return dtpc.Request{
		Source:      source,
		Destination: destination,
		Data: example.Item{
			ID:     itemID,
			Amount: itemQuantity,
		}}
}

func getAccountDoc(accountID string, resources map[string]example.Item) *example.AccountDoc {
	return &example.AccountDoc{
		ID:                  accountID,
		Resources:           resources,
		PendingTransactions: []string{},
		Version:             0,
	}
}

type TableInfo struct {
	TableName       string
	PrimaryKey      string
	SortKey         string
	SortKeyType     string
	ReadThroughput  int64
	WriteThroughput int64
	Indexes         []IndexInfo
}

type IndexInfo struct {
	IndexName       string
	PrimaryKey      string
	PrimaryKeyType  string
	SortKey         string
	SortKeyType     string
	ReadThroughput  int64
	WriteThroughput int64
}

var tables = []TableInfo{
	TableInfo{"accounts", "ID", "", "", 5, 5, nil},
	TableInfo{"transactions", "id", "", "S", 5, 5, []IndexInfo{
		IndexInfo{"state-index", "transaction_state", "N", "transaction_reference", "S", 5, 5},
	}},
}

func createTableInput(table TableInfo) *dynamodb.CreateTableInput {
	input := &dynamodb.CreateTableInput{
		TableName: aws.String(table.TableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(table.PrimaryKey),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(table.PrimaryKey),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(table.ReadThroughput),
			WriteCapacityUnits: aws.Int64(table.WriteThroughput),
		},
	}
	if table.SortKey != "" {
		input.AttributeDefinitions = append(input.AttributeDefinitions,
			&dynamodb.AttributeDefinition{
				AttributeName: aws.String(table.SortKey),
				AttributeType: aws.String(table.SortKeyType),
			},
		)

		input.KeySchema = append(input.KeySchema,
			&dynamodb.KeySchemaElement{
				AttributeName: aws.String(table.SortKey),
				KeyType:       aws.String("RANGE"),
			},
		)
	}
	if len(table.Indexes) > 0 {
		gsi := []*dynamodb.GlobalSecondaryIndex{}
		for _, index := range table.Indexes {
			gsi = append(gsi, newGlobalSecondaryIndex(index))
			input.AttributeDefinitions = append(input.AttributeDefinitions,
				&dynamodb.AttributeDefinition{
					AttributeName: aws.String(index.PrimaryKey),
					AttributeType: aws.String(index.PrimaryKeyType),
				},
				&dynamodb.AttributeDefinition{
					AttributeName: aws.String(index.SortKey),
					AttributeType: aws.String(index.SortKeyType),
				})
		}
		input.GlobalSecondaryIndexes = gsi
	}

	return input
}

func newGlobalSecondaryIndex(index IndexInfo) *dynamodb.GlobalSecondaryIndex {
	input := &dynamodb.GlobalSecondaryIndex{
		IndexName: aws.String(index.IndexName),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(index.PrimaryKey),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String(index.SortKey),
				KeyType:       aws.String("RANGE"),
			},
		},
		Projection: &dynamodb.Projection{
			NonKeyAttributes: []*string{
				aws.String("ID"),
			},
			ProjectionType: aws.String(dynamodb.ProjectionTypeInclude),
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(index.ReadThroughput),
			WriteCapacityUnits: aws.Int64(index.WriteThroughput),
		},
	}
	return input
}

func deleteTableInput(tableName string) *dynamodb.DeleteTableInput {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}
	return input
}
