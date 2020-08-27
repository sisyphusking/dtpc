package example

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"dtpc"
)

var (
	tableName   = "accounts"
	hashKeyName = "id"
)

type AccountFakeDynamoDB struct {
	dynamodbiface.DynamoDBAPI
}

func NewAccountFakeDynamoDB() *AccountFakeDynamoDB {
	return &AccountFakeDynamoDB{}
}

func (db *AccountFakeDynamoDB) GetItem(in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	out := make(map[string]string)
	if err := dynamodbattribute.UnmarshalMap(in.Key, &out); err != nil {
		return nil, err
	}
	mockAccountDoc := AccountDoc{
		ID:                  out["id"],
		PendingTransactions: []string{"mock_transaction_id"},
	}
	item, err := dynamodbattribute.MarshalMap(mockAccountDoc)
	if err != nil {
		return nil, err
	}
	res := &dynamodb.GetItemOutput{
		Item: item,
	}
	return res, nil
}

func (db *AccountFakeDynamoDB) PutItem(in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}

func (db *AccountFakeDynamoDB) UpdateItem(in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

func TestGet(t *testing.T) {
	accountHandler := NewHandlerImpl(NewAccountFakeDynamoDB(), tableName, hashKeyName)
	retval := &AccountDoc{}
	mockID := "mock_account_id"
	if err := accountHandler.Get(context.Background(), mockID, retval); err != nil {
		t.Fatal(err)
	}

	if retval.GetID() != mockID {
		t.Fatal(fmt.Errorf("expected %s but received %s", mockID, retval.GetID()))
	}
}

func TestPut(t *testing.T) {
	accountHandler := NewHandlerImpl(NewAccountFakeDynamoDB(), tableName, hashKeyName)
	mockAccountDoc := AccountDoc{
		ID: "mock_account_id",
	}
	if err := accountHandler.Put(context.Background(), mockAccountDoc); err != nil {
		t.Fatal(err)
	}
}

func TestUpdate(t *testing.T) {
	accountHandler := NewHandlerImpl(NewAccountFakeDynamoDB(), tableName, hashKeyName)
	mockSourceAccountID := "mock_source_account_id"
	mockDestinationAccountID := "mock_destination_account_id"
	mockTransactionID := "mock_transaction_id"
	mockTransferReq := dtpc.Request{
		Source:      mockSourceAccountID,
		Destination: mockDestinationAccountID,
		Data: Item{
			ID:     "mock_transfer_request_id",
			Amount: 10,
		},
	}

	methods := []string{
		mockSourceAccountID,
		mockDestinationAccountID,
	}
	for _, m := range methods {
		if err := accountHandler.Update(context.Background(), m, mockTransactionID, mockTransferReq); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCommit(t *testing.T) {
	accountHandler := NewHandlerImpl(NewAccountFakeDynamoDB(), tableName, hashKeyName)
	mockAccountID := "mock_account_id"
	mockTransactionID := "mock_transaction_id"

	if err := accountHandler.Commit(context.Background(), mockAccountID, mockTransactionID); err != nil {
		t.Fatal(err)
	}
}

func TestRollback(t *testing.T) {
	accountHandler := NewHandlerImpl(NewAccountFakeDynamoDB(), tableName, hashKeyName)
	mockSourceAccountID := "mock_source_account_id"
	mockDestinationAccountID := "mock_destination_account_id"
	mockTransactionID := "mock_transaction_id"
	mockTransferReq := dtpc.Request{
		Source:      mockSourceAccountID,
		Destination: mockDestinationAccountID,
		Data: Item{
			ID:     "mock_transfer_request_id",
			Amount: 10,
		},
	}

	methods := []string{
		mockSourceAccountID,
		mockDestinationAccountID,
	}
	for _, m := range methods {
		if err := accountHandler.Rollback(context.Background(), m, mockTransactionID, mockTransferReq); err != nil {
			t.Fatal(err)
		}
	}
}
