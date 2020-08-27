package dtpc

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type TransactioStoreFakeDynamoDB struct {
	dynamodbiface.DynamoDBAPI
}

func NewTransactioStoreFakeDynamoDB() *TransactioStoreFakeDynamoDB {
	return &TransactioStoreFakeDynamoDB{}
}

func (db *TransactioStoreFakeDynamoDB) PutItem(in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}

func (db *TransactioStoreFakeDynamoDB) UpdateItem(in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

func (db *TransactioStoreFakeDynamoDB) GetItem(in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	out := make(map[string]string)
	if err := dynamodbattribute.UnmarshalMap(in.Key, &out); err != nil {
		return nil, err
	}
	mockTransaction := Transaction{
		ID: out["id"],
	}
	item, err := dynamodbattribute.MarshalMap(mockTransaction)
	if err != nil {
		return nil, err
	}
	res := &dynamodb.GetItemOutput{
		Item: item,
	}
	return res, nil
}

func (db *TransactioStoreFakeDynamoDB) Query(in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	t1, err := dynamodbattribute.MarshalMap(Transaction{
		ID: "mock_transaction_id_1",
	})
	if err != nil {
		return nil, err
	}

	t2, err := dynamodbattribute.MarshalMap(Transaction{
		ID: "mock_transaction_id_2",
	})
	if err != nil {
		return nil, err
	}

	res := &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{t1, t2},
	}
	return res, nil
}

func TestInsert(t *testing.T) {
	ctx := context.Background()
	data := MockItem{
		ID:     "mock123456",
		Amount: 10,
	}

	store := NewTransactionStore(NewTransactioStoreFakeDynamoDB(), "transactions")
	ref := fmt.Sprintf("%s:%s", "mock_source_account_id", "mock_destination_account_id")
	id, err := store.Insert(ctx, "mock_source_account_id", "mock_destination_account_id", ref, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(id) < 1 {
		t.Fatal(fmt.Errorf("expected valid uuid but received nil"))
	}
}
func TestUpdateState(t *testing.T) {
	ctx := context.Background()
	store := NewTransactionStore(NewTransactioStoreFakeDynamoDB(), "transactions")

	states := []TransactionState{
		Pending,
		Applied,
		Done,
		Canceling,
		Cancelled,
	}

	for _, s := range states {
		if _, err := store.UpdateState(ctx, "mock_transaction_id", s); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGetTransaction(t *testing.T) {
	ctx := context.Background()
	store := NewTransactionStore(NewTransactioStoreFakeDynamoDB(), "transactions")

	id := "mock_transaction_id"
	tr, err := store.GetTransaction(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	if tr.ID != id {
		t.Fatal(fmt.Errorf("expected %s but received %s", id, tr.ID))
	}
}

func TestGetTransactionsInState(t *testing.T) {
	ctx := context.Background()
	store := NewTransactionStore(NewTransactioStoreFakeDynamoDB(), "transactions")

	states := []TransactionState{
		Pending,
		Applied,
		Done,
		Canceling,
		Cancelled,
	}

	for _, s := range states {
		query := fmt.Sprintf("%s:%s", "mock_transaction_source_id", "mock_transaction_destination_id")
		if _, err := store.GetTransactionsInState(ctx, s, query); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGetAllTransactionsInState(t *testing.T) {
	ctx := context.Background()
	store := NewTransactionStore(NewTransactioStoreFakeDynamoDB(), "transactions")

	states := []TransactionState{
		Pending,
		Applied,
		Done,
		Canceling,
		Cancelled,
	}

	for _, s := range states {
		if _, err := store.GetAllTransactionsInState(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
}
