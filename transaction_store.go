package dtpc

import (
	"time"

	"github.com/google/uuid"
	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// TransactionState indicates the current state of a transaction.
type TransactionState int

const (
	Pending TransactionState = iota
	Applied
	Done
	Canceling
	Cancelled
)

// TransactionStore contains required dependencies of TransactionStore
type TransactionStore struct {
	db        dynamodbiface.DynamoDBAPI
	tableName string
}

// Transaction contains data that will be stored in the sql.
type Transaction struct {
	// partition key, unique per transaction
	ID string `json:"id"`
	// GSI range key, unique, consist of sourceID, destinationID and the current timestamp
	TransactionReference string `json:"transaction_reference"`
	// GSI partition key, shows the state of a transaction
	TransactionState TransactionState `json:"transaction_state"`
	// ID of the source account
	Source string `json:"source"`
	// ID of the destination account
	Destination string `json:"destination"`
	// Data of a transaction
	Value interface{} `json:"value"`
	// Time of the latest modification to the transaction document
	LastModified time.Time `json:"last_modified"`
}

// NewTransactionStore initialises a new TransactionStore instance with a given sql instance.
func NewTransactionStore(db dynamodbiface.DynamoDBAPI, tableName string) *TransactionStore {
	return &TransactionStore{
		db:        db,
		tableName: tableName,
	}
}

// Insert adds transaction document to the transaction table.
// source and destination are ID values of the accounts that will be updated.
// data contains information of a transaction such as the currencyID and the amount to be transferred between two accounts.
func (ts *TransactionStore) Insert(ctx context.Context, source, destination, reference string, data interface{}) (string, error) {
	id := uuid.New().String()

	t := Transaction{
		ID:                   id,
		TransactionReference: reference,
		Source:               source,
		Destination:          destination,
		Value:                data,
		TransactionState:     Pending,
		LastModified:         time.Now(),
	}

	item, err := dynamodbattribute.MarshalMap(t)
	if err != nil {
		return id, err
	}

	in := &dynamodb.PutItemInput{
		TableName: aws.String(ts.tableName),
		Item:      item,
	}
	if _, err := ts.db.PutItem(in); err != nil {
		return id, err
	}
	return id, nil
}

// UpdateState updates the state of a transaction document.
func (ts *TransactionStore) UpdateState(ctx context.Context, id string, newState TransactionState) (*Transaction, error) {
	pk := map[string]string{
		"id": id,
	}
	key, err := dynamodbattribute.MarshalMap(pk)
	if err != nil {
		return nil, err
	}

	valMap := map[string]interface{}{
		":v": newState,
		":t": time.Now(),
	}
	vals, err := dynamodbattribute.MarshalMap(valMap)
	if err != nil {
		return nil, err
	}

	in := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(ts.tableName),
		Key:                       key,
		UpdateExpression:          aws.String("SET transaction_state = :v, last_modified = :t"),
		ExpressionAttributeValues: vals,
		ReturnValues:              aws.String("ALL_NEW"),
	}

	res, err := ts.db.UpdateItem(in)
	if err != nil {
		return nil, err
	}

	tr := &Transaction{}
	if err := dynamodbattribute.UnmarshalMap(res.Attributes, tr); err != nil {
		return nil, err
	}

	return tr, nil
}

// GetTransaction retrieves a transaction document by its ID value.
func (ts *TransactionStore) GetTransaction(ctx context.Context, id string) (*Transaction, error) {
	pk := map[string]string{
		"id": id,
	}
	key, err := dynamodbattribute.MarshalMap(pk)
	if err != nil {
		return nil, err
	}

	in := &dynamodb.GetItemInput{
		TableName: aws.String(ts.tableName),
		Key:       key,
	}

	res, err := ts.db.GetItem(in)
	if err != nil {
		return nil, err
	}

	t := &Transaction{}
	if err := dynamodbattribute.UnmarshalMap(res.Item, t); err != nil {
		return nil, err
	}

	return t, nil
}

// GetTransactionsInState gets all transaction documents of given state, source and destination accounts.
func (ts *TransactionStore) GetTransactionsInState(ctx context.Context, state TransactionState, query string) ([]*Transaction, error) {
	valMap := map[string]interface{}{
		":st": state,
		":tr": query,
	}
	vals, err := dynamodbattribute.MarshalMap(valMap)
	if err != nil {
		return nil, err
	}

	in := &dynamodb.QueryInput{
		TableName:                 aws.String(ts.tableName),
		IndexName:                 aws.String("state-index"),
		KeyConditionExpression:    aws.String("transaction_state = :st and begins_with (transaction_reference, :tr)"),
		ExpressionAttributeValues: vals,
	}

	res, err := ts.db.Query(in)
	if err != nil {
		return nil, err
	}

	transactions := []*Transaction{}
	if err := dynamodbattribute.UnmarshalListOfMaps(res.Items, &transactions); err != nil {
		return nil, err
	}

	return transactions, nil
}

// GetAllTransactionsInState gets all transcation documents of a given state.
// GetAllTransactionsInState is used for recovering all incomplete/failed transactions.
func (ts *TransactionStore) GetAllTransactionsInState(ctx context.Context, state TransactionState) ([]*Transaction, error) {
	valMap := map[string]interface{}{
		":st": state,
	}

	vals, err := dynamodbattribute.MarshalMap(valMap)
	if err != nil {
		return nil, err
	}

	namMap := map[string]*string{
		"#s": aws.String("Source"),
		"#v": aws.String("Value"),
	}

	in := &dynamodb.QueryInput{
		TableName:                 aws.String(ts.tableName),
		IndexName:                 aws.String("state-index"),
		KeyConditionExpression:    aws.String("transaction_state = :st"),
		ExpressionAttributeValues: vals,
		ExpressionAttributeNames:  namMap,
		ProjectionExpression:      aws.String("ID, #s, destination, #v, last_modified"),
	}

	res, err := ts.db.Query(in)
	if err != nil {
		return nil, err
	}

	transactions := []*Transaction{}
	if err := dynamodbattribute.UnmarshalListOfMaps(res.Items, &transactions); err != nil {
		return nil, err
	}

	return transactions, nil
}
