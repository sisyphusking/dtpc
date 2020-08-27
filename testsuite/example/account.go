package example

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"golang.org/x/net/context"

	"dtpc"
)

const maxUpdateAttempts = 10
const UpdateRetryInterval = 100

// TransactionMethod contains valid methods for currency transfer.
type TransactionMethod int

const (
	Increment TransactionMethod = iota
	Decrement
)

var (
	errPendingTransactionIDNotFound = errors.New("pending transaction id not found")
)

// AccountDoc contains required data of account documents
type AccountDoc struct {
	// Partition key, unique per account
	ID string
	// A map of itemID to item data
	Resources map[string]Item
	// A list of pending transactions for an account
	PendingTransactions []string
	// Version number of an account document required for Optimistic Locking
	Version int
}

func (a AccountDoc) GetID() string {
	return a.ID
}
func (a AccountDoc) GetPendingTransactions() []string {
	return a.PendingTransactions
}
func (a AccountDoc) GetVersion() int {
	return a.Version
}

// Items contains required data for an item.
type Item struct {
	ID     string
	Amount int
}

// HandlerImpl is an implementation of the AccountHandler interface required by Transaction Services.
type HandlerImpl struct {
	db          dynamodbiface.DynamoDBAPI
	tableName   string
	hashKeyName string
}

// NewHandlerImpl initialises a new instance of an Account Handler implementation
func NewHandlerImpl(db dynamodbiface.DynamoDBAPI, tableName, hashKeyName string) *HandlerImpl {
	return &HandlerImpl{
		db:          db,
		tableName:   tableName,
		hashKeyName: hashKeyName,
	}
}

// Get retrieves an account document from data store
func (h *HandlerImpl) Get(ctx context.Context, accountID string, retval dtpc.Account) error {
	pk := map[string]string{
		h.hashKeyName: accountID,
	}

	key, err := dynamodbattribute.MarshalMap(pk)
	if err != nil {
		return err
	}

	in := &dynamodb.GetItemInput{
		TableName: aws.String(h.tableName),
		Key:       key,
	}
	res, err := h.db.GetItem(in)
	if err != nil {
		return err
	}

	return dynamodbattribute.UnmarshalMap(res.Item, retval)
}

// Put inserts a new Account document to the sql
func (h *HandlerImpl) Put(ctx context.Context, doc dtpc.Account) error {
	item, err := dynamodbattribute.MarshalMap(doc)
	if err != nil {
		return err
	}
	// This is a temporary workaround due to dynamodbattribute inserts Null AttributeValue instead of empty Map when Marshaling empty map.
	// There have been some discussions around this: https://github.com/aws/aws-sdk-go/issues/682
	if item["Resources"] == nil {
		item["Resources"] = &dynamodb.AttributeValue{M: make(map[string]*dynamodb.AttributeValue)}
	}
	item["PendingTransactions"] = &dynamodb.AttributeValue{L: make([]*dynamodb.AttributeValue, 0)}

	in := &dynamodb.PutItemInput{
		TableName: aws.String(h.tableName),
		Item:      item,
	}

	if _, err := h.db.PutItem(in); err != nil {
		return err
	}

	return nil
}

// Update updates account documents by applying a transaction and appending the ID of the transaction to the pendingTransaction list.
// Optimistic locking is applied to support concurrent updates to a single account doccument.
func (h *HandlerImpl) Update(ctx context.Context, accountID, transactionID string, tr dtpc.Request) error {
	reqData, ok := tr.Data.(Item)
	if !ok {
		return fmt.Errorf("failed to unmarshalling transaction request %s into type Item", tr)
	}
	method := Decrement
	if accountID == tr.Destination {
		method = Increment
	}

	for i := 0; i < maxUpdateAttempts; i++ {
		err := h.findAndModify(ctx, accountID, transactionID, reqData, method)
		if err == nil {
			// Operation succeeded
			return nil
		}
		if !h.isAWSErrorConditionalCheckFailed(err) {
			return err
		}
		time.Sleep(UpdateRetryInterval * time.Millisecond)
	}
	return fmt.Errorf("Update failed because the process has reached the maximum number of retry attempts. transactionID: %s, accountID: %s", transactionID, accountID)
}

func (h *HandlerImpl) findAndModify(ctx context.Context, accountID, transactionID string, tr Item, method TransactionMethod) error {
	accountDoc := AccountDoc{}
	if err := h.Get(ctx, accountID, &accountDoc); err != nil {
		return err
	}
	currentVersion := accountDoc.GetVersion()

	pk := map[string]string{
		h.hashKeyName: accountID,
	}

	key, err := dynamodbattribute.MarshalMap(pk)
	if err != nil {
		return err
	}

	valMap := map[string]interface{}{
		":tid":    []string{transactionID},
		":q":      tr.Amount,
		":cas":    currentVersion,
		":newcas": currentVersion + 1,
	}

	vals, err := dynamodbattribute.MarshalMap(valMap)
	if err != nil {
		return err
	}

	namMap := map[string]*string{
		"#pt": aws.String("PendingTransactions"),
		"#ii": aws.String(tr.ID),
		"#ia": aws.String("Amount"),
		"#ve": aws.String("Version"),
	}

	// method string
	var m string
	// condition expression string
	var ce string

	switch method {
	case Increment:
		m = "+"
		ce = fmt.Sprintf("#ve = :cas")
	case Decrement:
		m = "-"
		ce = fmt.Sprintf("Resources.#ii.#ia > :q AND #ve = :cas")
	default:
		return fmt.Errorf("unsupported transaction method %d", method)
	}

	ue := aws.String(fmt.Sprintf("SET #ve = :newcas, #pt = list_append (:tid, #pt), Resources.#ii.#ia = Resources.#ii.#ia %s :q", m))

	in := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(h.tableName),
		Key:                       key,
		UpdateExpression:          ue,
		ExpressionAttributeValues: vals,
		ExpressionAttributeNames:  namMap,
		ConditionExpression:       aws.String(ce),
	}

	if _, err := h.db.UpdateItem(in); err != nil {
		return err
	}
	return nil
}

// Commit updates an account document by removing a transaction ID from its PendingTransaction list.
// Optimistic locking is applied to support concurrent updates to a single account doccument.
func (h *HandlerImpl) Commit(ctx context.Context, accountID, transactionID string) error {
	//加入了重试机制
	for i := 0; i < maxUpdateAttempts; i++ {
		err := h.commit(ctx, accountID, transactionID)
		if err == nil {
			// Operation succeeded
			return nil
		}
		if !h.isAWSErrorConditionalCheckFailed(err) {
			return err
		}
		time.Sleep(UpdateRetryInterval * time.Millisecond)
	}
	return fmt.Errorf("Commit failed because the process has reached the maximum number of retry attempts. transactionID: %s, accountID: %s", transactionID, accountID)
}

func (h *HandlerImpl) commit(ctx context.Context, accountID, transactionID string) error {
	accountDoc := AccountDoc{}
	if err := h.Get(ctx, accountID, &accountDoc); err != nil {
		return err
	}
	currentVersion := accountDoc.GetVersion()

	pts := accountDoc.GetPendingTransactions()
	pendingTransactionIndex, err := getPendingTransactionIndex(pts, transactionID)
	if h.IsErrorPendingTransactionIDNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}

	pk := map[string]string{
		h.hashKeyName: accountID,
	}

	key, err := dynamodbattribute.MarshalMap(pk)
	if err != nil {
		return err
	}

	namMap := map[string]*string{
		"#pt": aws.String("PendingTransactions"),
		"#ve": aws.String("Version"),
	}

	valMap := map[string]interface{}{
		":cas":    currentVersion,
		":newcas": currentVersion + 1,
	}
	vals, err := dynamodbattribute.MarshalMap(valMap)
	if err != nil {
		return err
	}

	ce := fmt.Sprintf("#ve = :cas")

	in := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(h.tableName),
		Key:                       key,
		UpdateExpression:          aws.String(fmt.Sprintf("SET #ve = :newcas REMOVE #pt[%d]", pendingTransactionIndex)),
		ExpressionAttributeValues: vals,
		ExpressionAttributeNames:  namMap,
		ConditionExpression:       aws.String(ce),
	}

	if _, err := h.db.UpdateItem(in); err != nil {
		return err
	}
	return nil
}

// Rollback recovers a failed transaction by applying the opposite logic of currency transfer
// and removes a transaction ID from its PendingTransaction list.
// Optimistic locking is applied to support concurrent updates to a single account doccument.
func (h *HandlerImpl) Rollback(ctx context.Context, accountID, transactionID string, tr dtpc.Request) error {
	reqData, ok := tr.Data.(Item)
	if !ok {
		return fmt.Errorf("failed to unmarshalling transaction request %s into type Item", tr)
	}
	method := Increment
	if accountID == tr.Destination {
		method = Decrement
	}

	for i := 0; i < maxUpdateAttempts; i++ {
		err := h.rollback(ctx, accountID, transactionID, reqData, method)
		if err == nil {
			// Operation succeeded
			return nil
		}
		if !h.isAWSErrorConditionalCheckFailed(err) {
			return err
		}
		time.Sleep(UpdateRetryInterval * time.Millisecond)
	}
	return fmt.Errorf("Rollback failed because the process has reached the maximum number of retry attempts. transactionID: %s, accountID: %s", transactionID, accountID)
}

func (h *HandlerImpl) rollback(ctx context.Context, accountID, transactionID string, tr Item, method TransactionMethod) error {
	accountDoc := AccountDoc{}
	if err := h.Get(ctx, accountID, &accountDoc); err != nil {
		return err
	}
	currentVersion := accountDoc.GetVersion()

	pts := accountDoc.GetPendingTransactions()
	pendingTransactionIndex, err := getPendingTransactionIndex(pts, transactionID)
	if err != nil {
		return err
	}

	pk := map[string]string{
		h.hashKeyName: accountID,
	}

	key, err := dynamodbattribute.MarshalMap(pk)
	if err != nil {
		return err
	}

	valMap := map[string]interface{}{
		":q":   tr.Amount,
		":cas": currentVersion,
	}

	vals, err := dynamodbattribute.MarshalMap(valMap)
	if err != nil {
		return err
	}

	namMap := map[string]*string{
		"#pt": aws.String("PendingTransactions"),
		"#ii": aws.String(tr.ID),
		"#ia": aws.String("Amount"),
		"#ve": aws.String("Version"),
	}

	// method string
	var m string
	// condition expression string
	var ce string

	switch method {
	case Increment:
		m = "+"
		ce = fmt.Sprintf("#ve = :cas")
	case Decrement:
		m = "-"
		ce = fmt.Sprintf("Resources.#ii.#ia > :q AND #ve = :cas")
	default:
		return fmt.Errorf("unsupported transaction method %d", method)
	}

	ue := aws.String(fmt.Sprintf("ADD #ve 1 REMOVE #pt[%d] SET Resources.#ii.#ia = Resources.#ii.#ia %s :q", pendingTransactionIndex, m))

	in := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(h.tableName),
		Key:                       key,
		UpdateExpression:          ue,
		ExpressionAttributeValues: vals,
		ExpressionAttributeNames:  namMap,
		ConditionExpression:       aws.String(ce),
	}

	if _, err := h.db.UpdateItem(in); err != nil {
		return err
	}

	return nil
}

// IsErrorPendingTransactionIDNotFound checks if a given error matches errPendingTransactionIDNotFound.
func (h *HandlerImpl) IsErrorPendingTransactionIDNotFound(err error) bool {
	return err == errPendingTransactionIDNotFound
}

// isAWSErrorConditionalCheckFailed checks if a given error matches dynamodb.ErrCodeConditionalCheckFailedException.
func (h *HandlerImpl) isAWSErrorConditionalCheckFailed(err error) bool {
	aerr, ok := err.(awserr.RequestFailure)
	if !ok {
		return false
	}
	return aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException
}

func getPendingTransactionIndex(pts []string, st string) (int, error) {
	for i, pt := range pts {
		if pt == st {
			return i, nil
		}
	}
	return 0, errPendingTransactionIDNotFound
}
