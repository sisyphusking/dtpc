package dtpc

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"github.com/google/uuid"
	"context"

)

type FakeTransactionStore struct {
	TransactionHandler
	store map[string]*Transaction
}

func NewFakeTransactionStore() *FakeTransactionStore {
	return &FakeTransactionStore{
		store: make(map[string]*Transaction),
	}
}

// Insert simulates the insert behaviour and stores a transaction in map.
func (fts *FakeTransactionStore) Insert(ctx context.Context, source, destination, reference string, data interface{}) (string, error) {
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
	fts.store[id] = &t
	return id, nil
}

func (fts *FakeTransactionStore) UpdateState(ctx context.Context, id string, newState TransactionState) (*Transaction, error) {
	doc, ok := fts.store[id]
	if !ok {
		return nil, fmt.Errorf("transaction with id %s does not exist", id)
	}

	doc.TransactionState = newState

	fts.store[id] = doc
	return doc, nil
}

func (fts *FakeTransactionStore) GetTransactionsInState(ctx context.Context, state TransactionState, query string) ([]*Transaction, error) {
	transactions := make([]*Transaction, 0)
	for _, t := range fts.store {
		if t.TransactionState == state && strings.HasPrefix(t.TransactionReference, query) {
			transactions = append(transactions, t)
		}
	}
	return transactions, nil
}

func (fts *FakeTransactionStore) GetAllTransactionsInState(ctx context.Context, state TransactionState) ([]*Transaction, error) {
	transactions := make([]*Transaction, 0)
	for _, t := range fts.store {
		if t.TransactionState == state {
			transactions = append(transactions, t)
		}
	}
	return transactions, nil
}

// TransactionMethod contains valid methods for currency transfer.
type TransactionMethod int

const (
	Increment TransactionMethod = iota
	Decrement
)

var (
	errPendingTransactionIDNotFound = errors.New("pending transaction id not found")
)

type MockAccountDoc struct {
	ID                  string
	Resources           map[string]MockItem
	PendingTransactions []string
	Version             int
}

func (a MockAccountDoc) GetID() string {
	return a.ID
}
func (a MockAccountDoc) GetPendingTransactions() []string {
	return a.PendingTransactions
}
func (a MockAccountDoc) GetVersion() int {
	return a.Version
}

// MockItems contains required data for an MockItem.
type MockItem struct {
	ID     string
	Amount int
}

type FakeAccountStore struct {
	AccountHandler
	store map[string]MockAccountDoc
}

func NewFakeAccountStore() *FakeAccountStore {
	return &FakeAccountStore{
		store: make(map[string]MockAccountDoc),
	}
}

func (fas *FakeAccountStore) Put(ctx context.Context, doc Account) error {
	ad, ok := doc.(MockAccountDoc)
	if !ok {
		return fmt.Errorf("failed to assert doc %v into type MockAccountDoc", doc)
	}
	fas.store[doc.GetID()] = ad
	return nil
}

// Update simulate account update process by updating an existing account record in map.
func (fas *FakeAccountStore) Update(ctx context.Context, accountID, transactionID string, tr Request) error {
	reqData, ok := tr.Data.(MockItem)
	if !ok {
		return fmt.Errorf("failed to unmarshalling transaction request %s into type MockItem", tr)
	}
	method := Decrement
	if accountID == tr.Destination {
		method = Increment
	}

	ad, ok := fas.store[accountID]
	if !ok {
		return fmt.Errorf("account id %s does not exist", accountID)
	}

	ad.PendingTransactions = append(ad.PendingTransactions, transactionID)
	ad.Version = ad.Version + 1

	resource, ok := ad.Resources[reqData.ID]
	if !ok {
		return fmt.Errorf("failed to retrieve resource with ID %s", reqData.ID)
	}

	switch method {
	case Increment:
		resource.Amount = resource.Amount + reqData.Amount
	case Decrement:
		if resource.Amount < reqData.Amount {
			return fmt.Errorf("insufficient amount for resource %s", reqData.ID)
		}
		resource.Amount = resource.Amount - reqData.Amount
	}
	ad.Resources[reqData.ID] = resource
	fas.store[accountID] = ad

	return nil
}

func (fas *FakeAccountStore) Commit(ctx context.Context, accountID, transactionID string) error {
	ad, ok := fas.store[accountID]
	if !ok {
		return fmt.Errorf("account id %s does not exist", accountID)
	}

	i, err := getPendingTransactionIndex(ad.GetPendingTransactions(), transactionID)
	if err != nil {
		return err
	}

	ad.PendingTransactions = ad.PendingTransactions[:i+copy(ad.PendingTransactions[i:], ad.PendingTransactions[i+1:])]
	ad.Version = ad.Version + 1

	fas.store[accountID] = ad

	return nil
}

func (fas *FakeAccountStore) Rollback(ctx context.Context, accountID, transactionID string, tr Request) error {
	reqData, ok := tr.Data.(MockItem)
	if !ok {
		return fmt.Errorf("failed to unmarshalling transaction request %s into type MockItem", tr)
	}
	method := Increment
	if accountID == tr.Destination {
		method = Decrement
	}

	ad, ok := fas.store[accountID]
	if !ok {
		return fmt.Errorf("account id %s does not exist", accountID)
	}

	i, err := getPendingTransactionIndex(ad.GetPendingTransactions(), transactionID)
	if err != nil {
		return err
	}

	ad.PendingTransactions = ad.PendingTransactions[:i+copy(ad.PendingTransactions[i:], ad.PendingTransactions[i+1:])]
	ad.Version = ad.Version + 1

	resource, ok := ad.Resources[reqData.ID]
	if !ok {
		return fmt.Errorf("failed to retrieve resource with ID %s", reqData.ID)
	}
	switch method {
	case Increment:
		resource.Amount = resource.Amount + reqData.Amount
	case Decrement:
		if resource.Amount < reqData.Amount {
			return fmt.Errorf("insufficient amount for resource %s", reqData.ID)
		}
		resource.Amount = resource.Amount - reqData.Amount
	}
	ad.Resources[reqData.ID] = resource
	fas.store[accountID] = ad

	return nil
}

func TestStartTransaction(t *testing.T) {
	ctx := context.Background()
	fts := NewFakeTransactionStore()
	fas := NewFakeAccountStore()
	service := NewService(fts, fas)

	mockReq := Request{
		Source:      "mock_account_id_1",
		Destination: "mock_account_id_2",
		Data: MockItem{
			ID:     "mock_transfer_request_item_id",
			Amount: 10,
		},
	}

	mockItemMap := make(map[string]MockItem)
	mockItemMap["mock_transfer_request_item_id"] = MockItem{
		ID:     "mock_transfer_request_item_id",
		Amount: 10,
	}
	docs := []MockAccountDoc{
		{
			ID:                  "mock_account_id_1",
			Resources:           mockItemMap,
			PendingTransactions: make([]string, 1),
			Version:             0,
		},
		{
			ID:                  "mock_account_id_2",
			Resources:           mockItemMap,
			PendingTransactions: make([]string, 1),
			Version:             0,
		},
	}

	for _, doc := range docs {
		if err := fas.Put(ctx, doc); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := service.StartTransaction(ctx, mockReq); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverTransactions(t *testing.T) {
	ctx := context.Background()
	fts := NewFakeTransactionStore()
	fas := NewFakeAccountStore()
	service := NewService(fts, fas)

	ref := fmt.Sprintf("%s:%s", "mock_account_id_1", "mock_account_id_2")
	transactionID1, err := fts.Insert(ctx, "mock_account_id_1", "mock_account_id_2", ref, MockItem{
		ID:     "mock_transfer_request_item_id",
		Amount: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	transactionID2, err := fts.Insert(ctx, "mock_account_id_1", "mock_account_id_2", ref, MockItem{
		ID:     "mock_transfer_request_item_id",
		Amount: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	mockItemMap1 := make(map[string]MockItem)
	mockItemMap1["mock_transfer_request_item_id"] = MockItem{
		ID:     "mock_transfer_request_item_id",
		Amount: 30,
	}
	mockItemMap2 := make(map[string]MockItem)
	mockItemMap2["mock_transfer_request_item_id"] = MockItem{
		ID:     "mock_transfer_request_item_id",
		Amount: 30,
	}

	docs := []MockAccountDoc{
		{
			ID:                  "mock_account_id_1",
			Resources:           mockItemMap1,
			PendingTransactions: []string{transactionID1, transactionID2},
			Version:             0,
		},
		{
			ID:                  "mock_account_id_2",
			Resources:           mockItemMap2,
			PendingTransactions: []string{transactionID1, transactionID2},
			Version:             0,
		},
	}

	for _, doc := range docs {
		if err := fas.Put(ctx, doc); err != nil {
			t.Fatal(err)
		}
	}

	fts.store[transactionID1].TransactionState = Applied

	if err := service.RecoverTransactions(ctx, time.Now().Add(100*time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	if fts.store[transactionID1].TransactionState != Done {
		t.Fatal(fmt.Printf("expected transaction state to be %d but got %d", Done, fts.store[transactionID1].TransactionState))
	}
	if fts.store[transactionID2].TransactionState != Cancelled {
		t.Fatal(fmt.Printf("expected transaction state to be %d but got %d", Cancelled, fts.store[transactionID2].TransactionState))
	}
	if fas.store["mock_account_id_1"].Resources["mock_transfer_request_item_id"].Amount != 40 {
		t.Fatal(fmt.Printf("expected account 1 currency amount to be %d but got %d", 40, fas.store["mock_account_id_1"].Resources["mock_transfer_request_item_id"].Amount))
	}
	if fas.store["mock_account_id_2"].Resources["mock_transfer_request_item_id"].Amount != 20 {
		t.Fatal(fmt.Printf("expected account 2 currency amount to be %d but got %d", 20, fas.store["mock_account_id_2"].Resources["mock_transfer_request_item_id"].Amount))
	}
	if len(fas.store["mock_account_id_1"].GetPendingTransactions()) > 0 {
		t.Fatal(fmt.Printf("expected no pending transactions in account id 1 but got %v", fas.store["mock_account_id_1"].GetPendingTransactions()))
	}
	if len(fas.store["mock_account_id_2"].GetPendingTransactions()) > 0 {
		t.Fatal(fmt.Printf("expected no pending transactions in account id 2 but got %v", fas.store["mock_account_id_2"].GetPendingTransactions()))
	}
}

func getPendingTransactionIndex(pts []string, st string) (int, error) {
	for i, pt := range pts {
		if pt == st {
			return i, nil
		}
	}
	return 0, errPendingTransactionIDNotFound
}
