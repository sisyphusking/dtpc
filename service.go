package dtpc

import (
	"time"
	"context"
)

// AccountHandler defines required methods of account data handling for transaction processes.
type AccountHandler interface {
	Get(ctx context.Context, accountID string, retval Account) error
	Put(ctx context.Context, doc Account) error
	Update(ctx context.Context, accountID, transactionID string, tr Request) error
	Rollback(ctx context.Context, accountID, transactionID string, tr Request) error
	Commit(ctx context.Context, accountID, transactionID string) error
	IsErrorPendingTransactionIDNotFound(err error) bool
}

// Account defines mandatory fields of account documents
type Account interface {
	GetID() string
	GetPendingTransactions() []string
	GetVersion() int
}

// TransactionHandler defines required methods of transaction data handling for transaction processes.
type TransactionHandler interface {
	Insert(ctx context.Context, source, destination, reference string, data interface{}) (string, error)
	UpdateState(ctx context.Context, id string, newState TransactionState) (*Transaction, error)
	GetTransaction(ctx context.Context, id string) (*Transaction, error)
	GetTransactionsInState(ctx context.Context, state TransactionState, query string) ([]*Transaction, error)
	GetAllTransactionsInState(ctx context.Context, state TransactionState) ([]*Transaction, error)
}

type Service struct {
	Ts TransactionHandler
	Ah AccountHandler
}

type Request struct {
	// ID of the data source
	Source string
	// ID of the data destination
	Destination string
	// the range key for querying and sorting transaction requests
	Reference string
	// the actual data being transferred
	Data interface{}
}

type Response struct {
	// ID of the transaction
	TransactionID string
	// Timestamp of last modified time
	LastModified int64
}

// NewService initialises a new instance of Transaction Service.
func NewService(th TransactionHandler, ah AccountHandler) *Service {
	return &Service{
		Ts: th,
		Ah: ah,
	}
}

// StartTransaction performs a single transaction based on the two phase commits logic.
func (s *Service) StartTransaction(ctx context.Context, req Request, callbacks ...func() error) (*Response, error) {
	// Insert new transaction with initial state
	transactionID, err := s.Ts.Insert(ctx, req.Source, req.Destination, req.Reference, req.Data)
	if err != nil {
		// Failed to append transaction, err is returned and no rollback required.
		return nil, err
	}

	if err := s.applyTransaction(ctx, req, transactionID, callbacks...); err != nil {
		if err := s.recoverFromError(ctx, transactionID, req, Pending); err != nil {
			return nil, err
		}
		return nil, err
	}

	tr, err := s.commitTransaction(ctx, req, transactionID)
	if err != nil {
		if err := s.recoverFromError(ctx, transactionID, req, Applied); err != nil {
			return nil, err
		}
		return nil, err
	}

	return &Response{
		TransactionID: transactionID,
		LastModified:  tr.LastModified.Unix(),
	}, nil
}

func (s *Service) GetTransactions(ctx context.Context, state TransactionState, query string) ([]*Transaction, error) {
	return s.Ts.GetTransactionsInState(ctx, state, query)
}

// RecoverTransactions provides an option to correct failed or incomplete transaction due to extreme situations such as Network outage or Database outage.
// RecoverTransactions retrieve all incomplete transactions from the transaction table within a given timeframe and recover those transactions in sequence.
// recoverTime is used to ensure the newly added transactions are not picked up by the recovery process.
func (s *Service) RecoverTransactions(ctx context.Context, recoverTime time.Time) error {
	// Recovering transactions in Cancelling state
	cts, err := s.Ts.GetAllTransactionsInState(ctx, Canceling)
	if err != nil {
		return err
	}
	if err := s.recoverTransactions(ctx, cts, recoverTime, Canceling); err != nil {
		return err
	}

	// Recovering transactions in Applied state
	ats, err := s.Ts.GetAllTransactionsInState(ctx, Applied)
	if err != nil {
		return err
	}
	if err := s.recoverTransactions(ctx, ats, recoverTime, Applied); err != nil {
		return err
	}

	// Recovering transactions in Pending state
	pts, err := s.Ts.GetAllTransactionsInState(ctx, Pending)
	if err != nil {
		return err
	}
	return s.recoverTransactions(ctx, pts, recoverTime, Pending)
}

func (s *Service) recoverTransactions(ctx context.Context, ts []*Transaction, recoverTime time.Time, state TransactionState) error {
	if len(ts) > 0 {
		for _, t := range ts {
			if recoverTime.After(t.LastModified) {
				req := Request{
					Source:      t.Source,
					Destination: t.Destination,
					Data:        t.Value,
				}
				if err := s.recoverFromError(ctx, t.ID, req, state); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Service) applyTransaction(ctx context.Context, req Request, transactionID string, callbacks ...func() error) error {
	// Attempt to update the source account
	if err := s.Ah.Update(ctx, req.Source, transactionID, req); err != nil {
		// Failed to update source account, cancel transaction.
		return err
	}

	// Attempt to update the destination account
	if err := s.Ah.Update(ctx, req.Destination, transactionID, req); err != nil {
		// Failed to update destination account, cancel transaction
		return err
	}

	if len(callbacks) > 0 {
		for _, f := range callbacks {
			if err := f(); err != nil {
				return err
			}
		}
	}

	// Upon success of both updates, change transaction state to applied
	if _, err := s.Ts.UpdateState(ctx, transactionID, Applied); err != nil {
		// Failed to update state to Applied, cancel transaction
		return err
	}

	return nil
}

func (s *Service) commitTransaction(ctx context.Context, req Request, transactionID string) (*Transaction, error) {
	// Commit transactions by updating the pending transaction list of both accounts
	if err := s.Ah.Commit(ctx, req.Source, transactionID); err != nil {
		// Failed to commit transaction, retry commit transaction
		return nil, err
	}

	if err := s.Ah.Commit(ctx, req.Destination, transactionID); err != nil {
		// Failed to commit transaction, retry commit transaction
		return nil, err
	}

	// Upon success of both commits, change transaction state to done
	tr, err := s.Ts.UpdateState(ctx, transactionID, Done)
	if err != nil {
		// Failed to commit transaction, retry commit transaction
		return nil, err
	}

	return tr, nil
}

func (s *Service) cancelTransaction(ctx context.Context, req Request, transactionID string) error {
	// Attempt to rollback the destination account
	if err := s.Ah.Rollback(ctx, req.Destination, transactionID, req); err != nil {
		if !s.Ah.IsErrorPendingTransactionIDNotFound(err) {
			return err
		}
	}

	// Attempt to rollback the source account
	if err := s.Ah.Rollback(ctx, req.Source, transactionID, req); err != nil {
		if !s.Ah.IsErrorPendingTransactionIDNotFound(err) {
			return err
		}
	}
	// Upon success of both updates, change transaction state to cancelled
	if _, err := s.Ts.UpdateState(ctx, transactionID, Cancelled); err != nil {
		// Failed to update state to Cancelled, retry cancel transaction
		return err
	}

	return nil
}

func (s *Service) recoverFromError(ctx context.Context, transactionID string, req Request, state TransactionState) error {
	switch state {
	case Pending:
		return s.recoverFromPendingState(ctx, transactionID, req)
	case Applied:
		return s.recoverFromAppliedState(ctx, transactionID, req)
	case Canceling:
		return s.recoverFromCancellingState(ctx, transactionID, req)
	default:
		return nil
	}
}

func (s *Service) recoverFromPendingState(ctx context.Context, transactionID string, req Request) error {
	// Update transaction state to canceling
	if _, err := s.Ts.UpdateState(ctx, transactionID, Canceling); err != nil {
		return err
	}
	// Actually canceling the transaction
	return s.cancelTransaction(ctx, req, transactionID)
}

func (s *Service) recoverFromAppliedState(ctx context.Context, transactionID string, req Request) error {
	if _, err := s.commitTransaction(ctx, req, transactionID); err != nil {
		return err
	}
	return nil
}

func (s *Service) recoverFromCancellingState(ctx context.Context, transactionID string, req Request) error {
	return s.cancelTransaction(ctx, req, transactionID)
}
