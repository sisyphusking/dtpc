# Overview
dtpc is short for DynamoDB Two Phase Commits.
Package dtpc attempts to provide an alternative solution of handling multi-document transactions with DynamoDB.

# Structure
## Components
- Service: Contains main service methods such as StartTransaction() and RecoverTransactions().
- Account: Defines all dependencies of Account with a default AccountHandler implementation.
- TransactionStore: Defines all dependencies of TransactionHandler with a default TransactionHandler implementation.

## Basics
### Initialisation
```go
// Initialise a dynamodb client via the aws-sdk-go.
// Information on how to initialse a dynamodb client please refer to the offcial documentation of Dynamodb Go SDK: https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/
dynamodbCli := dynamodb.New(session.New())

// Initialise Transaction Store
ts := dtpc.NewTransactionStore(dynamodbCli)

// Initialise Account Handler. The example uses the sample account handler implementation.
ah := example.NewHandlerImpl(dynamodbCli, "your_account_table_name", "your_account_hash_key_name")

// Finally, initialise the Transaction Service
srv := dtpc.NewService(ts, ah)
```

### Start a Transaction
```go
// This request transfers 10 units of currency_id from source_account_id to destination_account_id.
req := dtpc.Request {
    Source: "source_account_id",
	Destination "destination_account_id",
	Data        Item {
        ID: "currency_id",
        Amount: 10,
    }
}

// Add your context information if necessary.
ctx := context.Background()

// Perform your transaction request.
err := srv.StartTransaction(ctx, req)
if err != nil {
    // Handle error
}
```

### Recover Transcations
In reality, your systems may experience extreme situations such as Network outage or Database outage. These situations can lead to inconsistent state of the records in your database. The two-phase commit pattern allows applications running the sequence to resume the transaction and arrive at a consistent state.
```go
// Set recoverTime to ensure the newly added transactions are not picked up by the recovery process.
// The following example recovers all transactions created/modified before a minute ago.
rt := time.Now().Add(-60000 * time.Millisecond)

// Start Recovery process
err := srv.RecoverTransactions(ctx context.Context, rt)
if err != nil {
    // Handle error
}
```

## Advance
### Implement custom Account Handler
For specific use cases in your application, you can implement a custom account handler to allow the transaction services working with your application. To implement a custom Account Handler, simply follow the sample implementation provided in the testsuite/example folder to implement the AccountHandler interface. You will need to define the behaviours of Get, Put, Update, Rollback and Commit, then pass your handler implementation instance when the dtpc service is being initialsed.

For example:
```go
// Initialise a dynamodb client via the aws-sdk-go.
dynamodbCli := dynamodb.New(session.New())

// Initialise Transaction Store
ts := dtpc.NewTransactionStore(dynamodbCli)

// Initialise Account Handler. The example uses the sample account handler implementation.
ah := InitialiseYourAccountHandler(...)

// Finally, initialise the Transaction Service
srv := dtpc.NewService(ts, ah)
```


## 补充
- 修改gopath为goModule
- 这里的startTransaction里每一步都有回滚操作，而且加入了重试机制，写法不错，值得学习

这里仅仅是保证单个事务实现2pc（比如a转账给b），如果一个业务操作包含多个2pc操作，不满足，而且这里的逻辑没有分布式的，都是单节点，且没有超时控制，请求hang住的时候，只要等到超时才cancel 
