package logger

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
)

type PostgresTransactionLogger struct {
	events chan<- Event //Write-only channel
	errors <-chan error //Read-only channel
	db     *sql.DB
}

type PostgresParams struct {
	host     string
	dbName   string
	user     string
	password string
}

func (p *PostgresTransactionLogger) WritePut(key, value string) {
	p.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (p *PostgresTransactionLogger) WriteDelete(key string) {
	p.events <- Event{EventType: EventDelete, Key: key}
}

func (p *PostgresTransactionLogger) Err() <-chan error {
	return p.errors
}

func (p *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	events := make(chan Event)
	errors := make(chan error)

	return events,errors
}

func (p *PostgresTransactionLogger) Run() {

}

func (p *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	//TODO TABLE VERIFICATION
	return true, nil
}

func (p *PostgresTransactionLogger) createTable() error {
	//TODO CREATE TABLE
	return nil
}

func NewPostgresTransactionLogger(p PostgresParams) (TransactionLogger, error) {
	connStr := fmt.Sprintf("host=%s dbname=%s user=%s password=%s", p.host, p.dbName, p.user, p.password)

	db, err := sql.Open("postgres", connStr)

	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	defer db.Close()

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("error connecting to db: %w", err)
	}

	logger := &PostgresTransactionLogger{db: db}
	exists, err := logger.verifyTableExists()

	if err != nil {
		return nil, fmt.Errorf("failed to verify if table exists: %w", err)
	}

	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("error creating table: %w", err)
		}
	}

	return logger, nil
}
