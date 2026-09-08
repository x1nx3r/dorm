package orm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/dionisius77/dorm/errkind"
	dormerrors "github.com/dionisius77/dorm/errors"
)

type transactionState struct {
	savepoint string
	closed    bool
}

var savepointSeq uint64

func nextSavepointName() string {
	return fmt.Sprintf("sp_%d", atomic.AddUint64(&savepointSeq, 1))
}

func (db *DB) currentContext() context.Context {
	if db == nil || db.ctx == nil {
		return context.Background()
	}
	return db.ctx
}

func (db *DB) session() *Session {
	return &Session{db: db, ctx: db.currentContext()}
}

func (db *DB) sessionWithContext(ctx context.Context) *Session {
	if db == nil {
		return &Session{}
	}
	if ctx == nil {
		ctx = db.currentContext()
	}
	return &Session{db: db, ctx: ctx}
}

func (db *DB) Find(dest any, opts ...QueryOption) error {
	if db == nil {
		return errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	return db.session().Find(dest, opts...)
}

func (db *DB) FindOne(dest any, opts ...QueryOption) error {
	if db == nil {
		return errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	return db.session().FindOne(dest, opts...)
}

func (db *DB) Count(model any, opts ...QueryOption) (int64, error) {
	if db == nil {
		return 0, errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	return db.session().Count(model, opts...)
}

func (db *DB) Exists(model any, opts ...QueryOption) (bool, error) {
	if db == nil {
		return false, errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	return db.session().Exists(model, opts...)
}

func (db *DB) Create(model any) error {
	if db == nil {
		return errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	return db.session().Create(model)
}

func (db *DB) CreateMany(ctx context.Context, models any) error {
	if db == nil {
		return errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	return db.sessionWithContext(ctx).CreateMany(models)
}

func (db *DB) Update(model any) error {
	if db == nil {
		return errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	return db.session().Update(model)
}

func (db *DB) UpdateMany(ctx context.Context, models any) error {
	if db == nil {
		return errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	return db.sessionWithContext(ctx).UpdateMany(models)
}

func (db *DB) UpdateWhere(model any, opts ...QueryOption) error {
	if db == nil {
		return errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	return db.session().UpdateWhere(model, opts...)
}

func (db *DB) Delete(model any) error {
	if db == nil {
		return errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	return db.session().Delete(model)
}

func (db *DB) DeleteMany(ctx context.Context, models any) error {
	if db == nil {
		return errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	return db.sessionWithContext(ctx).DeleteMany(models)
}

func (db *DB) SoftDelete(model any) error {
	if db == nil {
		return errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	return db.session().SoftDelete(model)
}

func (db *DB) Upsert(model any) error {
	if db == nil {
		return errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	return db.session().Upsert(model)
}

func (s *Session) Tx(fn func(*Session) error) error {
	if s == nil || s.db == nil {
		return dormerrors.NewTransactionError(dormerrors.KindTransactionClosed, "callback", "", nil)
	}
	return s.db.Tx(s.ctx, fn)
}

func (db *DB) Transaction(ctx context.Context, fn func(*DB) error) error {
	if db == nil {
		return dormerrors.NewTransactionError(dormerrors.KindConfiguration, "begin", "", fmt.Errorf("nil db"))
	}
	if ctx == nil {
		ctx = db.currentContext()
	}
	return db.traceOperation(ctx, "db.transaction", []Attribute{{Key: "orm.operation", Value: "transaction"}}, func(ctx context.Context) error {
		tx, err := db.Begin(ctx)
		if err != nil {
			return err
		}
		if fn == nil {
			rbErr := tx.Rollback()
			if rbErr != nil {
				return rbErr
			}
			return dormerrors.NewTransactionError(dormerrors.KindConfiguration, "callback", "", fmt.Errorf("nil callback"))
		}
		if err := fn(tx); err != nil {
			rbErr := tx.Rollback()
			if rbErr != nil {
				return errors.Join(err, rbErr)
			}
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	})
}

func (db *DB) Begin(ctx context.Context) (*DB, error) {
	if db == nil {
		return nil, dormerrors.NewTransactionError(dormerrors.KindConfiguration, "begin", "", fmt.Errorf("nil db"))
	}
	if ctx == nil {
		ctx = db.currentContext()
	}
	var txDB *DB
	err := db.traceOperation(ctx, "db.begin", []Attribute{{Key: "orm.operation", Value: "begin"}}, func(ctx context.Context) error {
		db.txMu.Lock()
		defer db.txMu.Unlock()
		if db.txState != nil && db.txState.closed {
			return dormerrors.NewTransactionError(dormerrors.KindTransactionClosed, "begin", db.txState.savepoint, nil)
		}
		if db.tx != nil {
			savepoint := nextSavepointName()
			if _, err := db.tx.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
				return dormerrors.NewTransactionError(dormerrors.KindRuntimeQuery, "begin", savepoint, err)
			}
			txDB = db.cloneForTransaction(ctx)
			txDB.txState = &transactionState{savepoint: savepoint}
			return nil
		}
		if db.db == nil {
			return dormerrors.NewTransactionError(dormerrors.KindConfiguration, "begin", "", fmt.Errorf("nil db"))
		}
		sqlTx, err := db.db.BeginTx(ctx, nil)
		if err != nil {
			return dormerrors.NewTransactionError(dormerrors.KindRuntimeQuery, "begin", "", err)
		}
		txDB = db.cloneForTransaction(ctx)
		txDB.db = nil
		txDB.tx = sqlTx
		txDB.txState = &transactionState{}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return txDB, nil
}

func (db *DB) Commit() error {
	if db == nil {
		return dormerrors.NewTransactionError(dormerrors.KindTransactionClosed, "commit", "", nil)
	}
	return db.traceOperation(db.currentContext(), "db.commit", []Attribute{{Key: "orm.operation", Value: "commit"}}, func(ctx context.Context) error {
		db.txMu.Lock()
		defer db.txMu.Unlock()
		if err := db.ensureTransactionOpen("commit"); err != nil {
			return err
		}
		defer db.markTransactionClosed()
		if db.txState != nil && db.txState.savepoint != "" {
			if _, err := db.tx.ExecContext(ctx, "RELEASE SAVEPOINT "+db.txState.savepoint); err != nil {
				return dormerrors.NewTransactionError(dormerrors.KindCommitFailed, "commit", db.txState.savepoint, err)
			}
			return nil
		}
		if db.tx == nil {
			return dormerrors.NewTransactionError(dormerrors.KindTransactionClosed, "commit", "", nil)
		}
		if err := db.tx.Commit(); err != nil {
			return dormerrors.NewTransactionError(dormerrors.KindCommitFailed, "commit", "", err)
		}
		return nil
	})
}

func (db *DB) Rollback() error {
	if db == nil {
		return dormerrors.NewTransactionError(dormerrors.KindTransactionClosed, "rollback", "", nil)
	}
	return db.traceOperation(db.currentContext(), "db.rollback", []Attribute{{Key: "orm.operation", Value: "rollback"}}, func(ctx context.Context) error {
		db.txMu.Lock()
		defer db.txMu.Unlock()
		if err := db.ensureTransactionOpen("rollback"); err != nil {
			return err
		}
		defer db.markTransactionClosed()
		if db.txState != nil && db.txState.savepoint != "" {
			if _, err := db.tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+db.txState.savepoint); err != nil {
				return dormerrors.NewTransactionError(dormerrors.KindRollbackFailed, "rollback", db.txState.savepoint, err)
			}
			return nil
		}
		if db.tx == nil {
			return dormerrors.NewTransactionError(dormerrors.KindTransactionClosed, "rollback", "", nil)
		}
		if err := db.tx.Rollback(); err != nil {
			return dormerrors.NewTransactionError(dormerrors.KindRollbackFailed, "rollback", "", err)
		}
		return nil
	})
}

func (db *DB) Tx(ctx context.Context, fn func(*Session) error) error {
	return db.Transaction(ctx, func(tx *DB) error {
		if fn == nil {
			return nil
		}
		return fn(tx.session())
	})
}

func (db *DB) cloneForTransaction(ctx context.Context) *DB {
	return &DB{
		db:                  db.db,
		tx:                  db.tx,
		ctx:                 ctx,
		dialect:             db.dialect,
		driverName:          db.driverName,
		schema:              db.schema,
		logger:              db.logger,
		observability:       db.observability,
		access:              db.access,
		queryAdvisor:        db.queryAdvisor,
		prepareStatements:   db.prepareStatements,
		softDeleteByDefault: db.softDeleteByDefault,
		batchSize:           db.batchSize,
		executionMode:       db.executionMode,
		dryRun:              db.dryRun,
		stmts:               map[string]*sql.Stmt{},
	}
}

func (db *DB) ensureTransactionOpen(operation string) error {
	if db.txState != nil && db.txState.closed {
		return dormerrors.NewTransactionError(dormerrors.KindTransactionClosed, operation, db.txState.savepoint, nil)
	}
	if db.tx == nil && db.db == nil {
		return dormerrors.NewTransactionError(dormerrors.KindTransactionClosed, operation, "", nil)
	}
	return nil
}

func (db *DB) markTransactionClosed() {
	if db == nil {
		return
	}
	if db.txState != nil {
		db.txState.closed = true
	}
}

func (db *DB) transactionClosedError(operation string) error {
	if db == nil {
		return dormerrors.NewTransactionError(dormerrors.KindConfiguration, operation, "", nil)
	}
	if db.txState != nil && db.txState.closed {
		return dormerrors.NewTransactionError(dormerrors.KindTransactionClosed, operation, db.txState.savepoint, nil)
	}
	return nil
}
