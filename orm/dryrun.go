package orm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dionisius77/dorm/errkind"
)

type executionMode string

const (
	executionModeNormal executionMode = ""
	executionModeDryRun executionMode = "dryrun"
)

const dryRunDriverName = "orm-dryrun"

var dryRunDriverOnce sync.Once

type dryRunState struct {
	mu       sync.Mutex
	recorder *dryRunRecorder
}

type dryRunDriver struct{}

type dryRunConn struct {
	id string
}

type dryRunRows struct {
	cols []string
	data [][]driver.Value
	idx  int
}

type dryRunResult struct{}

type dryRunTx struct{}

type dryRunRecorder struct {
	mu         sync.Mutex
	report     ExecutionReport
	hookOrder  int
	statements []ExecutionStatement
}

var (
	dryRunStateMu sync.Mutex
	dryRunStates  = map[string]*dryRunState{}
	dryRunSeq     uint64
)

func init() {
	dryRunDriverOnce.Do(func() {
		sql.Register(dryRunDriverName, dryRunDriver{})
	})
}

func (db *DB) cloneForDryRun() (*DB, error) {
	if db == nil {
		return nil, errkind.New(errkind.KindConfiguration, "orm: nil db")
	}
	recorder := newDryRunRecorder()
	sqlDB, id, err := openDryRunSQLDB(recorder)
	if err != nil {
		return nil, err
	}
	cp := &DB{
		db:                  sqlDB,
		tx:                  nil,
		ctx:                 db.ctx,
		dialect:             db.dialect,
		driverName:          db.driverName,
		schema:              db.schema,
		logger:              db.logger,
		observability:       db.observability,
		access:              db.access,
		queryAdvisor:        db.queryAdvisor,
		prepareStatements:   false,
		softDeleteByDefault: db.softDeleteByDefault,
		batchSize:           db.batchSize,
		executionMode:       executionModeDryRun,
		dryRun:              recorder,
		stmts:               map[string]*sql.Stmt{},
	}
	recorder.setMetadata("dryrun_id", id)
	return cp, nil
}

func openDryRunSQLDB(recorder *dryRunRecorder) (*sql.DB, string, error) {
	if recorder == nil {
		return nil, "", errkind.New(errkind.KindConfiguration, "orm: nil dry-run recorder")
	}
	id := fmt.Sprintf("dryrun_%d", atomic.AddUint64(&dryRunSeq, 1))
	dryRunStateMu.Lock()
	dryRunStates[id] = &dryRunState{recorder: recorder}
	dryRunStateMu.Unlock()
	db, err := sql.Open(dryRunDriverName, id)
	if err != nil {
		dryRunStateMu.Lock()
		delete(dryRunStates, id)
		dryRunStateMu.Unlock()
		return nil, "", errkind.Wrap(errkind.KindConfiguration, "orm: open dry-run sql db", err)
	}
	return db, id, nil
}

func newDryRunRecorder() *dryRunRecorder {
	return &dryRunRecorder{
		report: ExecutionReport{
			Version:         1,
			ExecutionStatus: ExecutionStatusSkipped,
			Metadata:        map[string]any{},
		},
	}
}

func (r *dryRunRecorder) setMetadata(key string, value any) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.report.Metadata == nil {
		r.report.Metadata = map[string]any{}
	}
	r.report.Metadata[key] = value
}

func (r *dryRunRecorder) setOperation(operation string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.report.Operation = operation
}

func (r *dryRunRecorder) setTable(table string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.report.Table = table
}

func (r *dryRunRecorder) recordStatement(operation, sqlText string, args ...any) {
	if r == nil {
		return
	}
	cpArgs := append([]any(nil), args...)
	r.mu.Lock()
	defer r.mu.Unlock()
	statement := ExecutionStatement{
		Operation:  operation,
		SQL:        sqlText,
		Parameters: cpArgs,
	}
	r.statements = append(r.statements, statement)
	r.report.Statements = append(r.report.Statements, statement)
	r.report.SQL = sqlText
	r.report.Parameters = cpArgs
}

func (r *dryRunRecorder) recordAccessPolicy(event AccessPolicyEvent) {
	if r == nil {
		return
	}
	cpArgs := append([]any(nil), event.Arguments...)
	event.Arguments = cpArgs
	r.mu.Lock()
	defer r.mu.Unlock()
	r.report.AccessPolicies = append(r.report.AccessPolicies, event)
}

func (r *dryRunRecorder) recordAuditAction(action AuditAction) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.report.AuditActions = append(r.report.AuditActions, action)
}

func (r *dryRunRecorder) recordHook(name, model string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hookOrder++
	r.report.LifecycleHooks = append(r.report.LifecycleHooks, LifecycleHookEvent{
		Order: r.hookOrder,
		Name:  name,
		Model: model,
	})
}

func (r *dryRunRecorder) recordAdvisor(findings []QueryAdvisorFinding) {
	if r == nil || len(findings) == 0 {
		return
	}
	cp := append([]QueryAdvisorFinding(nil), findings...)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.report.QueryAdvisor = append(r.report.QueryAdvisor, cp...)
}

func (r *dryRunRecorder) recordOptimisticLock(info *OptimisticLockingInfo) {
	if r == nil || info == nil {
		return
	}
	cp := *info
	r.mu.Lock()
	defer r.mu.Unlock()
	r.report.OptimisticLocking = &cp
}

func (r *dryRunRecorder) finalize() ExecutionReport {
	if r == nil {
		return ExecutionReport{ExecutionStatus: ExecutionStatusSkipped}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	report := r.report
	report.ExecutionStatus = ExecutionStatusSkipped
	report.Statements = append([]ExecutionStatement(nil), r.report.Statements...)
	report.AccessPolicies = append([]AccessPolicyEvent(nil), r.report.AccessPolicies...)
	report.AuditActions = append([]AuditAction(nil), r.report.AuditActions...)
	report.LifecycleHooks = append([]LifecycleHookEvent(nil), r.report.LifecycleHooks...)
	report.QueryAdvisor = append([]QueryAdvisorFinding(nil), r.report.QueryAdvisor...)
	if r.report.OptimisticLocking != nil {
		info := *r.report.OptimisticLocking
		report.OptimisticLocking = &info
	}
	if report.Metadata == nil {
		report.Metadata = map[string]any{}
	}
	report.Metadata["statement_count"] = len(report.Statements)
	return report
}

func (d dryRunDriver) Open(name string) (driver.Conn, error) {
	return &dryRunConn{id: name}, nil
}

func (c *dryRunConn) state() (*dryRunState, error) {
	dryRunStateMu.Lock()
	defer dryRunStateMu.Unlock()
	state, ok := dryRunStates[c.id]
	if !ok || state == nil {
		return nil, errkind.New(errkind.KindConfiguration, "orm: dry-run state not found")
	}
	return state, nil
}

func (c *dryRunConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare not supported")
}

func (c *dryRunConn) Close() error { return nil }

func (c *dryRunConn) Begin() (driver.Tx, error) {
	return &dryRunTx{}, nil
}

func (c *dryRunConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &dryRunTx{}, nil
}

func (c *dryRunConn) Ping(context.Context) error { return nil }

func (c *dryRunConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	state, err := c.state()
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	recorder := state.recorder
	state.mu.Unlock()
	if recorder != nil {
		recorder.recordStatement("exec", query, dryRunNamedValuesToAny(args)...)
	}
	return dryRunResult{}, nil
}

func (c *dryRunConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	state, err := c.state()
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	recorder := state.recorder
	state.mu.Unlock()
	if recorder != nil {
		recorder.recordStatement("query", query, dryRunNamedValuesToAny(args)...)
	}
	if strings.Contains(strings.ToLower(strings.TrimSpace(query)), "returning") {
		return &dryRunRows{
			cols: []string{"value"},
			data: [][]driver.Value{{nil}},
		}, nil
	}
	return &dryRunRows{cols: []string{"value"}}, nil
}

func (c *dryRunConn) CheckNamedValue(*driver.NamedValue) error { return nil }

func (r *dryRunRows) Columns() []string { return append([]string(nil), r.cols...) }

func (r *dryRunRows) Close() error { return nil }

func (r *dryRunRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.data) {
		return io.EOF
	}
	for i := range dest {
		if i < len(r.data[r.idx]) {
			dest[i] = r.data[r.idx][i]
		} else {
			dest[i] = nil
		}
	}
	r.idx++
	return nil
}

func (r dryRunResult) LastInsertId() (int64, error) { return 0, nil }

func (r dryRunResult) RowsAffected() (int64, error) { return 0, nil }

func (t *dryRunTx) Commit() error   { return nil }
func (t *dryRunTx) Rollback() error { return nil }

func dryRunNamedValuesToAny(args []driver.NamedValue) []any {
	if len(args) == 0 {
		return nil
	}
	out := make([]any, len(args))
	for i, arg := range args {
		out[i] = arg.Value
	}
	return out
}
