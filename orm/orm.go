package orm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/dionisius77/dorm/access"
	"github.com/dionisius77/dorm/dialect"
	"github.com/dionisius77/dorm/errkind"
	dormerrors "github.com/dionisius77/dorm/errors"
	"github.com/dionisius77/dorm/schema"
)

var ErrNotFound = dormerrors.ErrNotFound

const defaultBatchSize = 100

var (
	tableMetaCache sync.Map // map[reflect.Type]*schema.Table
)

type Logger interface {
	Printf(string, ...any)
}

type Config struct {
	DB                  *sql.DB
	Tx                  *sql.Tx
	Dialect             dialect.Dialect
	DriverName          string
	Schema              *schema.Schema
	Logger              Logger
	Observability       ObservabilityConfig
	Access              access.Engine
	PrepareStatements   bool
	SoftDeleteByDefault bool
	BatchSize           int
}

type DB struct {
	db                  *sql.DB
	tx                  *sql.Tx
	ctx                 context.Context
	dialect             dialect.Dialect
	driverName          string
	schema              *schema.Schema
	logger              Logger
	observability       ObservabilityConfig
	access              access.Engine
	prepareStatements   bool
	softDeleteByDefault bool
	batchSize           int
	stmtMu              sync.Mutex
	txMu                sync.Mutex
	stmts               map[string]*sql.Stmt
	txState             *transactionState
}

type Session struct {
	db          *DB
	ctx         context.Context
	withDeleted bool
}

func (s *Session) resolveModel(model any) (*tableMeta, reflect.Value, error) {
	return s.db.resolveModel(model)
}

func New(cfg Config) *DB {
	obs := cfg.Observability.Normalized()
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	return &DB{
		db:                  cfg.DB,
		tx:                  cfg.Tx,
		ctx:                 context.Background(),
		dialect:             cfg.Dialect,
		driverName:          cfg.DriverName,
		schema:              cfg.Schema,
		logger:              cfg.Logger,
		observability:       obs,
		access:              cfg.Access,
		prepareStatements:   cfg.PrepareStatements,
		softDeleteByDefault: cfg.SoftDeleteByDefault,
		batchSize:           batchSize,
		stmts:               map[string]*sql.Stmt{},
	}
}

func (db *DB) WithContext(ctx context.Context) *Session {
	return &Session{db: db, ctx: ctx}
}

func (db *DB) WithPolicy(policy access.Policy) *Session {
	return (&Session{db: db}).WithPolicy(policy)
}

func (s *Session) WithDeleted() *Session {
	if s == nil {
		return &Session{withDeleted: true}
	}
	cp := *s
	cp.withDeleted = true
	return &cp
}

func (s *Session) WithContext(ctx context.Context) *Session {
	if s == nil {
		return &Session{ctx: ctx}
	}
	cp := *s
	cp.ctx = access.WithPolicy(ctx, access.PolicyFromContext(s.ctx))
	return &cp
}

func (s *Session) WithPolicy(policy access.Policy) *Session {
	if s == nil {
		return &Session{ctx: access.WithPolicy(context.Background(), policy)}
	}
	cp := *s
	cp.ctx = access.WithPolicy(cp.ctx, policy)
	return &cp
}

func (s *Session) Find(dest any, opts ...QueryOption) error {
	return s.db.traceOperation(s.ctx, "db.query", []Attribute{{Key: "orm.operation", Value: "find"}}, func(ctx context.Context) error {
		state, err := s.buildState(dest, opts...)
		if err != nil {
			return err
		}
		sqlText, args, err := s.db.buildSelectSQL(ctx, state)
		if err != nil {
			return err
		}
		rows, err := s.db.queryContext(ctx, sqlText, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		if err := scanIntoSlice(rows, dest, state.table); err != nil {
			return err
		}
		return invokeAfterFindHooks(ctx, s.db, reflect.ValueOf(dest).Elem())
	})
}

func (s *Session) FindOne(dest any, opts ...QueryOption) error {
	opts = append(opts, Limit(1))
	if err := s.Find(dest, opts...); err != nil {
		return err
	}
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.Elem().Kind() != reflect.Slice || rv.Elem().Len() == 0 {
		return ErrNotFound
	}
	return nil
}

// Count returns the number of rows matching the model type and query options.
func (s *Session) Count(model any, opts ...QueryOption) (int64, error) {
	var count int64
	err := s.db.traceOperation(s.ctx, "db.query", []Attribute{{Key: "orm.operation", Value: "count"}}, func(ctx context.Context) error {
		s.db.logPolicyOverride(ctx)
		meta, _, err := s.db.resolveModel(model)
		if err != nil {
			return err
		}
		state := &queryState{table: meta.table}
		if s.withDeleted {
			state.withDeleted = true
		}
		for _, opt := range opts {
			opt(state)
		}
		if !state.withDeleted && access.PolicyFromContext(s.ctx).EnforcesSoftDelete() && softDeleteColumn(meta.table) != nil {
			state.where = append(state.where, predicate{expr: softDeleteColumn(meta.table).Name + " IS NULL"})
		}
		preds, _, err := s.db.access.Apply(s.ctx, meta.table, access.OpQuery, nil)
		if err != nil {
			return err
		}
		for _, pred := range preds {
			state.where = append(state.where, predicate{expr: pred.SQL, args: pred.Args})
		}
		var clauses []string
		var args []any
		for _, p := range state.where {
			clause, clauseArgs := bindClause(p.expr, p.args, len(args)+1, s.db.dialect)
			clauses = append(clauses, clause)
			args = append(args, clauseArgs...)
		}
		sqlText, err := s.db.dialect.RenderSelect(meta.table.Name, []string{"COUNT(*)"}, clauses, nil, nil, nil)
		if err != nil {
			return errkind.Wrap(errkind.KindUnsupportedFeature, "orm: render count", err)
		}
		rows, err := s.db.queryContext(ctx, sqlText, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		if !rows.Next() {
			return nil
		}
		if err := rows.Scan(&count); err != nil {
			return errkind.Wrap(errkind.KindRuntimeQuery, "orm: scan count", err)
		}
		return rows.Err()
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Session) Create(model any) error {
	return s.db.traceOperation(s.ctx, "db.insert", []Attribute{{Key: "orm.operation", Value: "create"}}, func(ctx context.Context) error {
		s.db.logPolicyOverride(ctx)
		return s.create(ctx, model)
	})
}

func (s *Session) create(ctx context.Context, model any) error {
	if err := invokeBeforeCreateHook(ctx, s.db, model); err != nil {
		return err
	}
	meta, rv, err := s.resolveModel(model)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	table := meta.table
	values := map[string]any{}
	if err := populateAuditAndScope(ctx, table, rv, values, now, access.OpInsert); err != nil {
		return err
	}
	if err := applyStructFieldValues(ctx, table, rv, values, now, true); err != nil {
		return err
	}
	cols, args := orderedColumnsAndArgs(table, values, true)
	if len(cols) == 0 {
		return errkind.New(errkind.KindInvalidSchema, fmt.Sprintf("orm: no insertable columns for %s", table.Name))
	}
	sqlText, err := s.db.dialect.RenderInsert(table.Name, cols, returningColumns(table))
	if err != nil {
		return errkind.Wrap(errkind.KindUnsupportedFeature, "orm: render insert", err)
	}
	if err := s.db.execReturning(ctx, sqlText, args, model, table); err != nil {
		return err
	}
	return invokeAfterCreateHook(ctx, s.db, model)
}

func (s *Session) Update(model any) error {
	return s.db.traceOperation(s.ctx, "db.update", []Attribute{{Key: "orm.operation", Value: "update"}}, func(ctx context.Context) error {
		s.db.logPolicyOverride(ctx)
		return s.update(ctx, model, nil, true)
	})
}

// UpdateWhere updates rows matching explicit query filters.
func (s *Session) UpdateWhere(model any, opts ...QueryOption) error {
	return s.db.traceOperation(s.ctx, "db.update", []Attribute{{Key: "orm.operation", Value: "update"}}, func(ctx context.Context) error {
		s.db.logPolicyOverride(ctx)
		return s.update(ctx, model, opts, false)
	})
}

func (s *Session) update(ctx context.Context, model any, opts []QueryOption, usePrimaryKey bool) error {
	if err := invokeBeforeUpdateHook(ctx, s.db, model); err != nil {
		return err
	}
	meta, rv, err := s.resolveModel(model)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	table := meta.table
	values := map[string]any{}
	if err := populateAuditAndScope(ctx, table, rv, values, now, access.OpUpdate); err != nil {
		return err
	}
	if err := applyStructFieldValues(ctx, table, rv, values, now, false); err != nil {
		return err
	}
	setPkCols := primaryKeyColumnNames(table)
	set, args := updateSetClauses(table, values, setPkCols, s.db.dialect)
	if len(set) == 0 {
		return errkind.New(errkind.KindInvalidSchema, fmt.Sprintf("orm: no updatable columns for %s", table.Name))
	}

	var whereCols []string
	var whereArgs []any
	var extraPreds []predicate
	if usePrimaryKey {
		whereCols, whereArgs, err = primaryKeyValues(table, rv)
		if err != nil {
			return err
		}
		if len(whereCols) == 0 {
			return errkind.New(errkind.KindInvalidSchema, "orm: update requires primary key")
		}
		extraPreds = append(extraPreds, accessPredicates(ctx, table)...)
	} else {
		state := &queryState{}
		for _, opt := range opts {
			opt(state)
		}
		if len(state.where) == 0 {
			return errkind.New(errkind.KindInvalidSchema, "orm: updatewhere requires where clauses")
		}
		extraPreds = append(extraPreds, state.where...)
		extraPreds = append(extraPreds, accessPredicates(ctx, table)...)
	}

	whereSQL, whereArgs := buildWhereClauses(whereCols, whereArgs, len(args), extraPreds, s.db.dialect)
	sqlText, err := s.db.dialect.RenderUpdate(table.Name, set, whereSQL, returningColumns(table))
	if err != nil {
		return errkind.Wrap(errkind.KindUnsupportedFeature, "orm: render update", err)
	}
	if err := s.db.execReturning(ctx, sqlText, append(args, whereArgs...), model, table); err != nil {
		return err
	}
	return invokeAfterUpdateHook(ctx, s.db, model)
}

func (s *Session) Delete(model any) error {
	return s.db.traceOperation(s.ctx, "db.delete", []Attribute{{Key: "orm.operation", Value: "delete"}}, func(ctx context.Context) error {
		s.db.logPolicyOverride(ctx)
		return s.delete(ctx, model)
	})
}

func (s *Session) SoftDelete(model any) error {
	return s.db.traceOperation(s.ctx, "db.delete", []Attribute{{Key: "orm.operation", Value: "soft_delete"}}, func(ctx context.Context) error {
		s.db.logPolicyOverride(ctx)
		if err := invokeBeforeDeleteHook(ctx, s.db, model); err != nil {
			return err
		}
		meta, rv, err := s.resolveModel(model)
		if err != nil {
			return err
		}
		table := meta.table
		soft := softDeleteColumn(table)
		if soft == nil {
			return errkind.New(errkind.KindInvalidSchema, fmt.Sprintf("orm: table %s has no soft delete column", table.Name))
		}
		if err := s.softDelete(ctx, model, table, rv, soft); err != nil {
			return err
		}
		return invokeAfterDeleteHook(ctx, s.db, model)
	})
}

func (s *Session) Upsert(model any) error {
	return s.db.traceOperation(s.ctx, "db.upsert", []Attribute{{Key: "orm.operation", Value: "upsert"}}, func(ctx context.Context) error {
		s.db.logPolicyOverride(ctx)
		meta, rv, err := s.resolveModel(model)
		if err != nil {
			return err
		}
		table := meta.table
		values := map[string]any{}
		if err := applyStructFieldValues(ctx, table, rv, values, time.Now().UTC(), true); err != nil {
			return err
		}
		if err := populateAuditAndScope(ctx, table, rv, values, time.Now().UTC(), access.OpInsert); err != nil {
			return err
		}
		cols, args := orderedColumnsAndArgs(table, values, true)
		if len(cols) == 0 {
			return errkind.New(errkind.KindInvalidSchema, fmt.Sprintf("orm: no upsertable columns for %s", table.Name))
		}
		conflict := conflictColumns(table)
		if len(conflict) == 0 {
			return errkind.New(errkind.KindInvalidSchema, "orm: upsert requires primary key or unique constraint")
		}
		insertSQL, err := s.db.dialect.RenderInsert(table.Name, cols, returningColumns(table))
		if err != nil {
			return err
		}
		set := upsertSetClauses(table, values, conflict, s.db.dialect)
		sqlText := strings.TrimSuffix(insertSQL, ";") + " ON CONFLICT (" + strings.Join(quoteColumns(conflict, s.db.dialect), ", ") + ") DO UPDATE SET " + strings.Join(set, ", ") + ";"
		return s.db.execReturning(ctx, sqlText, args, model, table)
	})
}

func (db *DB) SQLDB() *sql.DB {
	if db == nil {
		return nil
	}
	return db.db
}

func (db *DB) Dialect() dialect.Dialect {
	if db == nil {
		return nil
	}
	return db.dialect
}

func (db *DB) PingContext(ctx context.Context) error {
	return db.traceOperation(ctx, "db.ping", []Attribute{{Key: "orm.operation", Value: "ping"}}, func(ctx context.Context) error {
		if db == nil {
			return errkind.New(errkind.KindConfiguration, "orm: nil db")
		}
		if db.tx != nil {
			return nil
		}
		if db.db == nil {
			return errkind.New(errkind.KindConfiguration, "orm: nil db")
		}
		if err := db.db.PingContext(ctx); err != nil {
			return errkind.Wrap(errkind.KindRuntimeQuery, "orm: ping", err)
		}
		return nil
	})
}

func (db *DB) Ping(ctx context.Context) error {
	return db.PingContext(ctx)
}

func (db *DB) Stats() sql.DBStats {
	if db == nil || db.db == nil {
		return sql.DBStats{}
	}
	return db.db.Stats()
}

func (db *DB) Close() error {
	return db.traceOperation(context.Background(), "db.close", []Attribute{{Key: "orm.operation", Value: "close"}}, func(context.Context) error {
		if db == nil {
			return nil
		}
		db.stmtMu.Lock()
		for _, stmt := range db.stmts {
			_ = stmt.Close()
		}
		db.stmts = map[string]*sql.Stmt{}
		db.stmtMu.Unlock()
		if db.tx != nil {
			return nil
		}
		if db.db == nil {
			return nil
		}
		if err := db.db.Close(); err != nil {
			return errkind.Wrap(errkind.KindRuntimeQuery, "orm: close", err)
		}
		return nil
	})
}

type QueryOption func(*queryState)

func Where(expr string, args ...any) QueryOption {
	return func(q *queryState) {
		q.where = append(q.where, predicate{expr: expr, args: args})
	}
}

func OrderBy(expr string) QueryOption {
	return func(q *queryState) {
		q.orderBy = append(q.orderBy, expr)
	}
}

func Limit(n int) QueryOption {
	return func(q *queryState) {
		q.limit = &n
	}
}

func Offset(n int) QueryOption {
	return func(q *queryState) {
		q.offset = &n
	}
}

type queryState struct {
	table       *schema.Table
	columns     []string
	where       []predicate
	orderBy     []string
	limit       *int
	offset      *int
	withDeleted bool
}

type predicate struct {
	expr string
	args []any
}

func (s *Session) buildState(dest any, opts ...QueryOption) (*queryState, error) {
	meta, err := s.db.resolveDest(dest)
	if err != nil {
		return nil, err
	}
	s.db.logPolicyOverride(s.ctx)
	state := &queryState{table: meta.table, columns: columnsForTable(meta.table)}
	if s.withDeleted {
		state.withDeleted = true
	}
	for _, opt := range opts {
		opt(state)
	}
	if !state.withDeleted && access.PolicyFromContext(s.ctx).EnforcesSoftDelete() && softDeleteColumn(meta.table) != nil {
		state.where = append(state.where, predicate{expr: softDeleteColumn(meta.table).Name + " IS NULL"})
	}
	preds, _, err := s.db.access.Apply(s.ctx, meta.table, access.OpQuery, nil)
	if err != nil {
		return nil, err
	}
	for _, pred := range preds {
		state.where = append(state.where, predicate{expr: pred.SQL, args: pred.Args})
	}
	return state, nil
}

func (db *DB) buildSelectSQL(ctx context.Context, state *queryState) (string, []any, error) {
	var clauses []string
	var args []any
	for _, p := range state.where {
		clause, clauseArgs := bindClause(p.expr, p.args, len(args)+1, db.dialect)
		clauses = append(clauses, clause)
		args = append(args, clauseArgs...)
	}
	sqlText, err := db.dialect.RenderSelect(state.table.Name, quoteColumns(state.columns, db.dialect), clauses, state.orderBy, state.limit, state.offset)
	if err != nil {
		return "", nil, errkind.Wrap(errkind.KindUnsupportedFeature, "orm: render select", err)
	}
	return sqlText, args, nil
}

func (db *DB) resolveDest(dest any) (*tableMeta, error) {
	if dest == nil {
		return nil, errkind.New(errkind.KindConfiguration, "orm: nil destination")
	}
	rt := reflect.TypeOf(dest)
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Slice {
		return nil, errkind.New(errkind.KindInvalidSchema, "orm: destination must be pointer to slice")
	}
	elem := rt.Elem()
	if elem.Kind() == reflect.Pointer {
		elem = elem.Elem()
	}
	if elem.Kind() != reflect.Struct {
		return nil, errkind.New(errkind.KindInvalidSchema, "orm: slice element must be struct")
	}
	table := db.lookupTableForType(elem)
	return &tableMeta{table: table, typ: elem}, nil
}

type tableMeta struct {
	table *schema.Table
	typ   reflect.Type
}

func (db *DB) lookupTableForType(t reflect.Type) *schema.Table {
	if cached, ok := tableMetaCache.Load(t); ok {
		if table, ok := cached.(*schema.Table); ok {
			return table
		}
	}
	if db.schema != nil {
		for _, table := range db.schema.Tables {
			if table == nil {
				continue
			}
			if table.GoTypeName == t.Name() {
				tableMetaCache.Store(t, table)
				return table
			}
		}
	}
	table := &schema.Table{
		Name:        schema.Pluralize(schema.ToSnakeCase(t.Name())),
		GoTypeName:  t.Name(),
		PackageName: t.PkgPath(),
		Columns:     columnsFromType(t),
	}
	tableMetaCache.Store(t, table)
	return table
}

func (db *DB) resolveModel(model any) (*tableMeta, reflect.Value, error) {
	rv := reflect.ValueOf(model)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return nil, reflect.Value{}, errkind.New(errkind.KindConfiguration, "orm: model must be non-nil pointer")
	}
	elem := rv.Elem()
	if elem.Kind() != reflect.Struct {
		return nil, reflect.Value{}, errkind.New(errkind.KindConfiguration, "orm: model must point to struct")
	}
	return &tableMeta{table: db.lookupTableForType(elem.Type()), typ: elem.Type()}, elem, nil
}

func columnsFromType(t reflect.Type) []*schema.Column {
	return schema.ColumnsFromType(t)
}

func reflectTypeToSchemaType(t reflect.Type) schema.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if typ, ok := schema.ResolveCustomType(t); ok {
		return typ
	}
	switch t.Kind() {
	case reflect.Bool:
		return schema.Type{Name: "boolean", Kind: schema.TypeBool}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return schema.Type{Name: "bigint", Kind: schema.TypeInt}
	case reflect.Float32, reflect.Float64:
		return schema.Type{Name: "double precision", Kind: schema.TypeFloat}
	case reflect.String:
		return schema.Type{Name: "text", Kind: schema.TypeString}
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return schema.Type{Name: "bytea", Kind: schema.TypeBytes}
		}
		elem := reflectTypeToSchemaType(t.Elem())
		return schema.Type{Name: elem.Name + "[]", Kind: schema.TypeArray, ArrayOf: &elem}
	case reflect.Struct:
		if t.PkgPath() == "time" && t.Name() == "Time" {
			return schema.Type{Name: "timestamptz", Kind: schema.TypeTime}
		}
		if strings.EqualFold(t.Name(), "UUID") {
			return schema.Type{Name: "uuid", Kind: schema.TypeUUID}
		}
		return schema.Type{Name: schema.ToSnakeCase(t.Name()), Kind: schema.TypeCustom}
	default:
		return schema.Type{Name: "text", Kind: schema.TypeUnknown}
	}
}

func scanIntoSlice(rows *sql.Rows, dest any, table *schema.Table) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.Elem().Kind() != reflect.Slice {
		return errkind.New(errkind.KindInvalidSchema, "orm: destination must be pointer to slice")
	}
	slice := rv.Elem()
	elemType := slice.Type().Elem()
	isPtr := false
	if elemType.Kind() == reflect.Pointer {
		isPtr = true
		elemType = elemType.Elem()
	}
	cols, err := rows.Columns()
	if err != nil {
		return errkind.Wrap(errkind.KindRuntimeQuery, "orm: rows columns", err)
	}
	fieldMap := structFieldIndexMap(elemType)
	for rows.Next() {
		holder := make([]any, len(cols))
		for i := range holder {
			var value any
			holder[i] = &value
		}
		if err := rows.Scan(holder...); err != nil {
			return errkind.Wrap(errkind.KindRuntimeQuery, "orm: scan row", err)
		}
		item := reflect.New(elemType).Elem()
		for i, col := range cols {
			idx, ok := fieldMap[strings.ToLower(schema.ToSnakeCase(col))]
			if !ok {
				continue
			}
			f := item.FieldByIndex(idx)
			if !f.CanSet() {
				continue
			}
			if valuePtr, ok := holder[i].(*any); ok {
				assignReflectValue(f, *valuePtr)
			}
		}
		if isPtr {
			slice = reflect.Append(slice, item.Addr())
		} else {
			slice = reflect.Append(slice, item)
		}
	}
	rv.Elem().Set(slice)
	if err := rows.Err(); err != nil {
		return errkind.Wrap(errkind.KindRuntimeQuery, "orm: scan rows", err)
	}
	return nil
}

func structFieldIndexMap(t reflect.Type) map[string][]int {
	if fieldMap := schema.StructFieldIndexMap(t); fieldMap != nil {
		return fieldMap
	}
	out := map[string][]int{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		out[strings.ToLower(schema.ToSnakeCase(f.Name))] = []int{i}
	}
	return out
}

func assignReflectValue(dst reflect.Value, value any) {
	if !dst.CanSet() || value == nil {
		return
	}
	if dst.CanAddr() {
		if scanner, ok := dst.Addr().Interface().(sql.Scanner); ok {
			_ = scanner.Scan(value)
			return
		}
	}
	v := reflect.ValueOf(value)
	if v.Type().AssignableTo(dst.Type()) {
		dst.Set(v)
		return
	}
	if dst.Kind() == reflect.Pointer {
		elem := reflect.New(dst.Type().Elem())
		if scanner, ok := elem.Interface().(sql.Scanner); ok {
			_ = scanner.Scan(value)
			dst.Set(elem)
			return
		}
		assignReflectValue(elem.Elem(), value)
		dst.Set(elem)
		return
	}
	switch dst.Kind() {
	case reflect.String:
		dst.SetString(fmt.Sprint(value))
	case reflect.Bool:
		if b, ok := value.(bool); ok {
			dst.SetBool(b)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		setInt(dst, value)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		setUint(dst, value)
	case reflect.Float32, reflect.Float64:
		setFloat(dst, value)
	default:
		if v.Type().ConvertibleTo(dst.Type()) {
			dst.Set(v.Convert(dst.Type()))
		}
	}
}

func setInt(dst reflect.Value, value any) {
	switch v := value.(type) {
	case int64:
		dst.SetInt(v)
	case int:
		dst.SetInt(int64(v))
	case float64:
		dst.SetInt(int64(v))
	}
}

func setUint(dst reflect.Value, value any) {
	switch v := value.(type) {
	case int64:
		dst.SetUint(uint64(v))
	case int:
		dst.SetUint(uint64(v))
	case uint64:
		dst.SetUint(v)
	}
}

func setFloat(dst reflect.Value, value any) {
	switch v := value.(type) {
	case float64:
		dst.SetFloat(v)
	case float32:
		dst.SetFloat(float64(v))
	case int64:
		dst.SetFloat(float64(v))
	}
}

func populateAuditAndScope(ctx context.Context, table *schema.Table, rv reflect.Value, values map[string]any, now time.Time, op access.Operation) error {
	preds, writes, err := access.NewEngine().Apply(ctx, table, op, nil)
	_ = preds
	if err != nil {
		return err
	}
	for _, w := range writes {
		values[w.Field] = w.Value
		setFieldByColumn(rv, table, w.Field, w.Value)
	}
	for _, col := range table.Columns {
		if !access.PolicyFromContext(ctx).EnforcesAudit() {
			continue
		}
		switch {
		case col.CreatedAt && op == access.OpInsert:
			values[col.Name] = now
			setFieldByColumn(rv, table, col.Name, now)
		case col.UpdatedAt:
			values[col.Name] = now
			setFieldByColumn(rv, table, col.Name, now)
		case col.DeletedAt && op == access.OpDelete:
			values[col.Name] = now
			setFieldByColumn(rv, table, col.Name, now)
		}
	}
	return nil
}

func applyStructFieldValues(ctx context.Context, table *schema.Table, rv reflect.Value, values map[string]any, now time.Time, isInsert bool) error {
	if !access.PolicyFromContext(ctx).EnforcesAudit() {
		return nil
	}
	for _, col := range table.Columns {
		if _, ok := values[col.Name]; ok {
			continue
		}
		field := fieldByColumn(rv, table, col.Name)
		if !field.IsValid() || !field.CanInterface() {
			continue
		}
		v := field.Interface()
		if isZeroValue(v) {
			switch {
			case col.CreatedAt && isInsert:
				values[col.Name] = now
				setFieldByColumn(rv, table, col.Name, now)
			case col.UpdatedAt:
				values[col.Name] = now
				setFieldByColumn(rv, table, col.Name, now)
			case col.DeletedAt && !isInsert:
				values[col.Name] = now
				setFieldByColumn(rv, table, col.Name, now)
			case col.CreatedBy || col.UpdatedBy || col.DeletedBy || col.Scope != schema.ScopeNone:
				// already handled by access layer if context exists
			default:
				values[col.Name] = v
			}
			continue
		}
		values[col.Name] = v
	}
	return nil
}

func orderedColumnsAndArgs(table *schema.Table, values map[string]any, includePrimary bool) ([]string, []any) {
	var cols []string
	var args []any
	for _, col := range table.Columns {
		if !includePrimary && col.PrimaryKey {
			continue
		}
		v, ok := values[col.Name]
		if !ok {
			continue
		}
		cols = append(cols, col.Name)
		args = append(args, v)
	}
	return cols, args
}

func returningColumns(table *schema.Table) []string {
	var cols []string
	for _, col := range table.Columns {
		if col.PrimaryKey || col.CreatedAt || col.UpdatedAt || col.DeletedAt {
			cols = append(cols, col.Name)
		}
	}
	return cols
}

func updateSetClauses(table *schema.Table, values map[string]any, pkCols []string, d dialect.Dialect) ([]string, []any) {
	pk := map[string]struct{}{}
	for _, name := range pkCols {
		pk[name] = struct{}{}
	}
	var set []string
	var args []any
	argIndex := 1
	for _, col := range table.Columns {
		if _, ok := pk[col.Name]; ok {
			continue
		}
		v, ok := values[col.Name]
		if !ok {
			continue
		}
		set = append(set, fmt.Sprintf("%s = %s", d.QuoteIdent(col.Name), d.Placeholder(argIndex)))
		args = append(args, v)
		argIndex++
	}
	return set, args
}

func upsertSetClauses(table *schema.Table, values map[string]any, conflict []string, d dialect.Dialect) []string {
	conflictSet := map[string]struct{}{}
	for _, name := range conflict {
		conflictSet[name] = struct{}{}
	}
	var set []string
	for _, col := range table.Columns {
		if _, ok := conflictSet[col.Name]; ok {
			continue
		}
		if _, ok := values[col.Name]; !ok {
			continue
		}
		set = append(set, fmt.Sprintf("%s = EXCLUDED.%s", d.QuoteIdent(col.Name), d.QuoteIdent(col.Name)))
	}
	return set
}

func conflictColumns(table *schema.Table) []string {
	var cols []string
	for _, col := range table.Columns {
		if col.PrimaryKey || col.Unique {
			cols = append(cols, col.Name)
		}
	}
	return cols
}

func buildWhereClauses(cols []string, args []any, offset int, extra []predicate, d dialect.Dialect) ([]string, []any) {
	var where []string
	whereArgs := append([]any(nil), args...)
	argIndex := offset + 1
	for _, col := range cols {
		where = append(where, d.QuoteIdent(col)+" = "+d.Placeholder(argIndex))
		argIndex++
	}
	for _, p := range extra {
		clause, clauseArgs := bindClause(p.expr, p.args, argIndex, d)
		where = append(where, clause)
		whereArgs = append(whereArgs, clauseArgs...)
		argIndex += len(clauseArgs)
	}
	return where, whereArgs
}

func accessPredicates(ctx context.Context, table *schema.Table) []predicate {
	preds, _, _ := access.NewEngine().Apply(ctx, table, access.OpQuery, nil)
	var clauses []predicate
	for _, pred := range preds {
		clauses = append(clauses, predicate{expr: pred.SQL, args: pred.Args})
	}
	return clauses
}

func softDeleteColumn(table *schema.Table) *schema.Column {
	return access.SoftDeleteColumn(table)
}

func (s *Session) softDelete(ctx context.Context, model any, table *schema.Table, rv reflect.Value, soft *schema.Column) error {
	now := time.Now().UTC()
	values := map[string]any{}
	if err := populateAuditAndScope(ctx, table, rv, values, now, access.OpDelete); err != nil {
		return err
	}
	values[soft.Name] = now
	setFieldByColumn(rv, table, soft.Name, now)
	pkCols, pkArgs, err := primaryKeyValues(table, rv)
	if err != nil {
		return err
	}
	set, args := updateSetClauses(table, values, pkCols, s.db.dialect)
	if len(set) == 0 {
		set = []string{s.db.dialect.QuoteIdent(soft.Name) + " = " + s.db.dialect.Placeholder(1)}
		args = []any{now}
	}
	whereSQL, whereArgs := buildWhereClauses(pkCols, pkArgs, len(args), accessPredicates(ctx, table), s.db.dialect)
	sqlText, err := s.db.dialect.RenderUpdate(table.Name, set, whereSQL, nil)
	if err != nil {
		return errkind.Wrap(errkind.KindUnsupportedFeature, "orm: render soft delete", err)
	}
	_, err = s.db.execContext(ctx, sqlText, append(args, whereArgs...)...)
	_ = values
	if err != nil {
		return errkind.Wrap(errkind.KindRuntimeQuery, "orm: soft delete", err)
	}
	return nil
}

func (s *Session) delete(ctx context.Context, model any) error {
	if err := invokeBeforeDeleteHook(ctx, s.db, model); err != nil {
		return err
	}
	meta, rv, err := s.resolveModel(model)
	if err != nil {
		return err
	}
	table := meta.table
	if soft := softDeleteColumn(table); soft != nil && access.PolicyFromContext(ctx).EnforcesSoftDelete() {
		if err := s.softDelete(ctx, model, table, rv, soft); err != nil {
			return err
		}
		return invokeAfterDeleteHook(ctx, s.db, model)
	}
	pkCols, pkArgs, err := primaryKeyValues(table, rv)
	if err != nil {
		return err
	}
	if len(pkCols) == 0 {
		return errkind.New(errkind.KindInvalidSchema, "orm: delete requires primary key")
	}
	whereSQL, whereArgs := buildWhereClauses(pkCols, pkArgs, 0, accessPredicates(ctx, table), s.db.dialect)
	sqlText, err := s.db.dialect.RenderDelete(table.Name, whereSQL, nil)
	if err != nil {
		return errkind.Wrap(errkind.KindUnsupportedFeature, "orm: render delete", err)
	}
	_, err = s.db.execContext(ctx, sqlText, whereArgs...)
	if err != nil {
		return err
	}
	return invokeAfterDeleteHook(ctx, s.db, model)
}

func (db *DB) batchSizeLimit(total int) int {
	if db == nil || db.batchSize <= 0 {
		if total <= 0 {
			return defaultBatchSize
		}
		if total < defaultBatchSize {
			return total
		}
		return defaultBatchSize
	}
	if total <= 0 || total > db.batchSize {
		return db.batchSize
	}
	return total
}

func primaryKeyValues(table *schema.Table, rv reflect.Value) ([]string, []any, error) {
	var cols []string
	var args []any
	for _, col := range table.Columns {
		if !col.PrimaryKey {
			continue
		}
		field := fieldByColumn(rv, table, col.Name)
		if !field.IsValid() {
			return nil, nil, errkind.New(errkind.KindInvalidSchema, fmt.Sprintf("orm: missing primary key field %s", col.Name))
		}
		cols = append(cols, col.Name)
		args = append(args, field.Interface())
	}
	return cols, args, nil
}

func primaryKeyColumnNames(table *schema.Table) []string {
	if table == nil {
		return nil
	}
	var cols []string
	for _, col := range table.Columns {
		if col.PrimaryKey {
			cols = append(cols, col.Name)
		}
	}
	return cols
}

func fieldByColumn(rv reflect.Value, table *schema.Table, col string) reflect.Value {
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	indexMap := structFieldIndexMap(rv.Type())
	if idx, ok := indexMap[strings.ToLower(schema.ToSnakeCase(col))]; ok {
		return rv.FieldByIndex(idx)
	}
	return reflect.Value{}
}

func setFieldByColumn(rv reflect.Value, table *schema.Table, col string, value any) {
	field := fieldByColumn(rv, table, col)
	if !field.IsValid() || !field.CanSet() {
		return
	}
	assignReflectValue(field, value)
}

func quoteColumns(cols []string, d dialect.Dialect) []string {
	out := make([]string, 0, len(cols))
	for _, col := range cols {
		out = append(out, d.QuoteIdent(col))
	}
	return out
}

func columnsForTable(table *schema.Table) []string {
	if table == nil {
		return nil
	}
	cols := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		cols = append(cols, col.Name)
	}
	return cols
}

func bindClause(expr string, args []any, start int, d dialect.Dialect) (string, []any) {
	if expr == "" {
		return "", nil
	}
	if len(args) == 0 {
		return expr, nil
	}
	var b strings.Builder
	next := start
	for _, r := range expr {
		if r == '?' {
			b.WriteString(d.Placeholder(next))
			next++
			continue
		}
		b.WriteRune(r)
	}
	return b.String(), append([]any(nil), args...)
}

func isZeroValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map:
		return rv.IsNil()
	default:
		return rv.IsZero()
	}
}

func (db *DB) execContext(ctx context.Context, sqlText string, args ...any) (sql.Result, error) {
	if err := db.transactionClosedError("exec"); err != nil {
		return nil, err
	}
	start := time.Now()
	entry := SQLLogEntry{
		SQL:        sqlText,
		Args:       append([]any(nil), args...),
		Timestamp:  start,
		Visibility: db.observability.TraceSQL,
		Driver:     db.driverName,
		Dialect:    db.dialectName(),
		Operation:  "exec",
	}
	if db.observability.MaskParameters && db.observability.TraceSQL == TraceSQLStatementWithArgs {
		entry.Args = maskSQLArgs(db.observability, entry.Args)
	}
	var result sql.Result
	err := db.traceWithSpan(ctx, "db.sql.exec", sqlTraceVisibilityAttrs(entry, db), func(ctx context.Context, span Span) error {
		if db.tx != nil {
			res, execErr := db.tx.ExecContext(ctx, sqlText, args...)
			if execErr != nil {
				wrapped := errkind.Wrap(errkind.KindRuntimeQuery, "orm: exec", execErr)
				duration := time.Since(start)
				entry.Duration = duration
				entry.Err = wrapped
				entry.Slow = isSlowQuery(duration, db.observability.SlowQueryThreshold)
				db.recordExecMetrics(ctx, duration, wrapped, -1)
				if span != nil {
					span.SetAttributes(sqlTraceVisibilityAttrs(entry, db)...)
				}
				db.logSQL(ctx, entry)
				return wrapped
			}
			result = res
		} else {
			if db.db == nil {
				return errkind.New(errkind.KindConfiguration, "orm: nil db")
			}
			if stmt, ok := db.prepared(sqlText); ok {
				res, execErr := stmt.ExecContext(ctx, args...)
				if execErr != nil {
					wrapped := errkind.Wrap(errkind.KindRuntimeQuery, "orm: exec", execErr)
					duration := time.Since(start)
					entry.Duration = duration
					entry.Err = wrapped
					entry.Slow = isSlowQuery(duration, db.observability.SlowQueryThreshold)
					db.recordExecMetrics(ctx, duration, wrapped, -1)
					if span != nil {
						span.SetAttributes(sqlTraceVisibilityAttrs(entry, db)...)
					}
					db.logSQL(ctx, entry)
					return wrapped
				}
				result = res
			} else {
				res, execErr := db.db.ExecContext(ctx, sqlText, args...)
				if execErr != nil {
					wrapped := errkind.Wrap(errkind.KindRuntimeQuery, "orm: exec", execErr)
					duration := time.Since(start)
					entry.Duration = duration
					entry.Err = wrapped
					entry.Slow = isSlowQuery(duration, db.observability.SlowQueryThreshold)
					db.recordExecMetrics(ctx, duration, wrapped, -1)
					if span != nil {
						span.SetAttributes(sqlTraceVisibilityAttrs(entry, db)...)
					}
					db.logSQL(ctx, entry)
					return wrapped
				}
				result = res
			}
		}
		rowsAffected := int64(-1)
		if result != nil {
			if count, countErr := result.RowsAffected(); countErr == nil {
				rowsAffected = count
			}
		}
		duration := time.Since(start)
		entry.Duration = duration
		entry.AffectedRows = rowsAffected
		entry.Slow = isSlowQuery(duration, db.observability.SlowQueryThreshold)
		db.recordExecMetrics(ctx, duration, nil, rowsAffected)
		if span != nil {
			span.SetAttributes(sqlTraceVisibilityAttrs(entry, db)...)
		}
		db.logSQL(ctx, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (db *DB) queryContext(ctx context.Context, sqlText string, args ...any) (*sql.Rows, error) {
	if err := db.transactionClosedError("query"); err != nil {
		return nil, err
	}
	start := time.Now()
	entry := SQLLogEntry{
		SQL:        sqlText,
		Args:       append([]any(nil), args...),
		Timestamp:  start,
		Visibility: db.observability.TraceSQL,
		Driver:     db.driverName,
		Dialect:    db.dialectName(),
		Operation:  "query",
	}
	if db.observability.MaskParameters && db.observability.TraceSQL == TraceSQLStatementWithArgs {
		entry.Args = maskSQLArgs(db.observability, entry.Args)
	}
	var rows *sql.Rows
	err := db.traceWithSpan(ctx, "db.sql.query", sqlTraceVisibilityAttrs(entry, db), func(ctx context.Context, span Span) error {
		if db.tx != nil {
			res, queryErr := db.tx.QueryContext(ctx, sqlText, args...)
			if queryErr != nil {
				wrapped := errkind.Wrap(errkind.KindRuntimeQuery, "orm: query", queryErr)
				duration := time.Since(start)
				entry.Duration = duration
				entry.Err = wrapped
				entry.Slow = isSlowQuery(duration, db.observability.SlowQueryThreshold)
				db.recordQueryMetrics(ctx, duration, wrapped, -1)
				if span != nil {
					span.SetAttributes(sqlTraceVisibilityAttrs(entry, db)...)
				}
				db.logSQL(ctx, entry)
				return wrapped
			}
			rows = res
		} else {
			if db.db == nil {
				return errkind.New(errkind.KindConfiguration, "orm: nil db")
			}
			if stmt, ok := db.prepared(sqlText); ok {
				res, queryErr := stmt.QueryContext(ctx, args...)
				if queryErr != nil {
					wrapped := errkind.Wrap(errkind.KindRuntimeQuery, "orm: query", queryErr)
					duration := time.Since(start)
					entry.Duration = duration
					entry.Err = wrapped
					entry.Slow = isSlowQuery(duration, db.observability.SlowQueryThreshold)
					db.recordQueryMetrics(ctx, duration, wrapped, -1)
					if span != nil {
						span.SetAttributes(sqlTraceVisibilityAttrs(entry, db)...)
					}
					db.logSQL(ctx, entry)
					return wrapped
				}
				rows = res
			} else {
				res, queryErr := db.db.QueryContext(ctx, sqlText, args...)
				if queryErr != nil {
					wrapped := errkind.Wrap(errkind.KindRuntimeQuery, "orm: query", queryErr)
					duration := time.Since(start)
					entry.Duration = duration
					entry.Err = wrapped
					entry.Slow = isSlowQuery(duration, db.observability.SlowQueryThreshold)
					db.recordQueryMetrics(ctx, duration, wrapped, -1)
					if span != nil {
						span.SetAttributes(sqlTraceVisibilityAttrs(entry, db)...)
					}
					db.logSQL(ctx, entry)
					return wrapped
				}
				rows = res
			}
		}
		duration := time.Since(start)
		entry.Duration = duration
		entry.Slow = isSlowQuery(duration, db.observability.SlowQueryThreshold)
		db.recordQueryMetrics(ctx, duration, nil, -1)
		if span != nil {
			span.SetAttributes(sqlTraceVisibilityAttrs(entry, db)...)
		}
		db.logSQL(ctx, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (db *DB) execReturning(ctx context.Context, sqlText string, args []any, model any, table *schema.Table) error {
	if !strings.Contains(strings.ToUpper(sqlText), " RETURNING ") {
		_, err := db.execContext(ctx, sqlText, args...)
		return err
	}
	rows, err := db.queryContext(ctx, sqlText, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			return errkind.Wrap(errkind.KindRuntimeQuery, "orm: rows columns", err)
		}
		holder := make([]any, len(cols))
		for i := range holder {
			var value any
			holder[i] = &value
		}
		if err := rows.Scan(holder...); err != nil {
			return errkind.Wrap(errkind.KindRuntimeQuery, "orm: scan returning row", err)
		}
		rv := reflect.ValueOf(model)
		if rv.Kind() == reflect.Pointer && !rv.IsNil() {
			for i, col := range cols {
				if valuePtr, ok := holder[i].(*any); ok {
					setFieldByColumn(rv, table, col, *valuePtr)
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return errkind.Wrap(errkind.KindRuntimeQuery, "orm: returning rows", err)
	}
	return nil
}

func (db *DB) logPolicyOverride(ctx context.Context) {
	if db == nil || db.logger == nil {
		return
	}
	policy := access.PolicyFromContext(ctx)
	if policy.IsDefault() {
		return
	}
	db.logger.Printf("access policy override: %s", policy.Name())
}

func (db *DB) prepared(sqlText string) (*sql.Stmt, bool) {
	if !db.prepareStatements {
		return nil, false
	}
	db.stmtMu.Lock()
	defer db.stmtMu.Unlock()
	if stmt, ok := db.stmts[sqlText]; ok {
		return stmt, true
	}
	var stmt *sql.Stmt
	var err error
	if db.tx != nil {
		stmt, err = db.tx.PrepareContext(context.Background(), sqlText)
	} else if db.db != nil {
		stmt, err = db.db.PrepareContext(context.Background(), sqlText)
	}
	if err != nil {
		return nil, false
	}
	db.stmts[sqlText] = stmt
	return stmt, true
}
