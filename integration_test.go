package dorm_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dionisius77/dorm/access"
	"github.com/dionisius77/dorm/dialect/postgres"
	driverpostgres "github.com/dionisius77/dorm/driver/postgres"
	"github.com/dionisius77/dorm/internal/itest"
	"github.com/dionisius77/dorm/migrate"
	"github.com/dionisius77/dorm/orm"
	"github.com/dionisius77/dorm/schema"
	"github.com/dionisius77/dorm/seed"
)

const integrationModelSource = `package models

import (
	"time"
)

type Product struct {
	ID         string     ` + "`orm:\"pk\"`" + `
	CompanyID  string     ` + "`orm:\"company\"`" + `
	SKU        string     ` + "`orm:\"unique\"`" + `
	Name       string
	PriceCents int64
	DeletedAt  *time.Time ` + "`orm:\"soft_delete\"`" + `
	CreatedAt  time.Time  ` + "`orm:\"created_at\"`" + `
	CreatedBy  string     ` + "`orm:\"created_by\"`" + `
	UpdatedAt  time.Time  ` + "`orm:\"updated_at\"`" + `
	UpdatedBy  string     ` + "`orm:\"updated_by\"`" + `
	DeletedBy  string     ` + "`orm:\"deleted_by\"`" + `
}

type Role struct {
	ID        string    ` + "`orm:\"pk\"`" + `
	Code      string    ` + "`orm:\"unique\"`" + `
	Name      string
	CreatedAt time.Time ` + "`orm:\"created_at\"`" + `
	UpdatedAt time.Time ` + "`orm:\"updated_at\"`" + `
}
`

type requestIDKey struct{}

type sqlCaptureLogger struct {
	mu       sync.Mutex
	entries  []orm.SQLLogEntry
	requests []string
}

func (l *sqlCaptureLogger) LogSQL(ctx context.Context, entry orm.SQLLogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entry)
	if v, ok := ctx.Value(requestIDKey{}).(string); ok {
		l.requests = append(l.requests, v)
	}
}

func (l *sqlCaptureLogger) LogError(context.Context, error) {}

func (l *sqlCaptureLogger) Entries() []orm.SQLLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]orm.SQLLogEntry(nil), l.entries...)
}

func (l *sqlCaptureLogger) Requests() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.requests...)
}

func TestIntegrationORMHappyPaths(t *testing.T) {
	project, _ := prepareMigratedProject(t)

	logger := &sqlCaptureLogger{}
	db := project.OpenDB(t, orm.ObservabilityConfig{
		TraceSQL:   orm.TraceSQLStatement,
		SQLLogging: orm.SQLLogTrace,
		Logger:     logger,
	}, true)

	if err := db.Ping(context.Background()); err != nil {
		t.Fatalf("ping db: %v", err)
	}

	ctxA := access.WithContext(context.Background(), access.Context{
		CompanyID: "company-a",
		UserID:    "user-a",
		Values:    map[string]any{"request_id": "req-123"},
	})
	ctxA = context.WithValue(ctxA, requestIDKey{}, "req-123")
	ctxB := access.WithContext(context.Background(), access.Context{
		CompanyID: "company-b",
		UserID:    "user-a",
	})

	p1 := Product{
		ID:         "product-1",
		SKU:        "SKU-001",
		Name:       "Alpha",
		PriceCents: 100,
	}
	p2 := Product{
		ID:         "product-2",
		SKU:        "SKU-002",
		Name:       "Beta",
		PriceCents: 200,
	}
	p3 := Product{
		ID:         "product-3",
		SKU:        "SKU-003",
		Name:       "Gamma",
		PriceCents: 300,
	}
	p4 := Product{
		ID:         "product-4",
		SKU:        "SKU-004",
		Name:       "Delta",
		PriceCents: 400,
	}

	if err := db.WithContext(ctxA).Create(&p1); err != nil {
		t.Fatalf("create p1: %v", err)
	}
	if err := db.WithContext(ctxA).Create(&p2); err != nil {
		t.Fatalf("create p2: %v", err)
	}
	if err := db.WithContext(ctxB).Create(&p3); err != nil {
		t.Fatalf("create p3: %v", err)
	}
	if err := db.WithContext(ctxA).Create(&p4); err != nil {
		t.Fatalf("create p4: %v", err)
	}

	roleAdmin := Role{
		ID:   "role-admin",
		Code: "ADMIN",
		Name: "Administrator",
	}
	roleUser := Role{
		ID:   "role-user",
		Code: "USER",
		Name: "User",
	}
	roleViewer := Role{
		ID:   "role-viewer",
		Code: "VIEWER",
		Name: "Viewer",
	}
	if err := db.WithContext(context.Background()).Create(&roleAdmin); err != nil {
		t.Fatalf("create role admin: %v", err)
	}
	if err := db.WithContext(context.Background()).Create(&roleUser); err != nil {
		t.Fatalf("create role user: %v", err)
	}
	if err := db.WithContext(context.Background()).Create(&roleViewer); err != nil {
		t.Fatalf("create role viewer: %v", err)
	}

	var fetched []Product
	if err := db.WithContext(ctxA).Find(&fetched, orm.Where("sku = ?", p1.SKU)); err != nil {
		t.Fatalf("find created product: %v", err)
	}
	if len(fetched) != 1 {
		t.Fatalf("expected 1 product, got %d", len(fetched))
	}
	if fetched[0].CompanyID != "company-a" {
		t.Fatalf("expected company scope to be injected, got %s", fetched[0].CompanyID)
	}
	if fetched[0].CreatedAt.IsZero() || fetched[0].UpdatedAt.IsZero() {
		t.Fatalf("expected audit timestamps to be populated: %#v", fetched[0])
	}
	if fetched[0].CreatedBy != "user-a" || fetched[0].UpdatedBy != "user-a" {
		t.Fatalf("expected audit users to be populated: %#v", fetched[0])
	}

	p1.Name = "Alpha Prime"
	p1.PriceCents = 150
	if err := db.WithContext(ctxA).Update(&p1); err != nil {
		t.Fatalf("update product: %v", err)
	}
	fetched = nil
	if err := db.WithContext(ctxA).Find(&fetched, orm.Where("sku = ?", p1.SKU)); err != nil {
		t.Fatalf("find updated product: %v", err)
	}
	if len(fetched) != 1 || fetched[0].Name != "Alpha Prime" || fetched[0].UpdatedBy != "user-a" {
		t.Fatalf("expected updated product, got %#v", fetched)
	}

	var page []Role
	if err := db.WithContext(context.Background()).Find(&page, orm.Where("code <> ?", "VIEWER"), orm.OrderBy("code ASC"), orm.Limit(1), orm.Offset(1)); err != nil {
		t.Fatalf("query builder find: %v", err)
	}
	if len(page) != 1 || page[0].Code != "USER" {
		t.Fatalf("expected paginated role query to return USER, got %#v", page)
	}

	var queryOptions []Product
	if err := db.WithContext(ctxA).Find(&queryOptions,
		orm.Select("products.id, products.company_id, products.sku, products.name, products.price_cents, products.deleted_at, products.created_at, products.created_by, products.updated_at, products.updated_by, products.deleted_by"),
		orm.Distinct(),
		orm.LeftJoin("roles r", "r.id = products.id"),
		orm.Where("products.price_cents >= ?", 150),
		orm.OrderBy("products.price_cents DESC"),
		orm.Limit(2),
		orm.Offset(0),
	); err != nil {
		t.Fatalf("query options integration query: %v", err)
	}
	if len(queryOptions) != 2 {
		t.Fatalf("expected two rows from query options query, got %#v", queryOptions)
	}
	if got := skus(queryOptions); !equalStrings(got, []string{"SKU-004", "SKU-002"}) {
		t.Fatalf("unexpected query options rows: %v", got)
	}

	if err := db.WithContext(ctxA).Delete(&p1); err != nil {
		t.Fatalf("delete product: %v", err)
	}
	fetched = nil
	if err := db.WithContext(ctxA).WithDeleted().Find(&fetched, orm.Where("sku = ?", p1.SKU)); err != nil {
		t.Fatalf("find soft deleted product: %v", err)
	}
	if len(fetched) != 1 || fetched[0].DeletedAt == nil || fetched[0].DeletedBy != "user-a" {
		t.Fatalf("expected soft deleted row, got %#v", fetched)
	}

	var defaultVisible []Product
	if err := db.WithPolicy(access.Default()).WithContext(ctxA).Find(&defaultVisible, orm.OrderBy("sku ASC")); err != nil {
		t.Fatalf("default policy query: %v", err)
	}
	if got := skus(defaultVisible); !equalStrings(got, []string{"SKU-002", "SKU-004"}) {
		t.Fatalf("expected default policy to see only company A active rows, got %v", got)
	}

	var ignoreCompany []Product
	if err := db.WithPolicy(access.IgnoreCompany()).WithContext(ctxA).Find(&ignoreCompany, orm.OrderBy("sku ASC")); err != nil {
		t.Fatalf("ignore company query: %v", err)
	}
	if got := skus(ignoreCompany); !equalStrings(got, []string{"SKU-002", "SKU-003", "SKU-004"}) {
		t.Fatalf("expected ignore company to exclude soft deleted rows, got %v", got)
	}

	var ignoreRLS []Product
	if err := db.WithPolicy(access.IgnoreRLS()).WithContext(ctxA).Find(&ignoreRLS, orm.OrderBy("sku ASC")); err != nil {
		t.Fatalf("ignore rls query: %v", err)
	}
	if got := skus(ignoreRLS); !equalStrings(got, []string{"SKU-002", "SKU-003", "SKU-004"}) {
		t.Fatalf("expected ignore rls to keep soft delete enforced, got %v", got)
	}

	var systemRows []Product
	if err := db.WithPolicy(access.System()).WithContext(ctxA).Find(&systemRows, orm.OrderBy("sku ASC")); err != nil {
		t.Fatalf("system query: %v", err)
	}
	if got := skus(systemRows); !equalStrings(got, []string{"SKU-001", "SKU-002", "SKU-003", "SKU-004"}) {
		t.Fatalf("expected system policy to see all rows, got %v", got)
	}

	if err := db.Tx(context.Background(), func(tx *orm.Session) error {
		return tx.Create(&Role{
			ID:        "tx-ok",
			Code:      "TX_OK",
			Name:      "Committed",
			CreatedAt: time.Time{},
			UpdatedAt: time.Time{},
		})
	}); err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	var committed []Role
	if err := db.WithContext(context.Background()).Find(&committed, orm.Where("code = ?", "TX_OK")); err != nil {
		t.Fatalf("find committed row: %v", err)
	}
	if len(committed) != 1 {
		t.Fatalf("expected committed row, got %#v", committed)
	}

	err := db.Tx(context.Background(), func(tx *orm.Session) error {
		if err := tx.Create(&Role{
			ID:   "tx-rollback",
			Code: "TX_ROLLBACK",
			Name: "Rolled back",
		}); err != nil {
			return err
		}
		return errors.New("force rollback")
	})
	if err == nil {
		t.Fatal("expected rollback error")
	}
	var rolledBack []Role
	if err := db.WithContext(context.Background()).Find(&rolledBack, orm.Where("code = ?", "TX_ROLLBACK")); err != nil {
		t.Fatalf("find rolled back row: %v", err)
	}
	if len(rolledBack) != 0 {
		t.Fatalf("expected rollback to discard row, got %#v", rolledBack)
	}

	var observabilityRows []Product
	requestCtx := context.WithValue(ctxA, requestIDKey{}, "req-123")
	if err := db.WithContext(requestCtx).Find(&observabilityRows, orm.Where("sku = ?", "SKU-004")); err != nil {
		t.Fatalf("observability query: %v", err)
	}
	if len(logger.Entries()) == 0 {
		t.Fatal("expected sql log entries")
	}
	foundRequestID := false
	foundStatement := false
	for _, entry := range logger.Entries() {
		if entry.Visibility == orm.TraceSQLStatement && strings.TrimSpace(entry.SQL) != "" {
			foundStatement = true
		}
	}
	for _, request := range logger.Requests() {
		if request == "req-123" {
			foundRequestID = true
			break
		}
	}
	if !foundStatement {
		t.Fatal("expected statement-level sql visibility")
	}
	if !foundRequestID {
		t.Fatal("expected context to propagate to sql logger")
	}

	if err := db.Ping(context.Background()); err != nil {
		t.Fatalf("ping db again: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
}

func TestIntegrationMigrationGenerateRunRollbackAndDrift(t *testing.T) {
	project := itest.NewProject(t)
	project.WriteFile(t, "models/core.go", integrationModelSource)
	project.WriteJSON(t, "orm.json", map[string]any{
		"root":           ".",
		"models_dir":     "models",
		"migrations_dir": "migrations",
		"schemas_dir":    "schemas",
		"snapshot_path":  "schemas/current.snapshot.json",
		"schema_name":    project.Schema,
		"driver":         "postgres",
		"dsn":            project.DSN,
		"config_file":    "orm.json",
	})

	service := migrate.NewService(migrate.Config{
		Root:          project.ModelsDir,
		MigrationsDir: project.MigrationsDir,
		SnapshotPath:  project.SnapshotPath,
		SchemaName:    project.Schema,
		Dialect:       postgres.New(),
		Inspector:     driverpostgres.New(driverpostgres.Config{DSN: project.DSN, SchemaName: project.Schema}).Inspector(),
	})
	result, err := service.Generate(context.Background())
	if err != nil {
		t.Fatalf("generate migration: %v", err)
	}
	if result == nil || result.Diff == nil || result.Diff.Empty() {
		t.Fatal("expected migration diff")
	}
	if err := service.Write(result); err != nil {
		t.Fatalf("write migration: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(project.MigrationsDir, "*.sql"))
	if err != nil {
		t.Fatalf("glob migrations: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected generated migration files")
	}

	db := project.OpenDB(t, orm.ObservabilityConfig{}, false)
	if err := service.Run(context.Background(), db.SQLDB()); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	report, err := schema.DetectDriftFromSource(context.Background(), project.ModelsDir, driverpostgres.New(driverpostgres.Config{DSN: project.DSN, SchemaName: project.Schema}).Inspector(), db.SQLDB(), project.Schema, project.SnapshotPath)
	if err != nil {
		t.Fatalf("detect drift: %v", err)
	}
	if report == nil || !report.Clean() {
		t.Fatalf("expected clean schema drift report, got %#v", report)
	}
	if err := service.Revert(context.Background(), db.SQLDB(), result.MigrationName); err != nil {
		t.Fatalf("rollback migration: %v", err)
	}

	sqlDB := project.OpenSQL(t)
	defer sqlDB.Close()
	var tableCount int
	if err := sqlDB.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_name IN ('products', 'roles')
	`, project.Schema).Scan(&tableCount); err != nil {
		t.Fatalf("check tables after rollback: %v", err)
	}
	if tableCount != 0 {
		t.Fatalf("expected rollback to remove generated tables, found %d", tableCount)
	}
}

func TestIntegrationSeedSync(t *testing.T) {
	project, _ := prepareMigratedProject(t)
	db := project.OpenDB(t, orm.ObservabilityConfig{}, true)

	roleA := Role{
		ID:   "seed-admin",
		Code: "ADMIN",
		Name: "Administrator",
	}
	roleB := Role{
		ID:   "seed-user",
		Code: "USER",
		Name: "User",
	}
	if err := seed.Sync(context.Background(), db, []Role{roleA, roleB}, seed.Key("Code")); err != nil {
		t.Fatalf("seed sync insert: %v", err)
	}
	roleA.Name = "Super Administrator"
	if err := seed.Sync(context.Background(), db, []Role{roleA, roleB}, seed.Key("Code")); err != nil {
		t.Fatalf("seed sync update: %v", err)
	}
	var roles []Role
	if err := db.WithContext(context.Background()).Find(&roles, orm.OrderBy("code ASC")); err != nil {
		t.Fatalf("load seed roles: %v", err)
	}
	if len(roles) != 2 {
		t.Fatalf("expected two seeded roles, got %#v", roles)
	}
	if roles[0].Code != "ADMIN" || roles[0].Name != "Super Administrator" {
		t.Fatalf("expected seeded role to be updated, got %#v", roles[0])
	}
	if roles[1].Code != "USER" || roles[1].Name != "User" {
		t.Fatalf("unexpected second role, got %#v", roles[1])
	}
}

func prepareMigratedProject(t *testing.T) (*itest.Project, *migrate.Result) {
	t.Helper()
	project := itest.NewProject(t)
	project.WriteFile(t, "models/core.go", integrationModelSource)
	project.WriteJSON(t, "orm.json", map[string]any{
		"root":           ".",
		"models_dir":     "models",
		"migrations_dir": "migrations",
		"schemas_dir":    "schemas",
		"snapshot_path":  "schemas/current.snapshot.json",
		"schema_name":    project.Schema,
		"driver":         "postgres",
		"dsn":            project.DSN,
		"config_file":    "orm.json",
	})

	service := migrate.NewService(migrate.Config{
		Root:          project.ModelsDir,
		MigrationsDir: project.MigrationsDir,
		SnapshotPath:  project.SnapshotPath,
		SchemaName:    project.Schema,
		Dialect:       postgres.New(),
		Inspector:     driverpostgres.New(driverpostgres.Config{DSN: project.DSN, SchemaName: project.Schema}).Inspector(),
	})
	result, err := service.Generate(context.Background())
	if err != nil {
		t.Fatalf("generate migration: %v", err)
	}
	if result == nil || result.Diff == nil || result.Diff.Empty() {
		t.Fatal("expected a non-empty migration diff")
	}
	if err := service.Write(result); err != nil {
		t.Fatalf("write migration: %v", err)
	}

	db := project.OpenDB(t, orm.ObservabilityConfig{}, false)
	if err := service.Run(context.Background(), db.SQLDB()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	return project, result
}

func skus(rows []Product) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.SKU)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type Role struct {
	ID        string `orm:"pk"`
	Code      string `orm:"unique"`
	Name      string
	CreatedAt time.Time `orm:"created_at"`
	UpdatedAt time.Time `orm:"updated_at"`
}

type Product struct {
	ID         string `orm:"pk"`
	CompanyID  string `orm:"company"`
	SKU        string `orm:"unique"`
	Name       string
	PriceCents int64
	DeletedAt  *time.Time `orm:"soft_delete"`
	CreatedAt  time.Time  `orm:"created_at"`
	CreatedBy  string     `orm:"created_by"`
	UpdatedAt  time.Time  `orm:"updated_at"`
	UpdatedBy  string     `orm:"updated_by"`
	DeletedBy  string     `orm:"deleted_by"`
}
