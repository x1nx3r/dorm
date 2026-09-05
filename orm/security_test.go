package orm

import (
	"testing"

	"github.com/dionisius77/dorm/dialect"
	"github.com/dionisius77/dorm/schema"
)

type placeholderDialect struct{}

func (placeholderDialect) Name() string { return "placeholder" }
func (placeholderDialect) QuoteIdent(s string) string {
	return "[" + s + "]"
}
func (placeholderDialect) Placeholder(i int) string {
	return "@" + itoa(i)
}
func (placeholderDialect) Capabilities() dialect.Capabilities               { return dialect.Capabilities{} }
func (placeholderDialect) ColumnDefinition(*schema.Column) (string, error)  { return "", nil }
func (placeholderDialect) RenderOperation(schema.Operation) (string, error) { return "", nil }
func (placeholderDialect) RenderMigration(*schema.Diff) ([]string, error)   { return nil, nil }
func (placeholderDialect) RenderSelect(table string, columns []string, where []string, orderBy []string, limit, offset *int) (string, error) {
	return "", nil
}
func (placeholderDialect) RenderInsert(table string, columns []string, returning []string) (string, error) {
	return "", nil
}
func (placeholderDialect) RenderUpdate(table string, set []string, where []string, returning []string) (string, error) {
	return "", nil
}
func (placeholderDialect) RenderDelete(table string, where []string, returning []string) (string, error) {
	return "", nil
}

func TestBuildWhereClausesUsesDialectQuotingAndPlaceholders(t *testing.T) {
	cols := []string{"company_id", "user_id"}
	where, args := buildWhereClauses(cols, []any{"company-1", "user-1"}, 0, []predicate{
		{expr: "name = ? AND deleted_at IS NULL", args: []any{"alice"}},
	}, placeholderDialect{})
	if len(where) != 3 {
		t.Fatalf("expected 3 clauses, got %d", len(where))
	}
	if where[0] != "[company_id] = @1" || where[1] != "[user_id] = @2" {
		t.Fatalf("expected dialect quoting and placeholders, got %#v", where[:2])
	}
	if where[2] != "name = @3 AND deleted_at IS NULL" {
		t.Fatalf("expected placeholder rebinding, got %q", where[2])
	}
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(args))
	}
}

func TestUpdateSetClausesUsesDialectQuoting(t *testing.T) {
	table := &schema.Table{
		Columns: []*schema.Column{
			{Name: "id", PrimaryKey: true},
			{Name: "company_id"},
			{Name: "name"},
		},
	}
	set, args := updateSetClauses(table, map[string]any{
		"company_id": "company-1",
		"name":       "alice",
	}, []string{"id"}, placeholderDialect{})
	if len(set) != 2 {
		t.Fatalf("expected 2 set clauses, got %d", len(set))
	}
	if set[0] != "[company_id] = @1" || set[1] != "[name] = @2" {
		t.Fatalf("expected dialect quoting, got %#v", set)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}
}

func TestUpsertSetClausesUsesDialectQuoting(t *testing.T) {
	table := &schema.Table{
		Columns: []*schema.Column{
			{Name: "id", PrimaryKey: true},
			{Name: "name"},
			{Name: "company_id"},
		},
	}
	set := upsertSetClauses(table, map[string]any{
		"name":       "alice",
		"company_id": "company-1",
	}, []string{"id"}, placeholderDialect{})
	if len(set) != 2 {
		t.Fatalf("expected 2 set clauses, got %d", len(set))
	}
	if set[0] != "[name] = EXCLUDED.[name]" || set[1] != "[company_id] = EXCLUDED.[company_id]" {
		t.Fatalf("expected dialect quoting, got %#v", set)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	return string(b[n:])
}
