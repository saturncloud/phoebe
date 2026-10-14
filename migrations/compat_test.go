package migrations

import (
	"io/fs"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The phoebe chart runs migrate as a pre-upgrade hook, so the PREVIOUS
// release's drainer writes into the NEW schema until the new pods roll. A
// contract step (a NOT NULL column without a default, a new CHECK, a type
// change, a dropped or renamed billing_event column, a dropped billing_event
// unique key) makes those writes fail with a 23xxx/42804/22P02 error, which the
// drainer treats as a bad row and drops. These tests enforce the README's
// "Upgrade order and backward compatibility" rule (expand now, contract one
// release later) on every migration after the grandfathered ones.

// grandfathered lists the migrations that existed when this rule was enforced.
// They shipped under coordinated cutovers (see the per-migration rollout
// sections of README.md) and are not re-checked.
var grandfathered = map[uint]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true, 9: true}

// compatOptOut marks a migration whose contract step was reviewed on purpose,
// e.g. the contract half of an expand/contract pair one release after the
// expand: "-- compat: reviewed <reason>". The reason is required.
var compatOptOut = regexp.MustCompile(`(?im)^\s*--\s*compat:[ \t]*reviewed[ \t]+\S`)

var (
	lineComment = regexp.MustCompile(`--[^\n]*`)
	spaces      = regexp.MustCompile(`\s+`)
	alterTable  = regexp.MustCompile(`^ALTER TABLE (?:IF EXISTS )?(?:ONLY )?(\S+) (.*)$`)
	addCheck    = regexp.MustCompile(`^ADD (?:CONSTRAINT \S+ )?CHECK\b`)
	addColumn   = regexp.MustCompile(`^ADD (?:COLUMN )?(?:IF NOT EXISTS )?(\S+)`)
	inlineCheck = regexp.MustCompile(`\bCHECK ?\(`)
	notNull     = regexp.MustCompile(`\bNOT NULL\b`)
	hasDefault  = regexp.MustCompile(`\bDEFAULT\b`)
	typeChange  = regexp.MustCompile(`^ALTER (?:COLUMN )?\S+ (?:SET DATA )?TYPE\b`)
	setNotNull  = regexp.MustCompile(`^ALTER (?:COLUMN )?\S+ SET NOT NULL\b`)
	dropColumn  = regexp.MustCompile(`^DROP (?:COLUMN )?(?:IF EXISTS )?(\S+)`)
	dropConstr  = regexp.MustCompile(`^DROP CONSTRAINT (?:IF EXISTS )?(\S+)`)
	dropIndex   = regexp.MustCompile(`^DROP INDEX (?:CONCURRENTLY )?(?:IF EXISTS )?(.+)$`)
	uniqueIndex = regexp.MustCompile(`CREATE UNIQUE INDEX (?:CONCURRENTLY )?(?:IF NOT EXISTS )?(\S+) ON (?:ONLY )?(\S+)`)
	uniqueConst = regexp.MustCompile(`CONSTRAINT (\S+) (?:PRIMARY KEY|UNIQUE)\b`)
)

// notColumnStart are the words after ADD that start a table constraint, not a
// column definition.
var notColumnStart = map[string]bool{"CONSTRAINT": true, "PRIMARY": true, "UNIQUE": true, "CHECK": true, "FOREIGN": true, "EXCLUDE": true}

// normalize strips -- comments, collapses whitespace and upper-cases the SQL.
func normalize(sql string) string {
	return strings.ToUpper(spaces.ReplaceAllString(lineComment.ReplaceAllString(sql, " "), " "))
}

// ident drops a schema prefix and identifier quotes: public."x" -> X.
func ident(s string) string {
	s = strings.Trim(s, `"`)
	if i := strings.LastIndex(s, "."); i >= 0 {
		s = s[i+1:]
	}
	return strings.Trim(s, `"`)
}

// splitTopLevel splits on commas outside parentheses.
func splitTopLevel(s string) []string {
	var out []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	return append(out, strings.TrimSpace(s[start:]))
}

// billingEventUniqueKeys returns the names of billing_event's primary key and
// unique constraints/indexes declared anywhere in the given migrations. The
// drainer's ON CONFLICT (request_id) depends on them.
func billingEventUniqueKeys(files map[string]string) map[string]bool {
	keys := map[string]bool{}
	for _, sql := range files {
		for _, stmt := range strings.Split(normalize(sql), ";") {
			stmt = strings.TrimSpace(stmt)
			if m := uniqueIndex.FindStringSubmatch(stmt); m != nil && ident(m[2]) == "BILLING_EVENT" {
				keys[ident(m[1])] = true
			}
			isBillingEvent := strings.HasPrefix(stmt, "CREATE TABLE") && strings.Contains(stmt, "BILLING_EVENT (")
			if m := alterTable.FindStringSubmatch(stmt); m != nil && ident(m[1]) == "BILLING_EVENT" {
				isBillingEvent = true
			}
			if isBillingEvent {
				for _, m := range uniqueConst.FindAllStringSubmatch(stmt, -1) {
					keys[ident(m[1])] = true
				}
			}
		}
	}
	return keys
}

// compatViolations returns every contract step in sql that would make the
// previous release's drainer (or rater) fail its writes during the
// pre-upgrade window.
func compatViolations(sql string, uniqueKeys map[string]bool) []string {
	var out []string
	for _, stmt := range strings.Split(normalize(sql), ";") {
		stmt = strings.TrimSpace(stmt)
		if m := dropIndex.FindStringSubmatch(stmt); m != nil {
			for _, name := range splitTopLevel(strings.TrimSuffix(strings.TrimSuffix(m[1], " CASCADE"), " RESTRICT")) {
				if uniqueKeys[ident(name)] {
					out = append(out, "drops billing_event's unique key "+ident(name)+": "+stmt)
				}
			}
			continue
		}
		m := alterTable.FindStringSubmatch(stmt)
		if m == nil {
			continue
		}
		table := ident(m[1])
		for _, clause := range splitTopLevel(m[2]) {
			switch {
			case addCheck.MatchString(clause):
				out = append(out, "adds a CHECK constraint: "+clause)
			case strings.HasPrefix(clause, "ADD "):
				c := addColumn.FindStringSubmatch(clause)
				if c == nil || notColumnStart[c[1]] {
					continue
				}
				if notNull.MatchString(clause) && !hasDefault.MatchString(clause) {
					out = append(out, "adds a NOT NULL column without a DEFAULT: "+clause)
				}
				if inlineCheck.MatchString(clause) {
					out = append(out, "adds a column with an inline CHECK: "+clause)
				}
			case typeChange.MatchString(clause):
				out = append(out, "changes a column type: "+clause)
			case setNotNull.MatchString(clause):
				out = append(out, "sets an existing column NOT NULL: "+clause)
			case table == "BILLING_EVENT" && strings.HasPrefix(clause, "RENAME"):
				out = append(out, "renames on billing_event: "+clause)
			case table == "BILLING_EVENT" && dropConstr.MatchString(clause):
				if name := ident(dropConstr.FindStringSubmatch(clause)[1]); uniqueKeys[name] {
					out = append(out, "drops billing_event's unique key "+name+": "+clause)
				}
			case table == "BILLING_EVENT" && dropColumn.MatchString(clause):
				out = append(out, "drops a billing_event column: "+clause)
			}
		}
	}
	return out
}

func readUpMigrations(t *testing.T) map[string]string {
	t.Helper()
	files := map[string]string{}
	entries, err := fs.ReadDir(FS, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".up.sql") {
			continue
		}
		b, err := fs.ReadFile(FS, e.Name())
		if err != nil {
			t.Fatal(err)
		}
		files[e.Name()] = string(b)
	}
	return files
}

func TestMigrations_ExpandOnlyForThePreviousRelease(t *testing.T) {
	files := readUpMigrations(t)
	keys := billingEventUniqueKeys(files)
	for name, sql := range files {
		prefix, _, _ := strings.Cut(name, "_")
		v, err := strconv.ParseUint(prefix, 10, 32)
		if err != nil {
			t.Errorf("%s: no numeric version prefix", name)
			continue
		}
		if grandfathered[uint(v)] || compatOptOut.MatchString(sql) {
			continue
		}
		for _, violation := range compatViolations(sql, keys) {
			t.Errorf("%s %s. The migrate Job runs before the new pods roll, so the previous release's drainer "+
				"writes into this schema and would poison-drop rows during the pre-upgrade window. Split it into "+
				"expand now / contract one release later (migrations/README.md, \"Upgrade order and backward "+
				"compatibility\"), or mark a deliberate contract step with `-- compat: reviewed <reason>`.",
				name, violation)
		}
	}
}

func TestCompatViolations_FlagsEachContractStep(t *testing.T) {
	keys := map[string]bool{"PK_BILLING_EVENT": true, "BILLING_EVENT_REQ_UQ": true}
	for _, c := range []struct {
		sql  string
		flag bool
	}{
		// allowed expand steps
		{"ALTER TABLE billing_event ADD COLUMN region text;", false},
		{"ALTER TABLE billing_event ADD COLUMN IF NOT EXISTS region text NOT NULL DEFAULT '';", false},
		{"ALTER TABLE billing_event ADD COLUMN a int, ADD COLUMN b text NOT NULL DEFAULT 'x';", false},
		{"CREATE INDEX billing_event_x_ix ON billing_event (region);", false},
		{"DROP INDEX billing_event_created_at_ix;", false},
		{"ALTER TABLE rated_usage DROP COLUMN legacy;", false},
		{"ALTER TABLE billing_event ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES t (a);", false},
		{"-- ALTER TABLE billing_event DROP COLUMN region;\nSELECT 1;", false},
		// (1) NOT NULL without DEFAULT
		{"ALTER TABLE billing_event ADD COLUMN region text NOT NULL;", true},
		{"alter table public.billing_event add region text not null;", true},
		{"ALTER TABLE billing_event ADD COLUMN a int DEFAULT 0, ADD COLUMN b text NOT NULL;", true},
		// (2) CHECK
		{"ALTER TABLE rated_usage ADD CONSTRAINT x_ck CHECK (cost >= 0);", true},
		{"ALTER TABLE billing_event ADD CHECK (prompt_tokens >= 0);", true},
		{"ALTER TABLE billing_event ADD COLUMN n int DEFAULT 0 CHECK (n >= 0);", true},
		// (3) type change
		{"ALTER TABLE billing_event ALTER COLUMN prompt_tokens TYPE bigint;", true},
		{"ALTER TABLE billing_event ALTER prompt_tokens SET DATA TYPE bigint;", true},
		// (4) SET NOT NULL
		{"ALTER TABLE rated_usage ALTER COLUMN region SET NOT NULL;", true},
		// (5) drop / rename on billing_event
		{"ALTER TABLE billing_event DROP COLUMN region;", true},
		{"ALTER TABLE billing_event DROP region;", true},
		{"ALTER TABLE billing_event RENAME COLUMN region TO zone;", true},
		// (6) the unique key
		{"ALTER TABLE billing_event DROP CONSTRAINT pk_billing_event;", true},
		{"DROP INDEX IF EXISTS billing_event_req_uq;", true},
	} {
		got := compatViolations(c.sql, keys)
		if (len(got) > 0) != c.flag {
			t.Errorf("compatViolations(%q) = %v, want flagged=%v", c.sql, got, c.flag)
		}
	}
}

func TestCompatOptOut_RequiresAReason(t *testing.T) {
	if !compatOptOut.MatchString("-- compat: reviewed contract step for 0010's expand\nALTER TABLE x DROP COLUMN y;") {
		t.Error("a marker with a reason must opt out")
	}
	if compatOptOut.MatchString("-- compat: reviewed\nALTER TABLE x DROP COLUMN y;") {
		t.Error("a marker without a reason must not opt out")
	}
}

// The unique-key list is derived from the migrations, so it must at least
// contain billing_event's primary key as 0001 declares it.
func TestBillingEventUniqueKeys_IncludesPrimaryKey(t *testing.T) {
	keys := billingEventUniqueKeys(readUpMigrations(t))
	if !keys["PK_BILLING_EVENT"] {
		t.Fatalf("unique keys = %v, want PK_BILLING_EVENT", keys)
	}
}

// The checker is not vacuous: the grandfathered cutover migrations that
// tightened the schema are exactly what it flags.
func TestCompatViolations_FlagsTheGrandfatheredContractSteps(t *testing.T) {
	files := readUpMigrations(t)
	keys := billingEventUniqueKeys(files)
	for _, name := range []string{
		"0005_invoice_grade_attempts.up.sql",
		"0007_serving_mode_explicit.up.sql",
		"0009_rollup_grain_graph_revision.up.sql",
	} {
		if len(compatViolations(files[name], keys)) == 0 {
			t.Errorf("%s: no violation found, want its contract steps flagged", name)
		}
	}
}
