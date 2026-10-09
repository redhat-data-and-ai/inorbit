package quality

import (
	"fmt"
	"strings"
)

// Column candidates are public names used by validation tables and by
// Elementary. Whichever of them exists is projected. Query text and expected
// or actual values are never selected. Missing columns are skipped, so a
// warehouse without a dimension column still loads.

type colCand struct {
	alias string
	names []string
}

func validationLatestSQL(rel string, have map[string]bool) (string, bool) {
	cols := projectCols("", have, []colCand{
		{"RUN_ID", []string{"RUN_ID"}},
		{"RUN_TIME", []string{"RUN_TIME", "EXECUTED_AT", "CREATED_AT"}},
		{"STATUS", []string{"STATUS"}},
		{"NAME", []string{"NAME", "CHECK_NAME"}},
		{"DESCRIPTION", []string{"DESCRIPTION", "CHECK_DESCRIPTION"}},
		{"SEVERITY", []string{"SEVERITY"}},
		{"TAGS", []string{"TAGS"}},
		{"DIMENSION", []string{"DIMENSION"}},
		{"ELEMENT", []string{"ELEMENT"}},
		{"IS_CDE", []string{"IS_CDE"}},
		{"TABLE_NAME", []string{"TABLE_NAME"}},
	})
	if len(cols) == 0 || !have["STATUS"] {
		return "", false
	}
	pred, ok := latestKeyPredicate("", have, []string{"RUN_ID"}, []string{"RUN_TIME", "EXECUTED_AT", "CREATED_AT"})
	if !ok {
		return "", false
	}
	return fmt.Sprintf("select %s from %s qualify %s", strings.Join(cols, ", "), rel, pred), true
}

// elementaryLatestSQL reads the latest Elementary run. A dimension is taken
// from the results table when that column exists. Otherwise it is taken from
// Elementary's dbt_tests catalog in the same database and schema
// (dimension / quality_dimension column, or the same keys inside meta).
// A blank dimension still falls back to the test name.
func elementaryLatestSQL(rel string, have map[string]bool, testsRel string, testsHave map[string]bool) (string, bool) {
	cols := projectCols("e", have, []colCand{
		{"INVOCATION_ID", []string{"INVOCATION_ID"}},
		{"TEST_EXECUTION_ID", []string{"TEST_EXECUTION_ID"}},
		{"TEST_UNIQUE_ID", []string{"TEST_UNIQUE_ID", "UNIQUE_ID"}},
		{"TEST_NAME", []string{"TEST_NAME", "TEST_SHORT_NAME", "TEST_ALIAS", "NAME"}},
		{"STATUS", []string{"STATUS"}},
		{"DETECTED_AT", []string{"DETECTED_AT", "GENERATED_AT", "CREATED_AT"}},
		{"SEVERITY", []string{"SEVERITY"}},
		{"TAGS", []string{"TAGS"}},
		{"IS_CDE", []string{"IS_CDE"}},
		{"TEST_COLUMN_NAME", []string{"TEST_COLUMN_NAME", "COLUMN_NAME"}},
		{"TABLE_NAME", []string{"TABLE_NAME", "MODEL_NAME"}},
	})
	if len(cols) == 0 || !have["STATUS"] {
		return "", false
	}
	join := ""
	if catalog := testsCatalogJoin(testsRel, testsHave, have); catalog != "" {
		if have["DIMENSION"] || have["QUALITY_DIMENSION"] {
			resultDim := "e.dimension"
			if !have["DIMENSION"] {
				resultDim = "e.quality_dimension"
			}
			cols = append(cols, fmt.Sprintf("coalesce(%s, t.dimension) as dimension", resultDim))
		} else {
			cols = append(cols, "t.dimension as dimension")
		}
		join = catalog
	} else {
		cols = append(cols, projectCols("e", have, []colCand{
			{"DIMENSION", []string{"DIMENSION", "QUALITY_DIMENSION"}},
		})...)
	}
	pred, ok := latestKeyPredicate("e", have, []string{"INVOCATION_ID", "TEST_EXECUTION_ID"}, []string{"DETECTED_AT", "GENERATED_AT", "CREATED_AT"})
	if !ok {
		return "", false
	}
	return fmt.Sprintf("select %s from %s e%s qualify %s", strings.Join(cols, ", "), rel, join, pred), true
}

func testsCatalogJoin(testsRel string, testsHave, resultsHave map[string]bool) string {
	dimExpr, ok := catalogDimensionExpr(testsHave)
	if testsRel == "" || !ok || !testsHave["UNIQUE_ID"] || !resultsHave["TEST_UNIQUE_ID"] {
		return ""
	}
	src := fmt.Sprintf("(select unique_id, %s from %s)", dimExpr, testsRel)
	if testsHave["GENERATED_AT"] {
		src = fmt.Sprintf("(select unique_id, %s from %s qualify row_number() over (partition by unique_id order by generated_at desc nulls last) = 1)", dimExpr, testsRel)
	}
	return fmt.Sprintf(" left join %s t on t.unique_id = e.test_unique_id", src)
}

// catalogDimensionExpr builds one dimension value from optional public columns.
// Elementary's dbt_tests model ships meta; dimension and quality_dimension are
// used only when a deployment has added them.
func catalogDimensionExpr(have map[string]bool) (string, bool) {
	var parts []string
	if have["DIMENSION"] {
		parts = append(parts, "nullif(trim(dimension), '')")
	}
	if have["QUALITY_DIMENSION"] {
		parts = append(parts, "nullif(trim(quality_dimension), '')")
	}
	if have["META"] {
		parts = append(parts,
			"nullif(trim(try_parse_json(to_varchar(meta)):dimension::varchar), '')",
			"nullif(trim(try_parse_json(to_varchar(meta)):quality_dimension::varchar), '')",
		)
	}
	if len(parts) == 0 {
		return "", false
	}
	expr := parts[0]
	if len(parts) > 1 {
		expr = "coalesce(" + strings.Join(parts, ", ") + ")"
	}
	return expr + " as dimension", true
}

// testsCatalogRelation is Elementary's dbt_tests model beside the results table.
func testsCatalogRelation(rel string) string {
	parts := strings.Split(rel, ".")
	if len(parts) != 3 || parts[2] == "DBT_TESTS" {
		return ""
	}
	return parts[0] + "." + parts[1] + ".DBT_TESTS"
}

func projectCols(alias string, have map[string]bool, cands []colCand) []string {
	var out []string
	for _, c := range cands {
		for _, name := range c.names {
			if !have[name] {
				continue
			}
			src := name
			if alias != "" {
				src = alias + "." + name
			}
			out = append(out, src+" as "+c.alias)
			break
		}
	}
	return out
}

// latestKeyPredicate keeps every row from the newest run. The key is the first
// candidate column that exists (run id, else invocation id). first_value
// ignore nulls skips a newest row whose id is null. The time column only
// orders that choice.
func latestKeyPredicate(alias string, have map[string]bool, keys, times []string) (string, bool) {
	var present []string
	for _, k := range keys {
		if have[k] {
			present = append(present, qualify(alias, k))
		}
	}
	timeCol := firstHave(have, times)
	if timeCol == "" {
		return "", false
	}
	timeCol = qualify(alias, timeCol)
	if len(present) == 0 {
		return fmt.Sprintf("%s = max(%s) over ()", timeCol, timeCol), true
	}
	key := present[0]
	if len(present) > 1 {
		key = "coalesce(" + strings.Join(present, ", ") + ")"
	}
	return fmt.Sprintf("%s = first_value(%s) ignore nulls over (order by %s desc nulls last)", key, key, timeCol), true
}

func qualify(alias, col string) string {
	if alias == "" {
		return col
	}
	return alias + "." + col
}

func firstHave(have map[string]bool, names []string) string {
	for _, n := range names {
		if have[n] {
			return n
		}
	}
	return ""
}
