package quality

import (
	"context"
	"fmt"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
)

// Source is implemented by a Snowflake watermark reader in live mode.
// Demo mode never calls this; the catalog JSON supplies checks.
type Source interface {
	LatestChecks(ctx context.Context, since time.Time) ([]domain.Check, error)
}

// SnowflakeSQL is the query the live worker will run with a warehouse role.
// Grants needed: catalog of quality sources plus SELECT on each data product
// validation and dbt test-result table.
func SnowflakeSQL(database, schema, table, tsCol string, since time.Time) string {
	return fmt.Sprintf(
		"SELECT * FROM %s.%s.%s WHERE %s > '%s' ORDER BY %s ASC LIMIT 5000",
		database, schema, table, tsCol, since.UTC().Format(time.RFC3339), tsCol,
	)
}
