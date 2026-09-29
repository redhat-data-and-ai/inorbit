package quality

import (
	"context"
	"fmt"

	"github.com/inorbit/inorbit/internal/domain"
)

// LineageRows loads MARTS.DP_LINEAGE (or the configured table). Missing objects are skipped.
func (s *Snowflake) LineageRows(ctx context.Context, table domain.QualityTable) ([]map[string]any, error) {
	if !s.ready() {
		return nil, fmt.Errorf("snowflake client is nil")
	}
	if !table.Enabled {
		return nil, nil
	}
	rel, err := qualified(table)
	if err != nil {
		return nil, err
	}
	q := `
select
  data_product_id,
  data_product_name,
  status,
  health_score,
  upstream_sources,
  upstream_count,
  downstream_consumers,
  direct_downstream_count,
  direct_dp_consumer_count,
  service_account_count,
  consumer_group_count,
  blast_radius_count,
  blast_radius_score,
  computed_at
from ` + rel
	rows, err := s.query(ctx, q)
	if err != nil {
		if isMissingObject(err) {
			return nil, nil
		}
		return nil, err
	}
	return rows, nil
}
