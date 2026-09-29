package quality

import (
	"context"
	"fmt"

	"github.com/inorbit/inorbit/internal/domain"
)

// PipelineRows loads MARTS.PIPELINE_STATUS (or the configured table). Missing objects are skipped.
func (s *Snowflake) PipelineRows(ctx context.Context, table domain.QualityTable) ([]map[string]any, error) {
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
	if s.knownMissing(rel) {
		return nil, nil
	}
	rows, err := s.query(ctx, "select * from "+rel)
	if err != nil {
		if isMissingObject(err) {
			s.markMissing(rel)
			return nil, nil
		}
		return nil, err
	}
	return rows, nil
}
