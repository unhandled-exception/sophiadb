package metadata

import (
	"fmt"
	"sort"
	"strings"

	"github.com/axiomhq/hyperloglog"
	"github.com/unhandled-exception/sophiadb/pkg/records"
)

type StatInfo struct {
	Blocks  int64
	Records int64

	distinctValues map[string]*hyperloglog.Sketch
}

func NewStatInfo(schema records.Schema) StatInfo {
	si := StatInfo{
		distinctValues: make(map[string]*hyperloglog.Sketch, schema.Count()),
	}

	for _, f := range schema.Fields() {
		si.distinctValues[f] = hyperloglog.New()
	}

	return si
}

func (si StatInfo) String() string {
	dv := make([]string, len(si.distinctValues))

	i := 0

	for k, v := range si.distinctValues {
		dv[i] = fmt.Sprintf("%s: %d", k, v.Estimate())
		i++
	}

	sort.Strings(dv)

	return fmt.Sprintf(
		"blocks: %d, records: %d, distinct values: [%s]",
		si.Blocks, si.Records, strings.Join(dv, ", "),
	)
}

func (si StatInfo) DistinctValues(fieldName string) (int64, bool) {
	dc, ok := si.distinctValues[fieldName]

	if !ok {
		return 0, false
	}

	return int64(dc.Estimate()), true
}

func (si *StatInfo) UpdateDistincValues(fieldName string, value []byte) {
	dc, ok := si.distinctValues[fieldName]
	if ok {
		dc.Insert(value)
	}
}
