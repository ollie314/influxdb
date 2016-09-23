package coordinator

import (
	"sort"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/tsdb"
)

type ShardGroup struct {
	Shards    []*tsdb.Shard
	StartTime time.Time
}

type ShardGroupMapping map[string][]*ShardGroup

func (a ShardGroupMapping) WalkShards(fn func(sh *tsdb.Shard) error) error {
	for _, shardGroups := range a {
		for _, sg := range shardGroups {
			for _, sh := range sg.Shards {
				if err := fn(sh); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (a ShardGroupMapping) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	m := make(map[string]influxql.Source)

	if err := a.WalkShards(func(sh *tsdb.Shard) error {
		expanded, err := sh.ExpandSources(sources)
		if err != nil {
			return err
		}

		for _, src := range expanded {
			switch src := src.(type) {
			case *influxql.Measurement:
				m[src.String()] = src
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)

	sorted := make([]influxql.Source, len(names))
	for i, name := range names {
		sorted[i] = m[name]
	}
	return sorted, nil
}

func (a ShardGroupMapping) FieldDimensions(sources influxql.Sources) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	if err := a.WalkShards(func(sh *tsdb.Shard) error {
		f, d, err := sh.FieldDimensions(sources)
		if err != nil {
			return err
		}

		for k, typ := range f {
			if _, ok := fields[k]; typ != influxql.Unknown && (!ok || typ < fields[k]) {
				fields[k] = typ
			}
		}
		for k := range d {
			dimensions[k] = struct{}{}
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return
}

func (a ShardGroupMapping) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	var ics []influxql.IteratorCreator
	for _, shardGroups := range a {
		for _, sg := range shardGroups {
			var ic influxql.IteratorCreator
			if len(sg.Shards) > 1 {
				group := make([]influxql.IteratorCreator, len(sg.Shards))
				for i, sh := range sg.Shards {
					group[i] = sh
				}
				ic = influxql.IteratorCreators(group)
			} else if len(sg.Shards) == 1 {
				ic = sg.Shards[0]
			}

			if ic == nil {
				continue
			}
			ics = append(ics, ic)
		}
	}
	return influxql.NewLazyIterator(ics, opt)
}
