package coordinator

import (
	"bytes"
	"runtime"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/tsdb"
)

type ShardInfo struct {
	*tsdb.Shard
	Measurements []string
	StartTime    time.Time
}

type ShardMapper []*ShardInfo

func (a ShardMapper) FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	for _, sh := range a {
		f, d, err := sh.FieldDimensions(sh.Measurements)
		if err != nil {
			return nil, nil, err
		}

		for k, typ := range f {
			if _, ok := fields[k]; typ != influxql.Unknown && (!ok || typ < fields[k]) {
				fields[k] = typ
			}
		}
		for k := range d {
			dimensions[k] = struct{}{}
		}
	}
	return
}

type SeriesInfo struct {
	*tsdb.Shard
	Measurement *tsdb.Measurement
	TagSet      *influxql.TagSet
	StartTime   time.Time
}

func (si *SeriesInfo) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	return si.Shard.CreateSeriesIterator(si.Measurement, si.TagSet, opt)
}

type SeriesList []*SeriesInfo

func (a SeriesList) Len() int { return len(a) }
func (a SeriesList) Less(i, j int) bool {
	if cmp := bytes.Compare(a[i].TagSet.Key, a[j].TagSet.Key); cmp < 0 {
		return true
	} else if cmp > 0 {
		return false
	}
	return a[i].StartTime.Before(a[j].StartTime)
}
func (a SeriesList) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a ShardMapper) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	var seriesList SeriesList
	var mu sync.Mutex
	parallelism := runtime.GOMAXPROCS(0)
	ch := make(chan struct{}, parallelism)
	for i := 0; i < parallelism; i++ {
		ch <- struct{}{}
	}

	var wg sync.WaitGroup
	for _, si := range a {
		wg.Add(1)
		<-ch
		go func(si *ShardInfo) {
			defer wg.Done()
			defer func() {
				ch <- struct{}{}
			}()

			for _, name := range si.Measurements {
				series, err := createTagSets(si, name, &opt)
				if err != nil || len(series) == 0 {
					return
				}

				mu.Lock()
				seriesList = append(seriesList, series...)
				mu.Unlock()
			}
		}(si)
	}
	wg.Wait()

	ics := make([]influxql.IteratorCreator, len(seriesList))
	for i, series := range seriesList {
		ics[i] = series
	}
	return influxql.NewLazyIterator(ics, opt)
}

func createTagSets(si *ShardInfo, name string, opt *influxql.IteratorOptions) ([]*SeriesInfo, error) {
	mm := si.Shard.MeasurementByName(name)
	if mm == nil {
		return nil, nil
	}

	// Determine tagsets for this measurement based on dimensions and filters.
	tagSets, err := mm.TagSets(opt.Dimensions, opt.Condition)
	if err != nil {
		return nil, err
	}

	// Calculate tag sets and apply SLIMIT/SOFFSET.
	tagSets = influxql.LimitTagSets(tagSets, opt.SLimit, opt.SOffset)

	series := make([]*SeriesInfo, 0, len(tagSets))
	for _, t := range tagSets {
		series = append(series, &SeriesInfo{
			Shard:       si.Shard,
			Measurement: mm,
			TagSet:      t,
			StartTime:   si.StartTime,
		})
	}
	return series, nil
}
