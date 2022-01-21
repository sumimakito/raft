package raft

import (
	"time"
)

const (
	MetricGoroutines = "goroutines"
)

type MetricsExporter interface {
	Record(time time.Time, name string, value interface{})
}

type metricAggregator interface {
	Metric() string
	Aggregate() map[string]interface{}
}

type timeMetricAggregator struct {
	metric string
	minMax StreamMinMaxInt64
	avg    StreamAverage
}

func newTimeMetricAggregator(metric string) *timeMetricAggregator {
	a := &timeMetricAggregator{
		metric: metric,
	}
	return a
}
