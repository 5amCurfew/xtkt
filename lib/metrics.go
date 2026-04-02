package lib

import (
	"sync"
	"time"
)

// TransformationMetrics tracks record transformation statistics.
type TransformationMetrics struct {
	Processed        uint64 `json:"processed"`
	TransformFailed  uint64 `json:"transform_failed"`
	FilteredBookmark uint64 `json:"filtered_bookmark"`
	mu               sync.Mutex
}

var TransformMetrics = &TransformationMetrics{}

type NotEmittedMetric struct {
	Total                  uint64 `json:"total"`
	FilteredBookmark       uint64 `json:"filtered_bookmark"`
	SchemaValidationFailed uint64 `json:"schema_validation_failed"`
	TransformFailed        uint64 `json:"transform_failed"`
}

type ExecutionMetric struct {
	ExecutionStart    time.Time     `json:"execution_start,omitempty"`
	ExecutionEnd      time.Time     `json:"execution_end,omitempty"`
	ExecutionDuration time.Duration `json:"execution_duration,omitempty"`

	Emitted uint64 `json:"emitted"`

	Processed          uint64           `json:"processed"`
	ProcessedPerSecond float64          `json:"processed_per_second"`
	NotEmitted         NotEmittedMetric `json:"not_emitted"`
}

func NewExecutionMetric() ExecutionMetric {
	return ExecutionMetric{ExecutionStart: time.Now().UTC()}
}

func (execution *ExecutionMetric) addTransformMetrics() {
	execution.Processed = TransformMetrics.Processed
	execution.NotEmitted.FilteredBookmark = TransformMetrics.FilteredBookmark
	execution.NotEmitted.TransformFailed = TransformMetrics.TransformFailed
	execution.NotEmitted.Total = execution.NotEmitted.FilteredBookmark + execution.NotEmitted.SchemaValidationFailed + execution.NotEmitted.TransformFailed
}

func (execution *ExecutionMetric) Complete() {
	execution.ExecutionEnd = time.Now().UTC()
	execution.ExecutionDuration = execution.ExecutionEnd.Sub(execution.ExecutionStart)

	execution.addTransformMetrics()

	if execution.ExecutionDuration.Seconds() > 0 {
		execution.ProcessedPerSecond = float64(execution.Processed) / execution.ExecutionDuration.Seconds()
	}
}
