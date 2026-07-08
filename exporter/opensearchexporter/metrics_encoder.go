// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Metric kind names shared by the SS4O and Data Prepper OTel v1 metric schemas.
const (
	metricKindGauge                = "GAUGE"
	metricKindSum                  = "SUM"
	metricKindHistogram            = "HISTOGRAM"
	metricKindExponentialHistogram = "EXPONENTIAL_HISTOGRAM"
	metricKindSummary              = "SUMMARY"
)

func (m *encodeModel) encodeMetric(
	resource pcommon.Resource,
	scope pcommon.InstrumentationScope,
	schemaURL string,
	metric pmetric.Metric,
	dp metricDataPoint,
) ([]byte, error) {
	if m.otelV1 {
		return m.encodeMetricOTelV1(resource, scope, schemaURL, metric, dp)
	}
	if m.sso {
		return m.encodeMetricSSO(resource, scope, schemaURL, metric, dp)
	}

	return nil, errMetricsMappingModeUnsupported
}

// encodeMetricSSO encodes a metric data point following the Simple Schema for Observability.
// See: https://github.com/opensearch-project/opensearch-catalog/tree/main/schema/observability/metrics
func (m *encodeModel) encodeMetricSSO(
	resource pcommon.Resource,
	scope pcommon.InstrumentationScope,
	schemaURL string,
	metric pmetric.Metric,
	dp metricDataPoint,
) ([]byte, error) {
	sso := ssoMetric{}
	if err := populateMetricDocBase(&sso.metricDocBase, metric, dp); err != nil {
		return nil, err
	}

	now := time.Now()
	sso.ObservedTimestamp = &now

	sso.Resource = attributesToMapString(resource.Attributes())
	sso.SchemaURL = schemaURL
	if serviceName, ok := resource.Attributes().Get("service.name"); ok {
		sso.ServiceName = serviceName.AsString()
	}

	ds := dataStream{}
	if m.dataset != "" {
		ds.Dataset = m.dataset
	}

	if m.namespace != "" {
		ds.Namespace = m.namespace
	}

	if ds != (dataStream{}) {
		ds.Type = "metric"
		sso.Attributes["data_stream"] = ds
	}

	sso.InstrumentationScope.Name = scope.Name()
	sso.InstrumentationScope.Version = scope.Version()
	sso.InstrumentationScope.SchemaURL = schemaURL
	sso.InstrumentationScope.Attributes = scope.Attributes().AsRaw()

	return json.Marshal(sso)
}

// encodeMetricOTelV1 encodes a metric data point following the Data Prepper OTel v1 metrics schema.
func (*encodeModel) encodeMetricOTelV1(
	resource pcommon.Resource,
	scope pcommon.InstrumentationScope,
	schemaURL string,
	metric pmetric.Metric,
	dp metricDataPoint,
) ([]byte, error) {
	doc := otelV1Metric{}
	if err := populateMetricDocBase(&doc.metricDocBase, metric, dp); err != nil {
		return nil, err
	}

	doc.Time = doc.Timestamp
	doc.Flags = int64(dp.Flags())

	// Data Prepper emits gauge and sum values as a plain double `value` field.
	switch {
	case doc.ValueDouble != nil:
		doc.Value = doc.ValueDouble
	case doc.ValueInt != nil:
		v := float64(*doc.ValueInt)
		doc.Value = &v
	}

	doc.Resource = otelV1Resource{
		Attributes:             resource.Attributes().AsRaw(),
		DroppedAttributesCount: resource.DroppedAttributesCount(),
		SchemaURL:              schemaURL,
	}
	doc.InstrumentationScope = otelV1Scope{
		Name:                   scope.Name(),
		Version:                scope.Version(),
		SchemaURL:              schemaURL,
		Attributes:             scope.Attributes().AsRaw(),
		DroppedAttributesCount: scope.DroppedAttributesCount(),
	}
	if serviceName, ok := resource.Attributes().Get("service.name"); ok {
		doc.ServiceName = serviceName.AsString()
	}

	return json.Marshal(doc)
}

// populateMetricDocBase fills the schema-independent fields of a metric
// document from a metric and one of its data points.
func populateMetricDocBase(doc *metricDocBase, metric pmetric.Metric, dp metricDataPoint) error {
	doc.Name = metric.Name()
	doc.Description = metric.Description()
	doc.Unit = metric.Unit()
	doc.StartTime = dp.StartTimestamp().AsTime()
	doc.Timestamp = dp.Timestamp().AsTime()
	doc.Attributes = dp.Attributes().AsRaw()

	switch dp := dp.(type) {
	case pmetric.NumberDataPoint:
		switch metric.Type() {
		case pmetric.MetricTypeGauge:
			doc.Kind = metricKindGauge
		case pmetric.MetricTypeSum:
			doc.Kind = metricKindSum
			sum := metric.Sum()
			doc.AggregationTemporality = temporalityString(sum.AggregationTemporality())
			monotonic := sum.IsMonotonic()
			doc.Monotonic = &monotonic
		default:
			return fmt.Errorf("unexpected metric type %q for a number data point", metric.Type())
		}
		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			value := dp.IntValue()
			doc.ValueInt = &value
		case pmetric.NumberDataPointValueTypeDouble:
			value := dp.DoubleValue()
			doc.ValueDouble = &value
		}
		doc.Exemplars = makeExemplars(dp.Exemplars())

	case pmetric.HistogramDataPoint:
		doc.Kind = metricKindHistogram
		doc.AggregationTemporality = temporalityString(metric.Histogram().AggregationTemporality())
		count := dp.Count()
		doc.Count = &count
		if dp.HasSum() {
			sum := dp.Sum()
			doc.Sum = &sum
		}
		if dp.HasMin() {
			minValue := dp.Min()
			doc.Min = &minValue
		}
		if dp.HasMax() {
			maxValue := dp.Max()
			doc.Max = &maxValue
		}
		counts := dp.BucketCounts().AsRaw()
		bounds := dp.ExplicitBounds().AsRaw()
		doc.BucketCountsList = counts
		doc.ExplicitBoundsList = bounds
		bucketCount := len(counts)
		doc.BucketCount = &bucketCount
		explicitBoundsCount := len(bounds)
		doc.ExplicitBoundsCount = &explicitBoundsCount
		doc.Buckets = makeExplicitBuckets(counts, bounds)
		doc.Exemplars = makeExemplars(dp.Exemplars())

	case pmetric.ExponentialHistogramDataPoint:
		doc.Kind = metricKindExponentialHistogram
		doc.AggregationTemporality = temporalityString(metric.ExponentialHistogram().AggregationTemporality())
		count := dp.Count()
		doc.Count = &count
		if dp.HasSum() {
			sum := dp.Sum()
			doc.Sum = &sum
		}
		if dp.HasMin() {
			minValue := dp.Min()
			doc.Min = &minValue
		}
		if dp.HasMax() {
			maxValue := dp.Max()
			doc.Max = &maxValue
		}
		scale := dp.Scale()
		doc.Scale = &scale
		zeroCount := dp.ZeroCount()
		doc.ZeroCount = &zeroCount
		positiveOffset := dp.Positive().Offset()
		doc.PositiveOffset = &positiveOffset
		negativeOffset := dp.Negative().Offset()
		doc.NegativeOffset = &negativeOffset
		doc.PositiveBuckets = makeExponentialBuckets(dp.Scale(), dp.Positive(), false)
		doc.NegativeBuckets = makeExponentialBuckets(dp.Scale(), dp.Negative(), true)
		doc.Exemplars = makeExemplars(dp.Exemplars())

	case pmetric.SummaryDataPoint:
		doc.Kind = metricKindSummary
		count := dp.Count()
		doc.Count = &count
		sum := dp.Sum()
		doc.Sum = &sum
		quantiles := dp.QuantileValues()
		if quantiles.Len() > 0 {
			doc.Quantiles = make([]metricQuantile, quantiles.Len())
			for i := 0; i < quantiles.Len(); i++ {
				doc.Quantiles[i] = metricQuantile{
					Quantile: quantiles.At(i).Quantile(),
					Value:    quantiles.At(i).Value(),
				}
			}
		}
		quantileValuesCount := quantiles.Len()
		doc.QuantileValuesCount = &quantileValuesCount

	default:
		return fmt.Errorf("unsupported data point type %T", dp)
	}

	return nil
}

// temporalityString converts an aggregation temporality to the
// protobuf-style enum name emitted by Data Prepper.
func temporalityString(temporality pmetric.AggregationTemporality) string {
	switch temporality {
	case pmetric.AggregationTemporalityDelta:
		return "AGGREGATION_TEMPORALITY_DELTA"
	case pmetric.AggregationTemporalityCumulative:
		return "AGGREGATION_TEMPORALITY_CUMULATIVE"
	default:
		return "AGGREGATION_TEMPORALITY_UNSPECIFIED"
	}
}

// makeExplicitBuckets materializes the buckets of an explicit-bounds histogram
// data point. Bucket i covers (bounds[i-1], bounds[i]]; the first and last
// buckets are unbounded and are capped at ±math.MaxFloat64 so the boundaries
// remain representable in JSON.
func makeExplicitBuckets(counts []uint64, bounds []float64) []metricBucket {
	if len(counts) == 0 {
		return nil
	}
	buckets := make([]metricBucket, len(counts))
	for i, count := range counts {
		bucket := metricBucket{Count: count, Min: -math.MaxFloat64, Max: math.MaxFloat64}
		if i > 0 && i-1 < len(bounds) {
			bucket.Min = bounds[i-1]
		}
		if i < len(bounds) {
			bucket.Max = bounds[i]
		}
		buckets[i] = bucket
	}
	return buckets
}

// makeExponentialBuckets materializes the buckets of an exponential histogram
// data point. Positive bucket i covers (base^(offset+i), base^(offset+i+1)]
// with base = 2^(2^-scale); negative buckets mirror the positive ones around
// zero.
func makeExponentialBuckets(scale int32, dpBuckets pmetric.ExponentialHistogramDataPointBuckets, negative bool) []metricBucket {
	counts := dpBuckets.BucketCounts().AsRaw()
	if len(counts) == 0 {
		return nil
	}
	base := math.Pow(2, math.Pow(2, float64(-scale)))
	offset := dpBuckets.Offset()
	buckets := make([]metricBucket, len(counts))
	for i, count := range counts {
		lower := math.Pow(base, float64(offset)+float64(i))
		upper := math.Pow(base, float64(offset)+float64(i)+1)
		if negative {
			lower, upper = -upper, -lower
		}
		buckets[i] = metricBucket{Count: count, Min: lower, Max: upper}
	}
	return buckets
}

// makeExemplars converts pmetric exemplars to their document representation.
func makeExemplars(exemplars pmetric.ExemplarSlice) []metricExemplar {
	if exemplars.Len() == 0 {
		return nil
	}
	out := make([]metricExemplar, exemplars.Len())
	for i := 0; i < exemplars.Len(); i++ {
		e := exemplars.At(i)
		exemplar := metricExemplar{
			Time:       e.Timestamp().AsTime(),
			Attributes: e.FilteredAttributes().AsRaw(),
		}
		switch e.ValueType() {
		case pmetric.ExemplarValueTypeInt:
			value := float64(e.IntValue())
			exemplar.Value = &value
		case pmetric.ExemplarValueTypeDouble:
			value := e.DoubleValue()
			exemplar.Value = &value
		}
		if !e.TraceID().IsEmpty() {
			exemplar.TraceID = e.TraceID().String()
		}
		if !e.SpanID().IsEmpty() {
			exemplar.SpanID = e.SpanID().String()
		}
		out[i] = exemplar
	}
	return out
}
