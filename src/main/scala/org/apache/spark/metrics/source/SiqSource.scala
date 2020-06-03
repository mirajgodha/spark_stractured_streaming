package org.apache.spark.metrics.source

import com.codahale.metrics.{Counter, Histogram,Meter,Timer ,MetricRegistry}

class SiqSource extends Source {

  override val sourceName: String = "SiqSource"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  val NUM_INPUT_ROWS_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("numInputRows"))
  val INPUT_ROWS_PER_SECOND_METER: Meter = metricRegistry.meter(MetricRegistry.name("inputRowsPerSecond"))
  val PROCESSED_ROWS_PER_SECOND_METER: Meter = metricRegistry.meter(MetricRegistry.name("processedRowsPerSecond"))

  //Duration ms counters
  val DURATION_MS_ADD_BATCH_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("DurationMsAddBatch"))
  val DURATION_MS_GET_BATCH_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("DurationMGgetBatch"))
  val DURATION_MS_GET_OFFSET_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("DurationMsSetOffset"))
  val DURATION_MS_QUERY_PLANNING_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("DurationMsQueryPlanning"))
  val DURATION_MS_TRIGGER_EXECUTION_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("DurationMsTriggerExecution"))
  val DURATION_MS_WAL_COMMIT_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("DurationMsWalCommit"))

  //Event time timers
  val EVENT_TIME_AVG_TIMER: Timer = metricRegistry.timer("eventTimeAvg")
  var EVENT_TIME_MAX_TIMER = metricRegistry.timer("eventTimeMax")
  var EVENT_TIME_MIN_TIMER = metricRegistry.timer("eventTimeMin")
  var EVENT_TIME_WATERMARK_TIMER = metricRegistry.timer("eventTimeWatermark")

  var STATE_OPERATORS_NUM_ROWS_TOTAL_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("StateOperatorsNumRowsTotal"))
  var STATE_OPERATORS_NUM_ROWS_UPDATED_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("StateOperatorsNumRowsUpdated"))
  var STATE_OPERATORS_MEMORY_USED_BYTES_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("StateOperatorsMemoryUsedBytes"))

}