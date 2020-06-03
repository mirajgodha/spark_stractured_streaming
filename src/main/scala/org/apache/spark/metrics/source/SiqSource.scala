package org.apache.spark.metrics.source

import com.codahale.metrics.{Counter, Histogram,Meter,Timer ,MetricRegistry}

class SiqSource extends Source {

  override val sourceName: String = "SiqSource"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  val NUM_INPUT_ROWS_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("num.Input.Rows"))
  val INPUT_ROWS_PER_SECOND_METER: Meter = metricRegistry.meter(MetricRegistry.name("input.Rows.PerSecond"))
  val PROCESSED_ROWS_PER_SECOND_METER: Meter = metricRegistry.meter(MetricRegistry.name("processed.Rows.PerSecond"))

  //Duration ms counters
  val DURATION_MS_ADD_BATCH_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("Duration.Ms.Add.Batch"))
  val DURATION_MS_GET_BATCH_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("Duration.Ms.Get.Batch"))
  val DURATION_MS_GET_OFFSET_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("Duration.Ms.Set.Offset"))
  val DURATION_MS_QUERY_PLANNING_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("Duration.Ms.Query.Planning"))
  val DURATION_MS_TRIGGER_EXECUTION_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("Duration.Ms.Trigger.Execution"))
  val DURATION_MS_WAL_COMMIT_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("Duration.Ms.Wal.Commit"))

  //Event time timers
  val EVENT_TIME_AVG_TIMER: Timer = metricRegistry.timer("event.Time.Avg")
  var EVENT_TIME_MAX_TIMER = metricRegistry.timer("event.Time.Max")
  var EVENT_TIME_MIN_TIMER = metricRegistry.timer("event.Time.Min")
  var EVENT_TIME_WATERMARK_TIMER = metricRegistry.timer("event.Time.Watermark")

  var STATE_OPERATORS_NUM_ROWS_TOTAL_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("State.Operators.Num.Rows.Total"))
  var STATE_OPERATORS_NUM_ROWS_UPDATED_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("State.Operators.Num.Rows.Updated"))
  var STATE_OPERATORS_MEMORY_USED_BYTES_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("State.Operators.Memory.Used.Bytes"))

}