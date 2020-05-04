package org.apache.spark.metrics.source

import com.codahale.metrics.{Counter, Histogram,Meter,Timer ,MetricRegistry}

class MySource extends Source {
  override val sourceName: String = "MySource"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  val FOO: Histogram = metricRegistry.histogram(MetricRegistry.name("fooHistory"))
  val FOO_COUNTER: Meter = metricRegistry.meter(MetricRegistry.name("numInputRows"))
  val FOO_TIMER: Timer = metricRegistry.timer(MetricRegistry.name("numInputRowsTimer"))
}