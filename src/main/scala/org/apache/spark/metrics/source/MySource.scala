package org.apache.spark.metrics.source

import com.codahale.metrics.{Counter, Histogram, MetricRegistry}

class MySource extends Source {
  override val sourceName: String = "MySource"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  val FOO: Histogram = metricRegistry.histogram(MetricRegistry.name("fooHistory"))
  val FOO_COUNTER: Counter = metricRegistry.counter(MetricRegistry.name("fooCounter"))
}