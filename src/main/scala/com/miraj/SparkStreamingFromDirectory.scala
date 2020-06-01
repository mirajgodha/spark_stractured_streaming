package com.miraj

import com.codahale.metrics.{Counter, Histogram, MetricRegistry}
import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.MySource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._


object SparkStreamingFromDirectory {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    val source: MySource = new MySource
//    spark.sparkContext.setLogLevel("ERROR")

    val chartListener = new StreamingQueryListener() {
      val MaxDataPoints = 100
      // a mutable reference to an immutable container to buffer n data points

      def onQueryStarted(event: QueryStartedEvent) = ()

      def onQueryTerminated(event: QueryTerminatedEvent) = {
      }

      def onQueryProgress(event: QueryProgressEvent) = {
        val queryProgress = event.progress
        // ignore zero-valued events
        println("----------------" + event.progress)
        println("----------------" + event.progress.numInputRows)
          if (queryProgress.numInputRows > 0) {
//          source.FOO_TIMER.update(queryProgress.timestamp.toLong)
          source.FOO_COUNTER.inc( event.progress.numInputRows)
        }
      }
    }

    spark.streams.addListener(chartListener)
    SparkEnv.get.metricsSystem.registerSource(source)

    val schema = StructType(
      List(
        StructField("RecordNumber", IntegerType, true),
        StructField("Zipcode", StringType, true),
        StructField("ZipCodeType", StringType, true),
        StructField("City", StringType, true),
        StructField("State", StringType, true),
        StructField("LocationType", StringType, true),
        StructField("Lat", StringType, true),
        StructField("Long", StringType, true),
        StructField("Xaxis", StringType, true),
        StructField("Yaxis", StringType, true),
        StructField("Zaxis", StringType, true),
        StructField("WorldRegion", StringType, true),
        StructField("Country", StringType, true),
        StructField("LocationText", StringType, true),
        StructField("Location", StringType, true),
        StructField("Decommisioned", StringType, true)
      )
    )

    val df = spark.readStream
      .schema(schema)
      .json("hdfs://siqhdp01/tmp/miraj/input/")

    val df1 = df.withColumn("timestamp", lit(current_timestamp()))
    df1.printSchema()

    import spark.implicits._

    val groupDF = df1.withWatermark("timestamp", "1 minutes").select("Zipcode", "timestamp")
      .groupBy(window($"timestamp", "1 minutes", "1 minutes"), $"Zipcode").count()
    groupDF.printSchema()

    var a = groupDF.writeStream.trigger(Trigger.Once).outputMode("append").format("json")
      .option("checkpointLocation", "hdfs://siqhdp01/tmp/miraj/checkpoint")
      .start("hdfs://siqhdp01/tmp/miraj/output")


    println(a.status)
    println("status: " + a.recentProgress)
    a.awaitTermination()
    spark.stop()

    /* groupDF.writeStream
       .format("console")
       .outputMode("append")
       .option("truncate",false).option("checkpointLocation", "hdfs://siqhdp01/tmp/miraj/checkpoint")
       .option("newRows",30)
       .start()
       .awaitTermination() */
  }
}




