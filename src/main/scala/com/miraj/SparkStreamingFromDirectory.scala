package com.miraj

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{Counter, Histogram, MetricRegistry}
import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.SiqSource
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

    val source: SiqSource = new SiqSource
//    spark.sparkContext.setLogLevel("ERROR")

    /**
     * Stractured streaming stats
     *
     * Total number of input rows: {
     * "id" : "211ecf53-e5b3-474f-baba-63182283c4d5",
     * "runId" : "6dd39643-0388-4fc6-8df3-d08864b94b37",
     * "name" : null,
     * "timestamp" : "2020-05-12T17:47:00.615Z",
     * "batchId" : 13,
     * "numInputRows" : 5,
     * "processedRowsPerSecond" : 0.3009328919650918,
     * "durationMs" : {
     * "addBatch" : 14776,
     * "getBatch" : 54,
     * "getOffset" : 555,
     * "queryPlanning" : 912,
     * "triggerExecution" : 16614,
     * "walCommit" : 64
     * },
     * "eventTime" : {
     * "avg" : "2020-02-23T09:15:00.000Z",
     * "max" : "2020-02-23T09:15:00.000Z",
     * "min" : "2020-02-23T09:15:00.000Z",
     * "watermark" : "2020-02-23T08:00:00.000Z"
     * },
     * "stateOperators" : [ {
     * "numRowsTotal" : 12,
     * "numRowsUpdated" : 5,
     * "memoryUsedBytes" : 111287,
     * "customMetrics" : {
     * "loadedMapCacheHitCount" : 200,
     * "loadedMapCacheMissCount" : 200,
     * "stateOnCurrentVersionSizeBytes" : 30823
     * }
     * } ],
     * "sources" : [ {
     * "description" : "FileStreamSource[hdfs://mrxcdh001-mst-01.cloud.in.guavus.com/data/SSOuput]",
     * "startOffset" : {
     * "logOffset" : 12
     * },
     * "endOffset" : {
     * "logOffset" : 13
     * },
     * "numInputRows" : 5,
     * "processedRowsPerSecond" : 0.3009328919650918
     * } ],
     * "sink" : {
     * "description" : "FileSink[hdfs://mrxcdh001-mst-01.cloud.in.guavus.com/khyati/output_hourly/]",
     * "numOutputRows" : -1
     * }
     * }
     */
    val siqListener = new StreamingQueryListener() {
      val MaxDataPoints = 5000
      // a mutable reference to an immutable container to buffer n data points

      def onQueryStarted(event: QueryStartedEvent) = ()

      def onQueryTerminated(event: QueryTerminatedEvent) = ()

      def onQueryProgress(event: QueryProgressEvent) = {
        val queryProgress = event.progress
        // ignore zero-valued events
        if (queryProgress.numInputRows > 0) {

          source.NUM_INPUT_ROWS_COUNTER.inc(queryProgress.numInputRows)

          try {
            source.INPUT_ROWS_PER_SECOND_METER.mark(queryProgress.inputRowsPerSecond.toLong)
          }catch {
            case e: Exception => println("Cannot send custom spark structured streaming metrics - INPUT_ROWS_PER_SECOND_METER")
              println(e.getMessage)
          }

          try {
            source.PROCESSED_ROWS_PER_SECOND_METER.mark(queryProgress.processedRowsPerSecond.toLong)
          }catch {
            case e: Exception => println("Cannot send custom spark structured streaming metrics - PROCESSED_ROWS_PER_SECOND_METER")
              println(e.getMessage)
          }

          try {
            source.DURATION_MS_ADD_BATCH_COUNTER.inc(queryProgress.durationMs.get("addBatch"))
            source.DURATION_MS_GET_BATCH_COUNTER.inc(queryProgress.durationMs.get("getBatch"))
            source.DURATION_MS_GET_OFFSET_COUNTER.inc(queryProgress.durationMs.get("getOffset"))
            source.DURATION_MS_QUERY_PLANNING_COUNTER.inc(queryProgress.durationMs.get("queryPlanning"))
            source.DURATION_MS_TRIGGER_EXECUTION_COUNTER.inc(queryProgress.durationMs.get("triggerExecution"))
            source.DURATION_MS_WAL_COMMIT_COUNTER.inc(queryProgress.durationMs.get("walCommit"))
          }catch {
            case e: Exception => println("Cannot send custom spark structured streaming metrics - DURATION_MS")
              println(e.getMessage)
          }


          try {
            val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'")
            source.EVENT_TIME_AVG_TIMER.update(format.parse(queryProgress.eventTime.get("avg")).getTime, TimeUnit.MILLISECONDS)
            source.EVENT_TIME_MAX_TIMER.update(format.parse(queryProgress.eventTime.get("max")).getTime, TimeUnit.MILLISECONDS)
            source.EVENT_TIME_MIN_TIMER.update(format.parse(queryProgress.eventTime.get("min")).getTime, TimeUnit.MILLISECONDS)
            source.EVENT_TIME_WATERMARK_TIMER.update(format.parse(queryProgress.eventTime.get("watermark")).getTime, TimeUnit.MILLISECONDS)
          }catch {
            case e: Exception => println("Cannot send custom spark structured streaming metrics - EVENT_TIME")
              println(e.getMessage)
          }

          try {
            source.STATE_OPERATORS_NUM_ROWS_TOTAL_COUNTER.inc(queryProgress.stateOperators(0).numRowsTotal)
            source.STATE_OPERATORS_NUM_ROWS_UPDATED_COUNTER.inc(queryProgress.stateOperators(0).numRowsUpdated)
            source.STATE_OPERATORS_MEMORY_USED_BYTES_COUNTER.inc(queryProgress.stateOperators(0).memoryUsedBytes)
          }catch {
            case e: Exception => println("Cannot send custom spark structured streaming metrics - STATE_OPERATORS")
              println(e.getMessage)
          }

        }
      }
    }

    spark.streams.addListener(siqListener)
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
      .json("hdfs://siq02/tmp/miraj/input/")

    val df1 = df.withColumn("timestamp", lit(current_timestamp()))
    df1.printSchema()

    import spark.implicits._

    val groupDF = df1.withWatermark("timestamp", "1 minutes").select("Zipcode", "timestamp")
      .groupBy(window($"timestamp", "1 minutes", "1 minutes"), $"Zipcode").count()
    groupDF.printSchema()

    var a = groupDF.writeStream
//.trigger(Trigger.Once)
.outputMode("append").format("json")
      .option("checkpointLocation", "hdfs://siq02/tmp/miraj/checkpoint")
      .start("hdfs://siq02/tmp/miraj/output")


    println(a.status)
    println("status: " + a.recentProgress)
    a.awaitTermination()
//    spark.stop()

    /* groupDF.writeStream
       .format("console")
       .outputMode("append")
       .option("truncate",false).option("checkpointLocation", "hdfs://siqhdp01/tmp/miraj/checkpoint")
       .option("newRows",30)
       .start()
       .awaitTermination() */
  }
}




