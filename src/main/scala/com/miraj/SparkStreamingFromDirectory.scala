package com.miraj

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._



object SparkStreamingFromDirectory {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

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
      .groupBy( window($"timestamp", "1 minutes", "1 minutes"),$"Zipcode").count()
    groupDF.printSchema()

    groupDF.writeStream.trigger(Trigger.Once).outputMode("append").format("json")
      .option("checkpointLocation", "hdfs://siqhdp01/tmp/miraj/checkpoint")
      .start("hdfs://siqhdp01/tmp/miraj/output")

    println("Active streams: " + spark.streams.active.size)
    spark.streams.awaitAnyTermination()
    println("Active streams after waitng:" + spark.streams.active.size)
   /* groupDF.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate",false).option("checkpointLocation", "hdfs://siqhdp01/tmp/miraj/checkpoint")
      .option("newRows",30)
      .start()
      .awaitTermination() */
  }
}
