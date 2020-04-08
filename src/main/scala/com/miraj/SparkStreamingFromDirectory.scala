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
      .json("file:///data/spark_stractured_streaming/src/main/scala/resources/")

    val df1 = df.withColumn("timestamp", lit(current_timestamp()))
    df1.printSchema()

    import spark.implicits._

    val groupDF = df1.withWatermark("timestamp", "1 minutes").select("Zipcode", "timestamp")
      .groupBy( window($"timestamp", "1 minutes", "1 minutes"),$"Zipcode").count()
    groupDF.printSchema()

    groupDF.writeStream.trigger(Trigger.Once).outputMode("append").format("json").option("truncate",false)
      .option("checkpointLocation", "/tmp/miraj")
      .start("file:///data/output")

    spark.streams.awaitAnyTermination()
   /* groupDF.writeStream.trigger(Trigger.Once)
      .format("console")
      .outputMode("complete")
      .option("truncate",false).option("checkpointLocation", "/tmp/miraj")
      .option("newRows",30)
      .start()
      .awaitTermination() */
  }
}
