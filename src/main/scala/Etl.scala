package com.hipages.hitest


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.Properties


object Etl {

  var jsonSchemaResourcePath: String = Properties.envOrElse("SCHEMA_RESOURCE", "source_data_schema.json")
  var rawJsonFilePath: String = Properties.envOrElse("SOURCE_PATH", "src/main/resources/source_event_data.json")
  var targetDirectoryPath: String = Properties.envOrElse("TARGET_DIR", "output")

  def main(args: Array[String]): Unit = {

    // Provide Spark
    implicit val spark = SparkSession.builder
      .appName("HiPagesEtl")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // Load data
    val rawDf = loadJson

    // Transform data
    val userActivityDf = createUserEventDf(rawDf)
    val hourlyActivityDf = createHourlyActivityDf(userActivityDf)

    // Store output
    userActivityDf.write.format("csv").mode("overwrite").save(s"$targetDirectoryPath/user_activity.csv")
    hourlyActivityDf.write.format("csv").mode("overwrite").save(s"$targetDirectoryPath/hourly_activity.csv")

    // Stop Spark
    spark.stop
  }

  def loadJson(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val jsonLines = spark.sparkContext.textFile(rawJsonFilePath)
    val jsonDf = spark.read.json(jsonLines)
      .withColumn("user_id", $"user.id")
      .withColumn("user_session_id", $"user.session_id")
      .withColumn("user_ip", $"user.ip")
    jsonDf
  }

  def createUserEventDf(rawDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val extractUrlPartsUdf = udf(extractUrlParts(_))
    rawDf
      .withColumn("url_split", extractUrlPartsUdf(col("url")))
      .withColumn("url_level1", col("url_split").getItem(0))
      .withColumn("url_level2", col("url_split").getItem(1))
      .withColumn("url_level3", col("url_split").getItem(2))
      .drop(col("url_split"))
      .drop(col("url"))
      .withColumnRenamed("action", "activity")
      .withColumnRenamed("timestamp", "time_stamp")
      .select("user_id", "time_stamp", "url_level1", "url_level2", "url_level3", "activity")
  }

  def createHourlyActivityDf(userActivityDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    userActivityDf
      .groupBy(
        from_unixtime(unix_timestamp(col("time_stamp"), "dd/MM/yyyy HH:mm:ss"), "yyyyMMddHH").as("time_bucket"),
        col("url_level1"),
        col("activity")
      )
      .agg(
        count("activity").as("activity_count"),
        countDistinct("user_id").as("user_count")
      )
  }

  def extractUrlParts(url: String): Seq[String] = {
    val pattern = 
      """([^\/]+:\/\/|)
        |([a-zA-Z]*\.[a-zA-Z.]*)[\/]*
        |([a-zA-z0-9%]*)[\/]*
        |([a-zA-z0-9%]*)[A-Za-z0-9-._~:/?#\[\]@!$&'()*+,;=]*
        |""".stripMargin.replaceAll("\n","").r
    url match {
        case pattern(protocol, domain, first, second) => Seq(domain, first, second)
        case _ => Seq("", "", "")
    }
  }
}
