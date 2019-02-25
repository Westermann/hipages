package com.hipages.hitest


import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.Properties
import scala.util.parsing.json.{JSON, JSONObject}


object Etl {

  var jsonSchemaResourcePath: String = Properties.envOrElse("SCHEMA_RESOURCE", "source_data_schema.json")
  var rawJsonFilePath: String = Properties.envOrElse("SOURCE_PATH", "src/main/resources/source_event_data.json")
  var targetDirectoryPath: String = Properties.envOrElse("TARGET_DIR", "output")

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession.builder
      .appName("HiPagesEtl")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val df = loadJson
    println(df.show())

    spark.stop
  }

  def loadJson(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val jsonLines = spark.sparkContext.textFile(rawJsonFilePath)
    jsonLines.foreach(println)
    spark.read.json(jsonLines)
  }
}
