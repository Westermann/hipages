package com.hipages.hitest.test


import com.hipages.hitest.Etl
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._


class EtlSpec extends FlatSpec with BeforeAndAfterAll {

  val etl = Etl 
  import etl._
  implicit lazy val spark = SparkSession.builder
    .appName("HiPagesEtlTest")
    .config("spark.master", "local[*]")
    .getOrCreate()

  "The Etl object" should "load the Json data correctly" in {
    Etl.loadJson
  }

  override def afterAll {
    spark.stop
  }
}
