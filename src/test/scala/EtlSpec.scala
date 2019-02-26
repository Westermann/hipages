package com.hipages.hitest.test


import com.hipages.hitest.Etl
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._


class EtlSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  val etl = Etl 
  import etl._
  implicit lazy val spark = SparkSession.builder
    .appName("HiPagesEtlTest")
    .config("spark.master", "local[*]")
    .getOrCreate()

  "The Etl object extractURLParts function" should "correctly extract url parts" in {
    Etl.extractUrlParts("https://www.hipages.com.au/first/second") shouldEqual Seq("www.hipages.com.au", "first", "second")
  }

  it should "correctly extract only one url part" in {
    Etl.extractUrlParts("https://www.hipages.com.au/") shouldEqual Seq("www.hipages.com.au", "", "")
  }

  it should "correctly extract from url without protocoll" in {
    Etl.extractUrlParts("hipages.com.au/") shouldEqual Seq("hipages.com.au", "", "")
  }
 
  it should "correctly extract no url from single word" in {
    Etl.extractUrlParts("invalid") shouldEqual Seq("", "", "")
  }
 
  it should "correctly extract from url with query" in {
    Etl.extractUrlParts("hipages.com/?asdf=5&h=6") shouldEqual Seq("hipages.com", "", "")
  }

  "The Etl object" should "load the Json data correctly" in {
    Etl.loadJson
  }

  it should "correctly generate a user event table" in {
    val df = Etl.loadJson
    Etl.createUserEventTable(df)
  }

}
