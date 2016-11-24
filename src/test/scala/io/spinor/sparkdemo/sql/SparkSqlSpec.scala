package io.spinor.sparkdemo.sql

import io.spinor.sparkdemo.util.DemoUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

/**
  * Tests for processing various types of files via SparkSQL.
  *
  * @author A. Mahmood (arshadm@spinor.io)
  */
class SparkSqlSpec extends FlatSpec with DemoUtil with Matchers {
  /** The logger. */
  final val logger = LoggerFactory.getLogger(classOf[SparkSqlSpec])

  "Loading JSON file via SparkSQL" should "be possible" in {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("NaiveBayesDemo")
    sparkConf.setMaster("local[2]")

    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._

    val dframe = sqlContext.read.json("src/test/resources/sql/test1.json")
    dframe.columns eq(Array("gloassary"))
  }
}
