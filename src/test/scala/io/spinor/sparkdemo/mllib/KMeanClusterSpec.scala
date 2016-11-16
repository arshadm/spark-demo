package io.spinor.sparkdemo.mllib

import io.spinor.sparkdemo.data.DigitalBreathTestData
import io.spinor.sparkdemo.util.DemoUtil
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

/**
  * A demo of the K-Mean clustering algorithm.
  *
  * @author A. Mahmood (arshadm@spinor.io)
  */
class KMeanClusterSpec extends FlatSpec with DemoUtil with Matchers {
  /** The number of clusters. */
  final val NUM_CLUSTERS = 3

  /** The maximum number of iterations. */
  final val MAX_ITERATIONS = 50

  /** The initialisation mode. */
  final val INITIALISATION_MODE = KMeans.K_MEANS_PARALLEL

  /** The number of runs. */
  final val NUM_RUNS = 1

  /** The epsilon value for comparison against doubles. */
  final val EPSILON = 1e-4

  /** The logger. */
  final val logger = LoggerFactory.getLogger(classOf[KMeanClusterSpec])

  /** The digital breathtest data. */
  final val digitalBreathTestData = new DigitalBreathTestData()

  "Running K-Means on DigitalBreathTestData2013" should "generate clusters" in {
    val csvDataFile = digitalBreathTestData.loadData()
    val labeledDataFile = digitalBreathTestData.labelData(csvDataFile)

    val sparkConf = new SparkConf()
    sparkConf.setAppName("NaiveBayesDemo")
    sparkConf.setMaster("local[2]")

    val sparkContext = new SparkContext(sparkConf)
    val fileRDD = sparkContext.textFile(labeledDataFile.getAbsolutePath)
    val labelledRDD = fileRDD.map(csvLine => Vectors.dense(csvLine.split(",").map(_.toDouble)))
    labelledRDD.cache()

    val kmeans = new KMeans()
    kmeans.setK(NUM_CLUSTERS)
    kmeans.setMaxIterations(MAX_ITERATIONS)
    kmeans.setInitializationMode(INITIALISATION_MODE)
    kmeans.setRuns(NUM_RUNS)
    kmeans.setEpsilon(EPSILON)

    val kmeansModel = kmeans.run(labelledRDD)

    logger.info(s"Input rows: ${labelledRDD.count()}")
    logger.info(s"K-Means cost: ${kmeansModel.computeCost(labelledRDD)}")

    kmeansModel.clusterCenters.foreach(center => logger.info(center.toString))

    val prediction = kmeansModel.predict(labelledRDD)
    prediction.countByValue().foreach(p => logger.info(p.toString()))

    csvDataFile.delete()
    labeledDataFile.delete()
  }
}
