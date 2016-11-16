package io.spinor.sparkdemo.mllib

import io.spinor.sparkdemo.data.DigitalBreathTestData
import io.spinor.sparkdemo.util.DemoUtil
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

/**
  * A demo of the naive bayes classification algorithm.
  *
  * @author A. Mahmood (arshadm@spinor.io)
  */
class NaiveBayesSpec extends FlatSpec with DemoUtil with Matchers {
  /** The logger. */
  final val logger = LoggerFactory.getLogger(classOf[NaiveBayesSpec])

  /** The digital breathtest data. */
  final val digitalBreathTestData = new DigitalBreathTestData()

  "Running naive bayes on DigitalBreathTestData2013" should "classify" in {
    val csvDataFile = digitalBreathTestData.loadData()
    val labeledDataFile = digitalBreathTestData.labelData(csvDataFile)

    val sparkConf = new SparkConf()
    sparkConf.setAppName("NaiveBayesDemo")
    sparkConf.setMaster("local[2]")

    val sparkContext = new SparkContext(sparkConf)
    val fileRDD = sparkContext.textFile(labeledDataFile.getAbsolutePath)
    val labelledRDD = fileRDD.map({ csvLine =>
      val colData = csvLine.split(',')

      LabeledPoint(colData(0).toDouble,
        Vectors.dense(colData.slice(1, colData.length).map(_.toDouble)))
    })

    val splittedData = labelledRDD.randomSplit(Array(0.7, 0.3), 13)
    val trainDataset = splittedData(0)
    val testDataset = splittedData(1)

    val trained = NaiveBayes.train(trainDataset)
    val prediction = trained.predict(testDataset.map(_.features))
    val predictionWithLabels = prediction.zip(testDataset.map(_.label))
    val accuracy = 100.0 * predictionWithLabels.filter(x => x._1 == x._2).count() / testDataset.count()

    logger.info(s"** Accuracy: $accuracy%")

    csvDataFile.delete()
    labeledDataFile.delete()
  }
}
