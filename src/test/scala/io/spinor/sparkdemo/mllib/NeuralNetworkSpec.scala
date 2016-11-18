package io.spinor.sparkdemo.mllib

import io.spinor.sparkdemo.data.MNISTData
import io.spinor.sparkdemo.util.DemoUtil
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

/**
  * Tests for the Spark based neural network.
  *
  * @author A. Mahmood (arshadm@spinor.io)
  */
class NeuralNetworkSpec extends FlatSpec with DemoUtil with Matchers {
  val logger = LoggerFactory.getLogger(classOf[NeuralNetworkSpec])

  "Training on MNIST data" should " run" in {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("NeuralNetworkDemo")
    sparkConf.setMaster("local[2]")

    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._

    val mNISTData = new MNISTData()
    val trainingData = mNISTData.getTrainingData()
    val trainingPoints = sparkContext.parallelize(trainingData.map(entry => LabeledPoint(entry._2, Vectors.dense(entry._1)))).toDF()

    val classifier = new MultilayerPerceptronClassifier()
    classifier
      .setLayers(Array(100, 100))
      .setBlockSize(125)
      .setSeed(1234L)
      .setMaxIter(2)

    val model = classifier.fit(trainingPoints)

    val testData = mNISTData.getTestData()
    val testPoints = sparkContext.parallelize(testData.map(entry => LabeledPoint(entry._2, Vectors.dense(entry._1)))).toDF()
    val result = model.transform(testPoints)
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")

    logger.info("accuracy:" + evaluator.evaluate(predictionAndLabels))
  }
}
