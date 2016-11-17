package io.spinor.sparkdemo.data

import java.io.File
import java.net.URL

import io.spinor.sparkdemo.util.DemoUtil

/**
  * Utility classs to download the MNIST data set.
  *
  * @author A. Mahmood (arshadm@spinor.io)
  */
class MNISTData extends DemoUtil {
  /** The training image dataset url. */
  val trainingImagesUrl = "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz"

  /** The training label dataset url. */
  val trainingLabelsUrl = "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz"

  /** The test images url. */
  val testImagesUrl = "http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz"

  /** The test labels url. */
  val testLabelsUrl = "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz"

  /** The location of the training images file. */
  val trainingImagesFile = new File("/tmp/train-images-idx3-ubyte.gz")

  /** The location of the training labels file. */
  val trainingLabelsFile = new File("/tmp/train-labels-idx1-ubyte.gz")

  /** The location of the test images file. */
  val testImagesFile = new File("/tmp/t10k-images-idx3-ubyte.gz")

  /** The location of the test labels file. */
  val testLabelsFile = new File("/tmp/t10k-labels-idx1-ubyte.gz")

  /**
    * Load the sample data.
    */
  def loadSampleData(): Unit = {
    super.downloadFile(new URL(trainingImagesUrl), trainingImagesFile)
    super.downloadFile(new URL(trainingLabelsUrl), trainingLabelsFile)
    super.downloadFile(new URL(testImagesUrl), testImagesFile)
    super.downloadFile(new URL(testLabelsUrl), testLabelsFile)
  }
}
