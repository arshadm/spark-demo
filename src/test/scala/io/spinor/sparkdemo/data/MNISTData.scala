package io.spinor.sparkdemo.data

import java.io.{File, FileInputStream, InputStream}
import java.net.URL
import java.nio.ByteBuffer
import java.util.zip.GZIPInputStream

import com.google.common.base.Preconditions.checkArgument
import io.spinor.sparkdemo.util.DemoUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Utility classs to download the MNIST data set.
  *
  * @author A. Mahmood (arshadm@spinor.io)
  */
class MNISTData extends DemoUtil {
  /** The training image dataset url. */
  final val TRAINING_IMAGES_URL = new URL("http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz")

  /** The training label dataset url. */
  final val TRAINING_LABELS_URL = new URL("http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz")

  /** The test images url. */
  final val TEST_IMAGES_URL = new URL("http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz")

  /** The test labels url. */
  final val TEST_LABELS_URL = new URL("http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz")

  /** The location of the training images file. */
  final val TRAINING_IMAGES_FILE = new File("/tmp/train-images-idx3-ubyte.gz")

  /** The location of the training labels file. */
  final val TRAINING_LABELS_FILE = new File("/tmp/train-labels-idx1-ubyte.gz")

  /** The location of the test images file. */
  final val TEST_IMAGES_FILE = new File("/tmp/t10k-images-idx3-ubyte.gz")

  /** The location of the test labels file. */
  final val TEST_LABELS_FILE = new File("/tmp/t10k-labels-idx1-ubyte.gz")

  /** The image file magic number. */
  final val IMAGES_MAGIC_NUMBER = 2051

  /** The image file magic number. */
  final val LABELS_MAGIC_NUMBER = 2049

  /** The expected number of rows. */
  final val EXPECTED_ROWS = 28

  /** The expected number of cols. */
  final val EXPECTED_COLS = 28

  /** The number of bytes per image. */
  final val BYTES_PER_IMAGE = 28 * 28

  /** The number of bytes per label. */
  final val BYTES_PER_LABEL = 1

  /* download the sample data. */
  super.downloadFile(TRAINING_IMAGES_URL, TRAINING_IMAGES_FILE)
  super.downloadFile(TRAINING_LABELS_URL, TRAINING_LABELS_FILE)
  super.downloadFile(TEST_IMAGES_URL, TEST_IMAGES_FILE)
  super.downloadFile(TEST_LABELS_URL, TEST_LABELS_FILE)

  /**
    * Gets the training images/labels RDD.
    *
    * @return the training images/labels RDD
    */
  def getTrainingData(): Array[(Array[Double], Double)] = {
    generateDataset(TRAINING_IMAGES_FILE, TRAINING_LABELS_FILE, 60000)
  }

  /**
    * Gets the test images/labels RDD.
    *
    * @return the test images/labels RDD
    */
  def getTestData(): Array[(Array[Double], Double)] = {
    generateDataset(TEST_IMAGES_FILE, TEST_LABELS_FILE, 10000)
  }

  /**
    * Generate the dataset RDD.
    *
    * @param imageFile the compressed images file
    * @param labelsFile the compressed labels file
    * @param expectedNumberOfImages the expected number of images/labels
    * @return the generated RDD
    */
  private def generateDataset(imageFile: File, labelsFile: File, expectedNumberOfImages: Int): Array[(Array[Double], Double)] = {
    val images = readImages(imageFile, expectedNumberOfImages)
    val labels = readLabels(labelsFile, expectedNumberOfImages)

    images zip labels
  }

  /**
    * Read the images into a sequence.
    *
    * @param imageFile              the compressed image file
    * @param expectedNumberOfImages the expected number of images
    * @return the sequence of images as byte arrays
    */
  private def readImages(imageFile: File, expectedNumberOfImages: Int): Array[Array[Double]] = {
    val gzipInputStream = new GZIPInputStream(new FileInputStream(imageFile))
    val images = ArrayBuffer.empty[Array[Double]]
    val tempBuffer = new Array[Byte](100)

    readAndCheckInt(gzipInputStream, IMAGES_MAGIC_NUMBER)
    readAndCheckInt(gzipInputStream, expectedNumberOfImages)
    readAndCheckInt(gzipInputStream, EXPECTED_ROWS)
    readAndCheckInt(gzipInputStream, EXPECTED_COLS)

    while (gzipInputStream.available() > 0) {
      val buffer = new Array[Byte](BYTES_PER_IMAGE)
      gzipInputStream.read(buffer, 0, BYTES_PER_IMAGE)

      images += buffer.map(_.toDouble)
    }

    gzipInputStream.close()

    images.toArray
  }

  /**
    * Read the labels into a sequence.
    *
    * @param imageFile              the compressed labels file
    * @param expectedNumberOfLabels the expected number of labels
    * @return the sequence of labels as bytes
    */
  private def readLabels(imageFile: File, expectedNumberOfLabels: Int): Array[Double] = {
    val gzipInputStream = new GZIPInputStream(new FileInputStream(imageFile))
    val labels = new Array[Byte](expectedNumberOfLabels)
    val tempBuffer = new Array[Byte](100)

    readAndCheckInt(gzipInputStream, LABELS_MAGIC_NUMBER)
    readAndCheckInt(gzipInputStream, expectedNumberOfLabels)

    gzipInputStream.read(labels, 0, expectedNumberOfLabels)
    gzipInputStream.close()

    labels.map(_.toDouble)
  }

  /**
    * Read an integer from the stream and check it's value.
    *
    * @param inputStream the input stream
    * @param expectedValue the expected value
    */
  private def readAndCheckInt(inputStream: InputStream, expectedValue: Int): Unit = {
    val tempBuffer = new Array[Byte](4)

    inputStream.read(tempBuffer, 0, 4)
    val value = ByteBuffer.wrap(tempBuffer).getInt
    checkArgument(value == expectedValue)
  }
}
