package io.spinor.sparkdemo.data

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.URL
import java.util.zip.ZipInputStream

import io.spinor.sparkdemo.util.DemoUtil
import org.apache.commons.io.IOUtils

import scala.collection.JavaConverters._

/**
  * This class defines methods for retrieving and labelling the digital breath tests data
  * from http://data.dft.gov.uk.s3.amazonaws.com/road-accidents-safety-data.
  *
  * @author A. Mahmood (arshadm@spinor.io)
  */
class DigitalBreathTestData extends DemoUtil {
  /** The month label generator. */
  final val monthLabel = (month: String) => month match {
    case "Jan" => 0.0
    case "Feb" => 1.0
    case "Mar" => 2.0
    case "Apr" => 3.0
    case "May" => 4.0
    case "Jun" => 5.0
    case "Jul" => 6.0
    case "Aug" => 7.0
    case "Sep" => 8.0
    case "Oct" => 9.0
    case "Nov" => 10.0
    case "Dec" => 11.0
    case _ => 99.9
  }

  /** The gender label generator. */
  final val genderLabel = (gender: String) => gender match {
    case "Male" => 0.0
    case "Female" => 1.0
    case "Unknown" => 2.0
    case _ => 99.9
  }

  /** The violation label. */
  final val violationLabel = (violation: String) => violation match {
    case "Moving Traffic Violation" => 0.0
    case "Other" => 1.0
    case "Road Traffic Collision" => 2.0
    case "Suspicion of Alcohol" => 3.0
    case _ => 99.9
  }

  /** The day label. */
  final val dayLabel = (day: String) => day match {
    case "Weekday" => 0.0
    case "Weekend" => 1.0
    case _ => 99.9
  }

  /** The time label. */
  final val timeLabel = (time: String) => time match {
    case "12am-4am" => 0.0
    case "4am-8am" => 1.0
    case "8am-12pm" => 2.0
    case "12pm-4pm" => 3.0
    case "4pm-8pm" => 4.0
    case "8pm-12pm" => 5.0
    case _ => 99.9
  }

  /** The age label. */
  final val ageLabel = (age: String) => age match {
    case "16-19" => 0.0
    case "20-24" => 1.0
    case "25-29" => 2.0
    case "30-39" => 3.0
    case "40-49" => 4.0
    case "50-59" => 5.0
    case "60-69" => 6.0
    case "70-98" => 7.0
    case _ => 99.9
  }

  /** The zip file. */
  val zipFile = new File("/tmp/DigitalBreathTestData2013.zip")

  /**
    * Load the sample data.
    *
    * @return the file reference for the sample data
    */
  def loadData(): File = {
    if (!zipFile.exists()) {
      val url = new URL("http://data.dft.gov.uk.s3.amazonaws.com/road-accidents-safety-data/DigitalBreathTestData2013.zip")

      // download the road safety data file
      super.downloadFile(url, zipFile)
    }

    // extract entry from zip file
    val zin = new ZipInputStream(new FileInputStream(zipFile))

    val ze = zin.getNextEntry
    val csvDataFile = File.createTempFile("DigitalBreathTestData2013", ".csv")
    IOUtils.copy(zin, new FileOutputStream(csvDataFile))

    csvDataFile
  }

  /**
    * Label the input file and write the output to a new file.
    *
    * @param file the input csv file
    * @return the output file
    */
  def labelData(file: File): File = {
    val outputFile = File.createTempFile("DigitalBreathTestData2013Labelled", ".csv")
    val outputStream = new FileOutputStream(outputFile)
    val lines = IOUtils.readLines(new FileInputStream(file)).asScala

    for (
      line <- lines
      if !line.startsWith("Reason")
    ) {
      val items = line.split(',')
      val reason = violationLabel(items(0))
      val month = monthLabel(items(1))
      val year = items(2)
      val day = dayLabel(items(3))
      val time = timeLabel(items(4))
      val alcohol = items(5)
      val age = ageLabel(items(6))
      val gender = genderLabel(items(7))

      val csvLine = s"$gender,$reason,$month,$year,$day,$time,$alcohol,$age\n"
      outputStream.write(csvLine.getBytes("UTF-8"))
    }

    outputStream.close()
    outputFile
  }
}
