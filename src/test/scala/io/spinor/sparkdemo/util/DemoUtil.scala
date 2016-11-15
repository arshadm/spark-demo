package io.spinor.sparkdemo.util

import java.io.{File, FileOutputStream}
import java.net.URL
import java.nio.channels.Channels

/**
  * Utility functions used by the demo.
  *
  * @author A. Mahmood (arshadm@spinor.io)
  */
trait DemoUtil {
  /**
    * Download a file from the specified URI and store in the given file,
    *
    * @param url  the url from which the data is to be downloaded
    * @param file the file the data is to be stored
    */
  def downloadFile(url: URL, file: File): Unit = {
    val os = new FileOutputStream(file)
    val rbc = Channels.newChannel(url.openStream())

    os.getChannel().transferFrom(rbc, 0, Long.MaxValue)
  }
}
