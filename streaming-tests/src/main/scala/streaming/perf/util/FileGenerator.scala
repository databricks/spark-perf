package streaming.perf.util

import org.apache.spark.{Logging, SparkContext}
import org.apache.hadoop.fs.{PathFilter, Path}
import org.apache.hadoop.conf.Configuration
import com.google.common.io.Files
import java.io.{IOException, File}
import java.text.SimpleDateFormat
import java.nio.charset.Charset
import java.util.Calendar
import scala.util.Random


class FileGenerator(sc: SparkContext, testDir: String, maxRecordsPerFile: Int, cleanerDelay: Long) extends Logging {

  val MAX_TRIES = 100
  val MAX_KEYS = 1000
  val INTERVAL = 100

  val testDirectory = new Path(testDir)
  val conf = new Configuration()
  val initFile = new Path(testDirectory, "test")
  val generatingThread = new Thread() { override def run() { generateFiles() }}
  val deletingThread = new Thread() { override def run() { deleteOldFiles() }}
  val localTestDir = Files.createTempDir()
  val localFile = new File(localTestDir, "temp")
  val df = new SimpleDateFormat("MM-dd-HH-mm-ss-SSS")

  var fs = testDirectory.getFileSystem(conf)

  def initialize() {
    if (fs.exists(testDirectory)) {
      fs.delete(testDirectory, true)
      fs.mkdirs(testDirectory)
    }
  }

  /** Start generating files */
  def start() {
    generatingThread.start()
    deletingThread.start()
    logInfo("Started")
  }

  /** Stop generating files */
  def stop() {
    generatingThread.interrupt()
    deletingThread.interrupt()
    generatingThread.join()
    logInfo("Interrupted")
    fs.close()
  }

  /** Delete test directory */
  def cleanup() {
    fs.delete(testDirectory, true)
  }

  /**
   * Generate files with increasing number of words in them.
   * First it will create files with "word1". The sequence is [ words1 ], [ word1 word1 ], [ word1 word1 word1 ], ...
   * Once it creates a files with "word1" up to maxRecords, then it will create file in same sequence with "word2".
   */
  private def generateFiles() {
    try {
      for (key <- 1 to MAX_KEYS) {
        if (localFile.exists()) localFile.delete()
        for (count <- 1 to maxRecordsPerFile) {
          Files.append("word" + key + " ", localFile, Charset.defaultCharset())
          val time = df.format(Calendar.getInstance().getTime())
          val finalFile = new Path(testDir, "file-" + time + "-" + key + "-" + count)
          val generated = copyFile(localFile, finalFile)
          if (generated) {
            logInfo("Generated file #" + count + " at " + System.currentTimeMillis() + ": " + finalFile)
          } else {
            logError("Could not generate file #" + count + ": " + finalFile)
            System.exit(0)
          }
          Thread.sleep(INTERVAL)
        }
      }
    } catch {
      case ie: InterruptedException =>
        logInfo("File generating thread interrupted")
      case e: Exception =>
        logError("Error generating files", e)
        System.exit(0)
    }
  }

  /** Copies a local file to a HDFS path */
  private def copyFile(localFile: File, finalFile: Path): Boolean = {
    var done = false
    var tries = 0
    while (!done && tries < MAX_TRIES) {
      tries += 1
      try {
        fs.copyFromLocalFile(new Path(localFile.toString), finalFile)
        done = true
      } catch {
        case ioe: IOException =>
          fs = testDirectory.getFileSystem(conf)
          logWarning("Attempt " + tries + " at generating file " + finalFile + " failed.", ioe)
      }
    }
    done
  }

  /** Delete old files */
  private def deleteOldFiles() {
    var interrupted = false
    while (!interrupted) {
      try {
        Thread.sleep(cleanerDelay * 1000 / 5)
        val newFilter = new PathFilter() {
          def accept(path: Path): Boolean = {
            val modTime = fs.getFileStatus(path).getModificationTime()
            logInfo("Mod time for " + path + " is " + modTime)
            modTime < (System.currentTimeMillis() - cleanerDelay * 1000)
          }
        }
        logInfo("Finding files older than " + (System.currentTimeMillis() - cleanerDelay * 1000))
        val oldFiles = fs.listStatus(testDirectory, newFilter).map(_.getPath)
        oldFiles.foreach(file => {
          logInfo("Deleting file " + file)
          fs.delete(file, true)
        })
      } catch {
        case ie: InterruptedException =>
          interrupted = true
          logInfo("File deleting thread interrupted")
        case e: Exception =>
          logWarning("Deleting files gave error ", e)
      }
    }
  }
}