package streaming.perf.util

import java.io.{BufferedReader, File, FileReader, IOException}
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.Calendar

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}

class FileGenerator(dataDir: String, tempDataDir: String, maxRecordsPerFile: Long, cleanerDelay: Long) {

  val MAX_TRIES = 100
  val MAX_KEYS = 1000
  val INTERVAL = 100
  val VERIFY_LOCAL_FILES = false

  val dataDirectory = new Path(dataDir)
  val tempDataDirectory = new Path(tempDataDir)
  val localFile = new File(Files.createTempDir(), "temp")
  val tempFile = new Path(tempDataDirectory, "temp-file")
  val conf = new Configuration()
  // val initFile = new Path(dataDirectory, "test")
  val generatingThread = new Thread() { override def run() { generateFiles() }}
  val deletingThread = new Thread() { override def run() { deleteOldFiles() }}
  val df = new SimpleDateFormat("MM-dd-HH-mm-ss-SSS")

  var fs_ : FileSystem = null

  def initialize() {
    if (fs.exists(dataDirectory)) {
      fs.delete(dataDirectory, true)
    }
    fs.mkdirs(dataDirectory)
    if (fs.exists(tempDataDirectory)) {
      fs.delete(tempDataDirectory, true)
    }
    fs.mkdirs(tempDataDirectory)
  }

  /** Start generating files */
  def start() {
    generatingThread.setDaemon(true)
    deletingThread.setDaemon(true)
    generatingThread.start()
    deletingThread.start()
    println("FileGenerator started")
  }

  /** Stop generating files */
  def stop() {
    generatingThread.interrupt()
    deletingThread.interrupt()
    println("FileGenerator Interrupted")
  }

  /** Delete test directory */
  def cleanup() {
    fs.delete(dataDirectory, true)
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
        for (count <- 1L to maxRecordsPerFile) {
          val word = "word" + key
          val newLine = if (count % 10 == 0) "\n" else ""
          Files.append(word + " " + newLine, localFile, Charset.defaultCharset())
          if (VERIFY_LOCAL_FILES) verifyLocalFile(word, count)
          val time = df.format(Calendar.getInstance().getTime())
          val finalFile = new Path(dataDir, "file-" + time + "-" + key + "-" + count)
          val generated = copyFile(localFile, finalFile)
          if (generated) {
            println("Generated file #" + count + " at " + System.currentTimeMillis() + ": " + finalFile)
          } else {
            println("Could not generate file #" + count + ": " + finalFile)
            System.exit(0)
          }
          Thread.sleep(INTERVAL)
        }
      }
    } catch {
      case ie: InterruptedException =>
        println("File generating thread interrupted")
      case e: Exception =>
        println("Error generating files", e)
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
        println("Copying from " + localFile + " to " + tempFile)
        fs.copyFromLocalFile(new Path(localFile.toString), tempFile)
        //if (fs.exists(tempFile)) println("" + tempFile + " exists") else println("" + tempFile + " does not exist")
        //println("Renaming from " + tempFile + " to " + finalFile)
        if (!fs.rename(tempFile, finalFile)) throw new Exception("Could not rename " + tempFile + " to " + finalFile)
        done = true
      } catch {
        case ioe: IOException =>
          println("Attempt " + tries + " at generating file " + finalFile + " failed.", ioe)
          reset()
      } finally {
        // if (fs.exists(tempFile)) fs.delete(tempFile, true)
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
        val oldFileThreshTime = System.currentTimeMillis - cleanerDelay * 1000
        val newFilter = new PathFilter() {
          def accept(path: Path): Boolean = {
            val modTime = fs.getFileStatus(path).getModificationTime()
            //println("Mod time for " + path + " is " + modTime)
            modTime < oldFileThreshTime
          }
        }
        println("Finding files older than " + oldFileThreshTime)
        val oldFiles = fs.listStatus(dataDirectory, newFilter).map(_.getPath)
        println("Found " + oldFiles.size + " old files")
        oldFiles.foreach(file => {
          println("Deleting file " + file)
          fs.delete(file, true)
        })
      } catch {
        case ie: InterruptedException =>
          interrupted = true
          println("File deleting thread interrupted")
        case e: Exception =>
          println("Deleting files gave error ", e)
          reset()
      }
    }
  }

  private def verifyLocalFile(expectedWord: String, expectedCount: Long) {
    val br = new BufferedReader(new FileReader(localFile))
    var line = ""
    var count = 0L
    var wordMatch = true
    line = br.readLine()
    while (line != null) {
      val words = line.split(" ").filter(_.size != 0)
      wordMatch = wordMatch && words.forall(_ == expectedWord)
      count += words.size
      line = br.readLine()
    }
    br.close()
    println("Local file has " + count + " occurrences of " + expectedWord +
      (if (count != expectedCount)  ", expected was " + expectedCount else ""))
  }

  private def fs: FileSystem = synchronized {
    if (fs_ == null) fs_ = dataDirectory.getFileSystem(new Configuration())
    fs_
  }

  private def reset() {
    fs_ = null
  }
}
