package streaming.perf

import joptsimple.OptionParser

class BasicTest extends PerfTest {

  def run(): Seq[String] = {
    println("Running BasicTest.run()")
    Seq("Result of BasicTest")
  }
}
