package streaming.perf

object TestRunner {

  def main(args: Array[String]) {
    if (args.size < 1) {
      println("streaming.perf.TestRunner requires 1 or more args, you gave %s, exiting".format(args.size))
      System.exit(1)
    }
    val testName = args(0)
    val perfTestArgs = args.slice(1, args.length)

    val testClassName = getTestClassName(testName)
    println("Running " + testName + " (class = " + testClassName + ")" +
      " with arguments " + perfTestArgs.mkString("[", ",", "]"))
    val test = Class.forName(testClassName).newInstance().asInstanceOf[PerfTest]
    test.initialize(testName, perfTestArgs)
    val result = test.run()
    println("\n" + ("=" * 100) + "\n\nResult: " + result)
    System.out.flush()
  }

  /**
   * Converts "reduce-by-key" to "ReduceByKeyTest".
   * 1. First letter and any letter after - is capitalized, - removed, rest untouched
   * 2. If "Test" isnt already there at the end, it is added.
   */
  def getTestClassName(testName: String) = {
    var caps = true

    val temp = testName.toCharArray.flatMap(c => {
      if (c.isLetterOrDigit) {
        if (caps) {
          caps = false
          Seq(c.toUpper)
        } else {
          Seq(c)
        }
      } else {
        caps = true
        Seq()
      }
    })
    val updatedName = new String(temp)
    println(testName + " --> " + updatedName)
    val className = if (updatedName.toLowerCase.endsWith("test")) {
      updatedName.substring(0, updatedName.length - 4) + "Test"
    } else {
      updatedName + "Test"
    }
    "streaming.perf." + className
  }
}

