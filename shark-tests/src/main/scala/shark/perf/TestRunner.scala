package shark.perf

import shark.{SharkContext, SharkEnv}

import spark.perf.PerfTest

object TestRunner {
  def main(args: Array[String]) {
    if (args.size < 2) {
      println(
        "shark.perf.TestRunner requires 2 or more args, you gave %s, exiting".format(args.size))
    }
    val testName = args(0)
    val master = args(1)
    val perfTestArgs = args.slice(2, args.length)

    val sharkContext = SharkEnv.initWithSharkContext("TestRunner: " + testName, master)
    val userDirectory = System.getProperty("user.dir")
    sharkContext.addJar(userDirectory + "/spark-tests/target/spark-perf-tests-assembly.jar")
    sharkContext.addJar(userDirectory + "/shark-tests/target/shark-perf-tests-assembly.jar")

    // Initialize Shark/Hive metastore and warehouse directories.
    val testWarehousesDir = System.getenv("PERF_TEST_WAREHOUSES")
    val metastoreDir = testWarehousesDir + testName + "-metastore"
    val setMetastoreSQLStmt = "set javax.jdo.option.ConnectionURL=jdbc:derby:; " +
      ("databaseName=%s;".format(metastoreDir)) + "create=true"
    sharkContext.sql(setMetastoreSQLStmt)
    val warehouseDir = testWarehousesDir + testName + "-warehouse"
    sharkContext.sql("set hive.metastore.warehouse.dir=" + warehouseDir)

/*
    val test: PerfTest =
      testName match {
        case "table-scan-query" => new TableScan(sharkContext)
    }
    test.initialize(perfTestArgs)
    test.createInputData()
    val results: Seq[Double] = test.run()
    println("results: " + results.map(r => "%.3f".format(r)).mkString(","))
*/
  }
}
