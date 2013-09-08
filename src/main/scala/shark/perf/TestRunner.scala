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

    val sc: SharkContext = SharkEnv.initWithSharkContext("TestRunner: " + testName, master)
    sc.addJar(System.getProperty("user.dir") + "/target/perf-tests-assembly.jar")

    // Initialize Shark/Hive metastore and warehouse directories using values passed to
    // config.COMMON_JAVA_OPTS.
    val testWarehousesDir = System.getenv("PERF_TEST_WAREHOUSES")
    val metastoreDir = testWarehousesDir + testName + "-metastore"
    val setMetastoreSQLStmt = "set javax.jdo.option.ConnectionURL=jdbc:derby:; " +
      ("databaseName=%s;".format(metastoreDir)) + "create=true"
    sc.sql(setMetastoreSQLStmt)
    val warehouseDir = testWarehousesDir + testName + "-warehouse"
    sc.sql("set hive.metastore.warehouse.dir=" + warehouseDir)
  }
}