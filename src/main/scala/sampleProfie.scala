import ProfileHelpers.runProfile
import org.apache.spark.sql.SparkSession

object sampleProfie {
  def main(args: Array[String]): Unit = {

    val spark = {
      SparkSession.builder()
        .appName("deUtils")
        .master("local")
        //      .enableHiveSupport()
        .getOrCreate()
    }

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val ipDF = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(s"$currentDirectory/src/test/resources/dtypes.csv")


    spark.conf.getAll map (println(_))

    ipDF.printSchema()

    val profiledDF = runProfile(ipDF)


    profiledDF.show(100)
    profiledDF.printSchema()

  }
}
