import ProfileHelpers.runProfile
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.sql.functions.expr

object sampleSedona {
  def main(args: Array[String]): Unit = {

    val spark = {
      SparkSession
        .builder()
        .appName("someThing")
        .master("local")
        .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
        .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.sql.files.maxPartitionBytes", "536870912")
        .config("spark.sql.sources.parallelPartitionDiscovery.threshold", "2100")
        .config("spark.sql.files.openCostInBytes", "41943040")
        .config("spark.sql.files.minPartitionNum", "12")
        .config("spark.kryoserializer.buffer.max", "2047")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", true)
        .config("spark.debug.maxToStringFields", 0)
        .config("sedona.global.charset", "utf8")
        .getOrCreate()
    }
    SedonaSQLRegistrator.registerAll(spark)

  val spatial_df=spark.sql("SELECT ST_Point(51.0,0.0) as point_")
    .withColumn("point_3857", expr("""ST_Transform(point_,"epsg:4326", "epsg:3857")"""))
    spatial_df.printSchema()
    spatial_df.show(3, false)

    val sdf=spark.sql("""
  select ROUND(
  ST_Distance(
  ST_Transform(ST_Point(51.354026080547015, 0.09604635637162225), 'epsg:4326', 'epsg:3857'),
  ST_Transform(ST_Point(51.35670567455075, 0.08205221154175488), 'epsg:4326', 'epsg:3857')
  )*0.000621371/1.60934,3)
  as distance_in_kms
  """)

    sdf.show(3, false)
  }
}
