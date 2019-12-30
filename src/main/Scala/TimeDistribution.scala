import GeneralFunctionSets.hourOfDay
import org.apache.spark.sql.SparkSession

/**
 * 统计每个小时的记录数量
 */
object TimeDistribution {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Time Distribution")
      .getOrCreate()

    val sc = spark.sparkContext

    val dataFile = sc.textFile(args(0)).map(line => {
      val fields = line.split(",")
      val time = hourOfDay(fields(1))
      (time, 1)
    })

    val result = dataFile.reduceByKey(_+_).repartition(1).sortBy(_._2, ascending = false)

    result.saveAsTextFile(args(1))

//    dataFile.collect().foreach(println)

    sc.stop()
  }

}
