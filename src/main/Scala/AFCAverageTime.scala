import org.apache.spark.sql.SparkSession
import GeneralFunctionSets.{transTimeToTimestamp, dayOfMonth_long}

object AFCAverageTime {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Matching Model")
      .getOrCreate()
    val sc = spark.sparkContext

    // (667979926,2019-06-04 08:42:22,坪洲,21,2019-06-04 08:55:23,宝安中心,22)
    val averageOfEveryDay = sc.textFile(args(0) + "/Destin/subway-pair/part-*").map(line => {
      val fields = line.split(",")
      val ot = transTimeToTimestamp(fields(1))
      val dt = transTimeToTimestamp(fields(4))
      val day_of_month = dayOfMonth_long(ot)
      (day_of_month, (dt - ot, 1))
    })
      .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
      .map(x => (x._1, x._2._1 / x._2._2))
      .repartition(1)
      .sortByKey(ascending = true)

    averageOfEveryDay.saveAsTextFile(args(0) + "/liutao/UI/AverageTimeOfAFC")
    sc.stop()

  }
}
