import org.apache.spark.sql.SparkSession
import GeneralFunctionSets.hourOfDay

import scala.collection.mutable.ArrayBuffer

/**
 * 统计每对OD在高峰时间段的客流量
 * 高峰时间段有7h、8h、18h、19h
 */
object ODDistribution {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OD Distribution")
      .getOrCreate()
    val sc = spark.sparkContext

    val peakHours = Set(7, 8, 18, 19)

    // (688561037,2019-06-02 07:49:34,兴东,21,2019-06-02 07:58:55,翻身,22)
    val readFile = sc.textFile(args(0)).map(line => {
      val fields = line.split(",")
      val ot = hourOfDay(fields(1))
      val os = fields(2)
      val dt = hourOfDay(fields(4))
      val ds = fields(5)
      val cot = if (ot == dt) ot else 24
      ((os, ds), cot)
    }).filter(x => peakHours.contains(x._2))

    val result = readFile.groupByKey().map(line => {
      val data = line._2
      val buff = new ArrayBuffer[Int]()
      for (v <- peakHours)
        buff.append(data.count(_==v))
      buff.append(data.size)
      (line._1._1, line._1._2, buff(0), buff(1), buff(2), buff(3), buff(4))
    }).cache()

    result.repartition(1).sortBy(_._7, ascending = false).saveAsTextFile(args(1))

    val sum1 = result.map(_._3).sum()
    val sum2 = result.map(_._4).sum()
    val sum3 = result.map(_._5).sum()
    val sum4 = result.map(_._6).sum()
    val sum5 = result.map(_._7).sum()
    println("total:", sum1, sum2, sum3, sum4, sum5)

    spark.stop()
  }
}
