import GeneralFunctionSets.hourOfDay
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
 * 统计每对OD在高峰时间段的客流量
 * 高峰时间段有7h、8h、18h、19h、20h、21h
 */
object ODDistribution {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("OD Distribution")
            .getOrCreate()
        val sc = spark.sparkContext

        val peakHours = Set(7, 8, 18, 19, 20, 21)

        // (688561037,2019-06-02 07:49:34,兴东,21,2019-06-02 07:58:55,翻身,22)
        val readFile = sc.textFile(args(0)).map(line => {
            val fields = line.split(",")
            val id = fields(0).drop(1)
            val ot = hourOfDay(fields(1))
            val os = fields(2)
            val dt = hourOfDay(fields(4))
            val ds = fields(5)
            val cot = if (ot == dt) ot else 24
            (id, (os, ds, cot))
        }).filter(x => peakHours.contains(x._2._3))

        val processedRDD = readFile.groupByKey().mapValues(_.toList).flatMap(line => {
            // 统计在固定时段从A站到B站的天数大于k的模式
            val k = 15
            val l = line._2.groupBy(x => x).mapValues(_.size).toList.filter(_._2 > k)
            for (v <- l) yield
                ((v._1._1, v._1._2), v._1._3)
        })

        val result = processedRDD.groupByKey().map(line => {
            val data = line._2
            val ph = List(7, 8, 18, 19, 20, 21)
            val buff = new ArrayBuffer[Int]()
            for (v <- ph)
                buff.append(data.count(_ == v))
            buff.append(data.size)
            (line._1._1, line._1._2, buff(0), buff(1), buff(2), buff(3), buff(4), buff(5), buff(6))
        }).cache()

        result.repartition(1).sortBy(_._9, ascending = false).saveAsTextFile(args(1))

        val sum1 = result.map(_._3).sum()
        val sum2 = result.map(_._4).sum()
        val sum3 = result.map(_._5).sum()
        val sum4 = result.map(_._6).sum()
        val sum5 = result.map(_._7).sum()
        val sum6 = result.map(_._8).sum()
        val sum7 = result.map(_._9).sum()
        //    println("total:", sum1, sum2, sum3, sum4, sum5)
        println("****************************************")
        println(s"total of 7h: $sum1")
        println(s"total of 8h: $sum2")
        println(s"total of 18h: $sum3")
        println(s"total of 19h: $sum4")
        println(s"total of 20h: $sum5")
        println(s"total of 21h: $sum6")
        println(s"total of all: $sum7")
        println("****************************************")
        spark.stop()
    }
}
