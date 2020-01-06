import org.apache.spark.sql.SparkSession
import GeneralFunctionSets.dayOfMonth_string

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * (667979926,2019-06-04 08:42:22,坪洲,21,2019-06-04 08:55:23,宝安中心,22)
 * 对AFC数据按天采样，仅保留15天的数据
 */
object SamplingAFCData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Sampling AFC").getOrCreate()

    val sc = spark.sparkContext


    val readFile = sc.textFile(args(0)).map(line => {
      val fields = line.split(",")
      val id = fields(0).drop(1)
      val ot = fields(1)
      val os = fields(2)
      val dt = fields(4)
      val ds = fields(5)
      val day = if (dayOfMonth_string(ot) == dayOfMonth_string(dt)) dayOfMonth_string(ot) else -1
      (id, ((ot, os, dt, ds), day))
    })

    val processingRDD = readFile.groupByKey().map(line => {
      val daySets: mutable.Set[Int] = mutable.Set()
      line._2.foreach(x => if (x._2 != -1) daySets.add(x._2))
      val half = daySets.size / 2
      val chooseDays = Random.shuffle(daySets).take(half)
      val sampledData = new ListBuffer[(String, String, String, String)]
      for (v <- line._2) {
        if (chooseDays.contains(v._2))
          sampledData.append(v._1)
      }
      (line._1, sampledData.toList)
    })

    val result = processingRDD.flatMap(line => for (v <- line._2) yield (line._1, v._1, v._2, v._3, v._4))
      .repartition(100)
      .sortBy(x => (x._1, x._2))

    result.saveAsTextFile(args(1))
    spark.stop()
  }
}
