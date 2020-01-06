import GeneralFunctionSets.{dayOfMonth_long, secondsOfDay, transTimeToTimestamp}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Stack}
import scala.math.{Pi, exp, pow, sqrt, abs, min}

/**
 * User unique identification
 */
object Model {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("OD Distribution")
      .getOrCreate()
    val sc = spark.sparkContext

    // Pre-processing
    // 读取AFC数据: (020798332,2019-06-24 10:06:50,碧海湾,2019-06-24 10:25:09,桥头)
    val AFCFile = sc.textFile(args(0)).map(line => {
      val fields = line.split(',')
      val id = fields(0).drop(1)
      val ot = transTimeToTimestamp(fields(1))
      val os = fields(2)
      val dt = transTimeToTimestamp(fields(3))
      val ds = fields.last.dropRight(1)
      (id, (ot, os, dt, ds))
    })

    // 划分AFC
    val AFCPartitions = AFCFile.groupByKey().mapValues(_.toList.sortBy(_._1))

    // AFC模式提取-基于核密度估计的聚类
    val AFCPatterns = AFCPartitions.map(line => {
      val pairs = line._2
      // 提取时间戳对应当天的秒数用于聚类
      val stampBuffer = new ArrayBuffer[Int]()
      pairs.foreach(v => {
        stampBuffer.append(secondsOfDay(v._1))
        stampBuffer.append(secondsOfDay(v._3))
      })
      val timestamps = stampBuffer.toArray.sorted
      // 设置带宽h，单位为秒
      val h = 1800
      // 计算局部密度
      val densityBuffer = new ArrayBuffer[Double]()
      for (t <- timestamps) {
        var temp = 0D
        for (v <- timestamps) {
          temp += RBF(v, t, h)
        }
        densityBuffer.append(temp / (timestamps.length * h))
      }
      val density = densityBuffer.toArray

      // 计算相对距离
      val dist = relative_dist(density, timestamps)

    })

    //    AFCPatterns.collect().foreach(x => {
    //      x._1.foreach(x => print(s"$x,"))
    //      println()
    //      x._2.foreach(x => print(s"$x,"))
    //      println()
    //      x._3.foreach(x => print(s"$x,"))
    //      println()
    //    })


    // 读取AP数据:(000000000000,2019-06-01 10:38:05,布吉,0,2019-06-01 10:43:50,上水径,15)
    val APFile = sc.textFile(args(1)).map(line => {
      val fields = line.split(",")
      val id = fields(0).drop(1)
      val ot = transTimeToTimestamp(fields(1))
      val os = fields(2)
      val o_stay = fields(3).toInt
      val dt = transTimeToTimestamp(fields(4))
      val ds = fields(5)
      val d_stay = fields.last.dropRight(1).toInt
      (id, (ot, os, o_stay, dt, ds, d_stay))
    })

    // 划分AP
    val APPartitions = APFile.groupByKey().mapValues(_.toList.sortBy(_._1))

    val APPatterns = APPartitions.map(line => {
      val pairs = line._2
      // 提取时间戳对应当天的秒数用于聚类
      val stampBuffer = new ArrayBuffer[Int]()
      pairs.foreach(v => {
        stampBuffer.append(secondsOfDay(v._1))
        stampBuffer.append(secondsOfDay(v._4))
      })
      val timestamps = stampBuffer.toArray.sorted
      // 设置带宽h，单位为秒
      val h = 1800
      // 计算局部密度
      val densityBuffer = new ArrayBuffer[Double]()
      for (t <- timestamps) {
        var temp = 0D
        for (v <- timestamps) {
          temp += RBF(v, t, h)
        }
        densityBuffer.append(temp / (timestamps.length * h))
      }
      val density = densityBuffer.toArray

      // 计算相对距离
      val dist = relative_dist(density, timestamps)


    })

//    APPatterns.collect().foreach(x => {
//      x._1.foreach(x => print(s"$x,"))
//      println()
//      x._2.foreach(x => print(s"$x,"))
//      println()
//      x._3.foreach(x => print(s"$x,"))
//      println()
//    })

    spark.stop()
  }

  // 高斯核函数
  def RBF(l : Long, x : Long, h: Int) : Double = {
    1 / sqrt(2 * Pi) * exp(-pow(x - l, 2) / (2 * pow(h, 2)))
  }

  // 计算相对距离
  def relative_dist(dens : Array[Double], pos : Array[Int]) : Array[Int] = {
    val dens_pos = new ArrayBuffer[(Double, Int)]()
    for (i <- dens.indices) {
      dens_pos.append((dens(i), pos(i)))
    }
    val dist_r = compute_dist(dens_pos.toArray)
    val dist_l = compute_dist(dens_pos.toArray.reverse).reverse
    val dist = new ArrayBuffer[Int]()
    for (i <- dist_r.indices) {
      if (dist_r(i) == -1 && dist_l(i) == -1)
        dist.append(pos.last - pos.head)
      else if (dist_r(i) != -1 && dist_l(i) != -1)
        dist.append(min(dist_r(i), dist_l(i)))
      else if (dist_l(i) != -1)
        dist.append(dist_l(i))
      else
        dist.append(dist_r(i))
    }
    dist.toArray
  }

  def compute_dist(info : Array[(Double, Int)]) : Array[Int] = {
    val result = new Array[Int](info.length)
    val s = mutable.Stack[Int]()
    s.push(0)
    var i = 1
    var index = 0
    while (i < info.length) {
      if (s.nonEmpty && info(i)._1 > info(s.top)._1) {
        index = s.pop()
        result(index) = abs(info(i)._2 - info(index)._2)
      }
      else{
        s.push(i)
        i += 1
      }
    }
    while (s.nonEmpty) {
      result(s.pop()) = -1
    }
    result
  }
}
