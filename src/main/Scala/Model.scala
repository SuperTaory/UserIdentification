import GeneralFunctionSets.{dayOfMonth_long, secondsOfDay, transTimeToString, transTimeToTimestamp}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.{Pi, abs, exp, min, pow, sqrt}

/**
 * User unique identification
 */
object Model {

  case class distAndKinds(var d:Long, var k:Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("OD Distribution")
      .getOrCreate()
    val sc = spark.sparkContext

    // 读取地铁站点名和编号映射关系
    val stationFile = sc.textFile(args(0))
    val stationNoToNameRDD = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationNo.toInt, stationName)
    })
    val stationNoToName = sc.broadcast(stationNoToNameRDD.collect().toMap)


    // 读取所有有效路径的数据
    val validPathFile = sc.textFile(args(1)).map(line => {
      // 仅保留站点编号信息
      val fields = line.split(' ').dropRight(5)
      val sou = stationNoToName.value(fields(0).toInt)
      val des = stationNoToName.value(fields(fields.length-1).toInt)
      val pathStations = new ListBuffer[String]
      fields.foreach(x => pathStations.append(stationNoToName.value(x.toInt)))
      ((sou, des), pathStations.toList)
    }).groupByKey().mapValues(_.toList).cache()

    // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
    val validPathMap = sc.broadcast(validPathFile.collect().toMap)

    // Pre-processing
    // 读取AFC数据: (020798332,2019-06-24 10:06:50,碧海湾,2019-06-24 10:25:09,桥头)
    val AFCFile = sc.textFile(args(0)).map(line => {
      val fields = line.split(',')
      val id = fields(0).drop(1)
      val ot = transTimeToTimestamp(fields(1))
      val os = fields(2)
      val dt = transTimeToTimestamp(fields(3))
      val ds = fields.last.dropRight(1)
      val o_day = dayOfMonth_long(ot)
      val d_day = dayOfMonth_long(dt)
      val day = if (o_day == d_day) o_day else 0
      (id, (ot, os, dt, ds, day))
    })

    // 划分AFC
    val AFCPartitions = AFCFile.groupByKey().mapValues(_.toList.sortBy(_._1))

    // AFC模式提取-基于核密度估计的聚类
    val AFCPatterns = AFCPartitions.map(line => {
      val pairs = line._2
      // 计算出行天数
      val daySets : mutable.Set[Int] = mutable.Set()
      pairs.foreach(x => daySets.add(x._5))

      // 统计主要站点-依照出现次数
      val stationCount = new ArrayBuffer[String]()
      pairs.foreach(x => {
        stationCount.append(x._2)
        stationCount.append(x._4)
      })
      val Q = 1
      val topStations = stationCount.groupBy(x => x)
        .mapValues(_.size)
        .toArray
        .sortBy(_._2)
        .takeRight(Q)
        .map(_._1)

      // 提取时间戳对应当天的秒数用于聚类
      val stampBuffer = new ArrayBuffer[Long]()
      pairs.foreach(v => {
        stampBuffer.append(secondsOfDay(v._1))
        stampBuffer.append(secondsOfDay(v._3))
      })
      val timestamps = stampBuffer.toArray.sorted
      // 设置带宽h，单位为秒
      val h = 1800
      // 计算局部密度
      val density_stamp_Buffer = new ArrayBuffer[(Double, Long)]()
      for (t <- timestamps) {
        var temp = 0D
        for (v <- timestamps) {
          temp += RBF(v, t, h)
        }
        density_stamp_Buffer.append((temp / (timestamps.length * h), t))
      }
      val density_stamp = density_stamp_Buffer.toArray.sortBy(_._2)

      // 判断是否存在聚类中心，若返回为空则不存在，否则分类
      val cluster_center = z_score(density_stamp)
//      cluster_center.foreach(x => println(x._1.toString + '\t' + (x._2/3600).toString + ":" + (x._2%3600/60).toString + ":" + (x._2%3600%60).toString))
//      println("DaySets:" + daySets.toArray.sorted.mkString(","))

      // 设置类边界距离并按照聚类中心分配数据
      val dc = 5400
      // 初始化类簇,结构为[所属类，出行片段]
      val clusters = new ArrayBuffer[(Int, (Long, String, Long, String, Int))]
      for (v <- pairs) {
        if (cluster_center.nonEmpty) {
          val o_stamp = secondsOfDay(v._1)
          val d_stamp = secondsOfDay(v._3)
          val o_to_c = distAndKinds(Long.MaxValue, 0)
          val d_to_c = distAndKinds(Long.MaxValue, 0)
          for (c <- cluster_center) {
            if (abs(o_stamp - c._2) < dc && abs(o_stamp - c._2) < o_to_c.d){
              o_to_c.k = c._1
              o_to_c.d = abs(o_stamp - c._2)
            }
            if (abs(d_stamp - c._2) < dc && abs(d_stamp - c._2) < d_to_c.d){
              d_to_c.k = c._1
              d_to_c.d = abs(d_stamp - c._2)
            }
          }
          if (o_to_c.k == d_to_c.k && o_to_c.k != 0)
            clusters.append(( o_to_c.k, v))
//          else
//            clusters.append((0, v))
        }
//        else
//          clusters.append((0,v))
      }
      // 按照所属类别分组
      val grouped = clusters.groupBy(_._1).toArray
      // 存储出行模式集合
      val afc_patterns = new ArrayBuffer[(String, String)]()
      grouped.foreach(g => {
        // 同一类中数据按照进出站分组
        val temp_data = g._2.toArray.groupBy(x => (x._2._2, x._2._4))
        temp_data.foreach(v => {
          // 超过总出行天数的1/2则视为出行模式
          if ( daySets.size >= 7 && v._2.length >= daySets.size / 2) {
            afc_patterns.append(v._1)
          }
        })
      })

      (line._1, pairs, afc_patterns.toArray, daySets)
    })

//    AFCPatterns.collect().foreach(x => {
//      x.foreach(x => {
//        println(x._1)
//        x._2.foreach(v => println(transTimeToString(v._2._1) + ',' + v._2._2 + ',' + transTimeToString(v._2._3) + ',' + v._2._4))
//      })
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
      val o_day = dayOfMonth_long(ot)
      val d_day = dayOfMonth_long(dt)
      val day = if (o_day == d_day) o_day else 0
      (id, (ot, os, o_stay, dt, ds, d_stay, day))
    })

    // 划分AP
    val APPartitions = APFile.groupByKey().mapValues(_.toList.sortBy(_._1))

    val APPatterns = APPartitions.map(line => {
      val pairs = line._2

      // 统计出行天数
      val daySets : mutable.Set[Int] = mutable.Set()
      pairs.foreach(x => daySets.add(x._7))

      // 统计主要站点-依照停留时间
      val stationCount = new ArrayBuffer[(String, Int)]()
      pairs.foreach(x => {
        stationCount.append((x._2, x._3))
        stationCount.append((x._5, x._6))
      })
      val Q = 1
      val topStations = stationCount.groupBy(x => x._1).mapValues(v => {
        var sum = 0
        v.foreach(x => sum += x._2)
        sum
      }).toArray.sortBy(_._2).takeRight(Q).map(_._1)

      // 提取时间戳对应当天的秒数用于聚类
      val stampBuffer = new ArrayBuffer[Long]()
      pairs.foreach(v => {
        stampBuffer.append(secondsOfDay(v._1))
        stampBuffer.append(secondsOfDay(v._4))
      })
      val timestamps = stampBuffer.toArray.sorted
      // 设置带宽h，单位为秒
      val h = 1800
      // 计算局部密度
      val density_stamp_Buffer = new ArrayBuffer[(Double, Long)]()
      for (t <- timestamps) {
        var temp = 0D
        for (v <- timestamps) {
          temp += RBF(v, t, h)
        }
        density_stamp_Buffer.append((temp / (timestamps.length * h), t))
      }
      val density_stamp = density_stamp_Buffer.toArray.sortBy(_._2)

      // 判断是否存在聚类中心，若返回为空则不存在，否则分类
      val cluster_center = z_score(density_stamp)
//      cluster_center.foreach(x => println(x._1.toString + '\t' + (x._2/3600).toString + ":" + (x._2%3600/60).toString + ":" + (x._2%3600%60).toString))
//      println("DaySets:" + daySets.toArray.sorted.mkString(","))

      // 设置类边界距离
      val dc = 5400
      // 初始化类簇,结构为[所属类，出行片段]
      val clusters = new ArrayBuffer[(Int, (Long, String, Int, Long, String, Int, Int))]
      for (v <- pairs) {
        if (cluster_center.nonEmpty) {
          val o_stamp = secondsOfDay(v._1)
          val d_stamp = secondsOfDay(v._4)
          val o_to_c = distAndKinds(Long.MaxValue, 0)
          val d_to_c = distAndKinds(Long.MaxValue, 0)
          for (c <- cluster_center) {
            if (abs(o_stamp - c._2) < dc && abs(o_stamp - c._2) < o_to_c.d){
              o_to_c.k = c._1
              o_to_c.d = abs(o_stamp - c._2)
            }
            if (abs(d_stamp - c._2) < dc && abs(d_stamp - c._2) < d_to_c.d){
              d_to_c.k = c._1
              d_to_c.d = abs(d_stamp - c._2)
            }
          }
          if (o_to_c.k == d_to_c.k && o_to_c.k != 0)
            clusters.append(( o_to_c.k, v))
//          else
//            clusters.append((0, v))
        }
//        else
//          clusters.append((0,v))
      }
      val grouped = clusters.groupBy(_._1).toArray
      // 存储AP的pattern
      val ap_patterns = new ArrayBuffer[(String, String)]()
      grouped.foreach(g => {
        val pairNum = g._2.size
        // 控制保留出现次数最多的p个站点
        val p = 2
        // 控制保留停留时间最长的q个站点
        val q = 2
        val osBuffer = new ArrayBuffer[(String, Long)]()
        val dsBuffer = new ArrayBuffer[(String, Long)]()
        g._2.foreach(x => {
          osBuffer.append((x._2._2, x._2._3))
          dsBuffer.append((x._2._5, x._2._6))
        })
        val osArray = osBuffer.groupBy(_._1).mapValues(x => {
          var sum = 0
          x.foreach(v => sum += v._2)
          (x.size, sum)
        }).toArray
        val top_os : mutable.Set[String] = mutable.Set()
        // 保存起始站点出现次数最多的p个站点
        osArray.sortBy(_._2._1).takeRight(p).foreach(x => top_os.add(x._1))
        // 保存起始站点停留时间最长的p个站点
        osArray.sortBy(_._2._2).takeRight(p).foreach(x => top_os.add(x._1))

        val dsArray = dsBuffer.groupBy(_._1).mapValues(x => {
          var sum = 0
          x.foreach(v => sum += v._2)
          (x.size, sum)
        }).toArray
        val top_ds : mutable.Set[String] = mutable.Set()
        // 保存目的站点出现次数最多的q个站点
        dsArray.sortBy(_._2._1).takeRight(q).foreach(x => top_ds.add(x._1))
        // 保存目的站点停留时间最长的q个站点
        dsArray.sortBy(_._2._2).takeRight(q).foreach(x => top_ds.add(x._1))

        // 覆盖同类出行中片段的比例cover
        val coverThreshold = 0.5
        val scoreBuffer = new ArrayBuffer[(String, String, Float)]()
        for (pick_o <- top_os; pick_d <- top_ds) {
          var flag = true
          var count = 0f
          val paths = validPathMap.value((pick_o, pick_d))
          // 对当前类簇中的每一条出行片段
          for (pair <- g._2) {
            // 从可能为出行模式的有效路径中查找是否能覆盖当前出行片段
            for (path <- paths if flag) {
              if (path.indexOf(pair._2._2) >= 0 && path.indexOf(pair._2._5) > path.indexOf(pair._2._2)){
                count += 1
                flag = false
              }
            }
            flag = true
          }
          scoreBuffer.append((pick_o, pick_d, count / pairNum))
        }
        val most_cover = scoreBuffer.maxBy(_._3)
        if (most_cover._3 > coverThreshold)
          ap_patterns.append((most_cover._1, most_cover._2))
      })



    })

//    APPatterns.collect().foreach(x => {
//      x.foreach(x => {
//        println(x._1)
//        x._2.foreach(v => println(transTimeToString(v._2._1) + ',' + v._2._2 + ',' + transTimeToString(v._2._4) + ',' + v._2._5))
//      })
//      println()
//    })

    spark.stop()
  }

  // 高斯核函数
  def RBF(l : Long, x : Long, h: Int) : Double = {
    1 / sqrt(2 * Pi) * exp(-pow(x - l, 2) / (2 * pow(h, 2)))
  }

  // 计算z_score自动选取聚类中心
  def z_score(dens_pos : Array[(Double, Long)]) : Array[(Int, Long)] = {
    val dist_r = compute_dist(dens_pos)
    val dist_l = compute_dist(dens_pos.reverse).reverse
    val dist_dens_pos = new ArrayBuffer[(Long, Double, Long)]()
    for (i <- dist_r.indices) {
      if (dist_r(i) == -1 && dist_l(i) == -1)
        dist_dens_pos.append((dens_pos.last._2 - dens_pos.head._2, dens_pos(i)._1, dens_pos(i)._2))
      else if (dist_r(i) != -1 && dist_l(i) != -1)
        dist_dens_pos.append((min(dist_r(i), dist_l(i)), dens_pos(i)._1, dens_pos(i)._2))
      else if (dist_l(i) != -1)
        dist_dens_pos.append((dist_l(i), dens_pos(i)._1, dens_pos(i)._2))
      else
        dist_dens_pos.append((dist_r(i), dens_pos(i)._1, dens_pos(i)._2))
    }
    var sum_dist = 0l
    var sum_dens = 0d
    dist_dens_pos.foreach(x => {
      sum_dist += x._1
      sum_dens += x._2
    })
    val avg_dist = sum_dist / dist_dens_pos.length
    val avg_dens = sum_dens / dist_dens_pos.length
    var total = 0d
    for (v <- dist_dens_pos) {
      total += pow(abs(v._1 - avg_dist), 2) + pow(abs(v._2 - avg_dens), 2)
    }
    val sd = sqrt(total / dist_dens_pos.length)
    val z_score = new ArrayBuffer[((Long, Double, Long), Double)]()
    var z_value = 0d
    for (v <- dist_dens_pos) {
      z_value = sqrt(pow(abs(v._1 - avg_dist), 2) + pow(abs(v._2 - avg_dens), 2)) / sd
      z_score.append((v, z_value))
    }
    val result = new ArrayBuffer[(Int, Long)]()
    val clustersInfo = z_score.toArray.filter(_._2 >= 3)
    for (i <- clustersInfo.indices) {
      result.append((i+1, clustersInfo(i)._1._3))
    }
    result.toArray
  }

  // 计算相对距离
  def compute_dist(info : Array[(Double, Long)]) : Array[Long] = {
    val result = new Array[Long](info.length)
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
