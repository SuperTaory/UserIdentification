import GeneralFunctionSets.{dayOfMonth_long, secondsOfDay, transTimeToString, transTimeToTimestamp}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.{Pi, abs, exp, min, pow, sqrt}

/**
 * User identification
 */
object Model {

  case class distAndKinds(var d:Long, var k:Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Matching Model")
      .getOrCreate()
    val sc = spark.sparkContext

    // 读取地铁站点名和编号映射关系
    val stationFile = sc.textFile(args(0) + "/liutao/AllInfo/stationInfo-UTF-8.txt")
    val stationNoToNameRDD = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationNo.toInt, stationName)
    })
    val stationNoToName = sc.broadcast(stationNoToNameRDD.collect().toMap)


    // 读取所有有效路径的数据
    val validPathFile = sc.textFile(args(0) + "/liutao/AllInfo/allpath.txt").map(line => {
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

    // 读取站间时间间隔
    val readODTimeInterval = sc.textFile(args(0) + "/liutao/UI/AllODTimeInterval/ShortPathTime/part-00000").map(line => {
      val p = line.split(',')
      val sou = p(0).drop(1)
      val des = p(1)
      val interval = p(2).dropRight(1).toLong
      ((sou, des), interval)
    })
    val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

    // Pre-processing
    // 读取AFC数据: (020798332,2019-06-24 10:06:50,碧海湾,2019-06-24 10:25:09,桥头)
    val AFCFile = sc.textFile(args(0) + "/liutao/UI/SampledAFCData/part-00021").map(line => {
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

    // 划分AFC,仅保留出行天数大于5天的数据
    val AFCPartitions = AFCFile.groupByKey().map(line => {
      val dataArray = line._2.toList.sortBy(_._1)
      val daySets : mutable.Set[Int] = mutable.Set()
      dataArray.foreach(x => daySets.add(x._5))
      (line._1, dataArray, daySets)
    }).filter(_._3.size > 5)
//    val AFCPartitions = AFCFile.groupByKey().mapValues(_.toList.sortBy(_._1)).filter(_._2.length > 10)


    // AFC模式提取-基于核密度估计的聚类
    val AFCPatterns = AFCPartitions.map(line => {
      val pairs = line._2
      val daySets = line._3

      // 统计主要站点-进出站出现次数最多的站点
      val stationCount = new ArrayBuffer[String]()
      pairs.foreach(x => {
        stationCount.append(x._2)
        stationCount.append(x._4)
      })
      // 控制保存主要站点的个数
      val Q = 2
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
          else
            clusters.append((0, v))
        }
        else
          clusters.append((0,v))
      }
      // 按照所属类别分组
      val grouped = clusters.groupBy(_._1).toArray.filter(x => x._1 > 0)
      // 存储出行模式集合
      val afc_patterns = new ArrayBuffer[(Int, (String, String))]()
      if (grouped.nonEmpty){
        grouped.foreach(g => {
          // 同一类中数据按照进出站分组
          val temp_data = g._2.toArray.groupBy(x => (x._2._2, x._2._4))
          temp_data.foreach(v => {
            // 超过总出行天数的1/2则视为出行模式
            if ( v._2.length >= 5 || v._2.length > daySets.size / 2) {
              afc_patterns.append((g._1, v._1))
            }
          })
        })
      }

      // id、出行片段集合、出行模式集合、主要站点集合、出行日期集合
      (daySets.size, (line._1, pairs, afc_patterns.toList, topStations.toList, daySets))
    })


    val AFCData = sc.broadcast(AFCPatterns.groupByKey().mapValues(_.toList).collect().toMap)

//    AFCPatterns.repartition(1).saveAsTextFile(args(0) + "/liutao/UI/testModel/afc")

/**
 * *************************分割线********************
 */

    // 读取AP数据:(000000000000,2019-06-01 10:38:05,布吉,0,2019-06-01 10:43:50,上水径,15)
    val APFile = sc.textFile(args(0) + "/liutao/UI/SampledAPData_n/part*").map(line => {
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
      // id、（起始时间、起始站点、停留时间、到达时间、目的站点、 停留时间、出行日期）
      (id, (ot, os, o_stay, dt, ds, d_stay, day))
    })

    // 划分AP
    val APPartitions = APFile.groupByKey().mapValues(_.toList.sortBy(_._1)).filter(_._2.length > 10)

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
      val Q = 2
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

      // 聚类中心，若返回为空则不存在，否则分类
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
          else
            clusters.append((0, v))
        }
        else
          clusters.append((0,v))
      }
      // 仅当类簇中数据个数大于5时进行出行模式的提取
      val grouped = clusters.groupBy(_._1).toArray.filter(x => x._1 > 0 && x._2.length > 5)
      // 存储AP的pattern
      val ap_patterns = new ArrayBuffer[(Int, (String, String))]()
      // 提取出行模式
      if (grouped.nonEmpty) {
        grouped.foreach(g => {
          val pairNum = g._2.size
          // 控制保留出现次数最多的p个站点
          val p = 1
          // 控制保留停留时间最长的q个站点
          val q = 1
          val osBuffer = new ArrayBuffer[(String, Long)]()
          val dsBuffer = new ArrayBuffer[(String, Long)]()
          g._2.foreach(x => {
            osBuffer.append((x._2._2, x._2._3))
            dsBuffer.append((x._2._5, x._2._6))
          })
          val osArray = osBuffer.groupBy(_._1).mapValues(x => {
            var sum = 0L
            x.foreach(v => sum += v._2)
            (x.size, sum)
          }).toArray
          val top_os : mutable.Set[String] = mutable.Set()
          // 保存起始站点出现次数最多的p个站点
          osArray.filter(_._2._1 > 1).sortBy(_._2._1).takeRight(p).foreach(x => top_os.add(x._1))
          // 保存起始站点停留时间最长的p个站点
          osArray.sortBy(_._2._2).takeRight(q).foreach(x => top_os.add(x._1))

          val dsArray = dsBuffer.groupBy(_._1).mapValues(x => {
            var sum = 0L
            x.foreach(v => sum += v._2)
            (x.size, sum)
          }).toArray
          val top_ds : mutable.Set[String] = mutable.Set()
          // 保存目的站点出现次数最多的q个站点
          dsArray.filter(_._2._1 > 1).sortBy(_._2._1).takeRight(p).foreach(x => top_ds.add(x._1))
          // 保存目的站点停留时间最长的q个站点
          dsArray.sortBy(_._2._2).takeRight(q).foreach(x => top_ds.add(x._1))

          // 计算覆盖同类出行中片段的比例cover
          val coverThreshold = 0.6
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
          // 选取覆盖度最高的OD对
          val most_cover = scoreBuffer.maxBy(_._3)
          // 若覆盖度大于阈值则加入到出行模式集合中
          if (most_cover._3 > coverThreshold)
            ap_patterns.append((g._1, (most_cover._1, most_cover._2)))
        })
      }

      // AP补全
      val complement = new ArrayBuffer[((Long, String, Int, Long, String, Int, Int), Int)]()
      if (ap_patterns.isEmpty) {
        pairs.foreach(v => complement.append((v,0)))
      }
      else {
        val patternMap = ap_patterns.toMap
        grouped.foreach(g => {
          if (patternMap.contains(g._1)) {
            var flag = true
            val pattern = patternMap(g._1)
            val paths = validPathMap.value(pattern)
            for (pair <- g._2) {
              for (p <- paths if flag) {
                val index_po = p.indexOf(pair._2._2)
                val index_pd = p.indexOf(pair._2._5)
                if (index_po >= 0 && index_pd > index_po) {
                  if (index_po == 0  && index_pd == p.length-1) {
                    flag = false
                    complement.append((pair._2, pair._1))
                  }
                  else if (index_po == 0 && index_pd != p.length-1) {
                    flag = false
                    val new_dt = pair._2._4 + ODIntervalMap.value((pair._2._5, pattern._2))
                    complement.append(((pair._2._1, pair._2._2, pair._2._3, new_dt, pattern._2, 0, pair._2._7), g._1))
                  }
                  else if (index_po != 0 && index_pd == p.length-1) {
                    flag = false
                    val new_ot = pair._2._1 - ODIntervalMap.value((pattern._1, pair._2._2))
                    complement.append(((new_ot, pattern._1, 0, pair._2._4, pair._2._5, pair._2._6, pair._2._7), g._1))
                  }
                  else if (index_po != 0 && index_pd != p.length-1){
                    flag = false
                    val new_ot = pair._2._1 - ODIntervalMap.value((pattern._1, pair._2._2))
                    val new_dt = pair._2._4 + ODIntervalMap.value((pair._2._5, pattern._2))
                    complement.append(((new_ot, pattern._1, 0, new_dt, pattern._2, 0, pair._2._7), g._1))
                  }
                }
              }
              if (!flag)
                flag = true
              else {
                // 不属于此出行模式，遂未补全
                complement.append((pair._2, 0))
              }
            }
          }
          else {
            // 当前类簇中的出行片段不存在出行模式
            g._2.foreach(line => complement.append((line._2, 0)))
          }
        })
      }

      (line._1, complement.toList, ap_patterns.toList, topStations.toList, daySets)
    }).cache()
//    APPatterns.repartition(1).saveAsTextFile(args(0) + "/liutao/UI/testModel/ap")



    // 轨迹筛选与匹配
    val mergeData = APPatterns.filter(_._5.size > 3).flatMap(ap => {
      val floatingDays = 0
      val start = ap._5.size - floatingDays
      val candidateDays = AFCData.value.keys.toSet.filter(x => x >= start)
      for (i <- candidateDays; afc <- AFCData.value(i)) yield{
        (ap, afc)
      }
    }).filter(x => x._1._5.intersect(x._2._5).size > 2)

    val matchData = mergeData.map(line => {
      var score = 0d
      val ap = line._1._2.sortBy(_._1._1)
      val afc = line._2._2.sortBy(_._1)
      val ap_patterns = line._1._3
      val afc_patterns = line._2._3
      val tr_ap_afc = new ArrayBuffer[(Int, Int)]()
      val tr_ap = new ArrayBuffer[Int]()
      val tr_afc = new ArrayBuffer[Int]()
      var index_ap = 0
      var index_afc = 0
      while (index_ap < ap.length && index_afc < afc.length) {
        val cur_ap = ap(index_ap)._1
        val cur_afc = afc(index_afc)
        if (cur_ap._4 < cur_afc._1) {
          tr_ap.append(index_ap)
          index_ap += 1
        }
        else if (cur_ap._1 > cur_afc._3) {
          tr_afc.append(index_afc)
          index_afc += 1
        }
        else if (cur_ap._1 > cur_afc._1 - 300 && cur_ap._4 < cur_afc._3 + 300){
          val paths = validPathMap.value((cur_afc._2, cur_afc._4))
          var flag = true
          for (p <- paths if flag) {
            if (p.indexOf(cur_ap._2) >= 0 && p.indexOf(cur_ap._5) > p.indexOf(cur_ap._2)) {
              if (abs(cur_afc._1 + ODIntervalMap.value(p.head, cur_ap._2) - cur_ap._1) < 300) {
                if (abs(cur_ap._4 + ODIntervalMap.value(cur_ap._5, p.last) - cur_afc._3) < 300) {
                  flag = false
                  tr_ap_afc.append((index_ap, index_afc))
                  index_afc += 1
                  index_ap += 1
                }
              }
            }
          }
          if (flag) {
            // 跳出while循环
            index_afc = afc.length
            index_ap = ap.length
            tr_ap.clear()
            tr_afc.clear()
            tr_ap_afc.clear()
          }
        }
        else {
          index_afc = afc.length
          index_ap = ap.length
          tr_ap.clear()
          tr_afc.clear()
          tr_ap_afc.clear()
        }
      }

      // 分为三类完毕,开始计算相似度
      var score_tr1 = 0d
      var score_tr2 = 0d
      var score_tr3 = 0d
      val n = 0.1
      if (tr_ap_afc.nonEmpty) {
        val prepare = new ArrayBuffer[((String, String),((Long, Long), (Long, Long)))]()
        for (pair <- tr_ap_afc) {
          val ap_pair = ap(pair._1)._1
          val afc_pair = afc(pair._2)
          val afc_od = (afc_pair._2, afc_pair._4)
          prepare.append((afc_od, ((ap_pair._1, ap_pair._4), (afc_pair._1, afc_pair._3))))
        }
        val groupByPattern = prepare.groupBy(_._1).mapValues(line => {
          val data = line.sortBy(_._2._2._1)
          var tempScore = 0d
          for (i <- data.indices) {
            val v = data(i)._2
            val ap_length = abs(v._1._2 - v._1._1).toDouble
            val afc_length = abs(v._2._2 - v._2._1).toDouble
            tempScore += (ap_length / afc_length) / exp(n * i)
          }
          tempScore
        })
        groupByPattern.foreach(x => score_tr1 += x._2)
      }
      if (tr_ap.nonEmpty)
        score_tr2 = tr_ap.length
      if (tr_afc.nonEmpty)
        score_tr3 = tr_afc.length
      val x1 = 2
      val x2 = 0.1
      val x3 = 0.1
      if (tr_afc.nonEmpty && tr_ap.nonEmpty && tr_ap_afc.nonEmpty)
        score = (x1 * score_tr1 + x2 * score_tr2 + x3 * score_tr3) / (tr_ap_afc.length + tr_ap.length + tr_afc.length)
      (line._2._1, (line._1._1, score_tr1))
    })

    val matchResult = matchData.groupByKey().map(line => {
      val mostMatchList = line._2.toList.sortBy(_._2).takeRight(3)
      (line._1, mostMatchList.last, mostMatchList(1), mostMatchList.head)
    })

    matchResult.repartition(1).sortBy(_._2._2, ascending = false).saveAsTextFile(args(0) + "/liutao/UI/testModel/matchResult_21")


//    println("---------------:" + AFCPatterns.count())
//    println("---------------:" + APPatterns.count())
//    println("---------------:" + mergeAndMatch.count())

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
    var sum_dist = 0L
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
    // z-score大于3认为是类簇中心
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
