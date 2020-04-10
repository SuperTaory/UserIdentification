import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import GeneralFunctionSets.transTimeToTimestamp
import GeneralFunctionSets.dayOfMonth_long
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.{abs, max}

object MatchPerMonth {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MatchPerMonth")
    val sc = new SparkContext(conf)

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
      val des = stationNoToName.value(fields.last.toInt)
      val pathStations = new ListBuffer[String]
      fields.foreach(x => pathStations.append(stationNoToName.value(x.toInt)))
      ((sou, des), pathStations.toList)
    }).groupByKey().mapValues(_.toList).cache()

    // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
    val perODMap = sc.broadcast(validPathFile.collect().toMap)

    // 将OD之间的有效路径涵盖的站点处理为Set集合，OD-pair作为键
    val validPathStationSetRDD  = validPathFile.map(v => {
      val temp_set: mutable.Set[String] = mutable.Set()
      v._2.foreach(path => temp_set.++=(path.toSet))
      (v._1, temp_set)
    })
    val validPathStationSet = sc.broadcast(validPathStationSetRDD.collect().toMap)

//    // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
//    val perODMap = validPathFile.groupByKey().mapValues(_.toList).collect().toMap
//    // 将OD之间的有效路径涵盖的站点处理为Set集合，OD-pair作为键
//    val validPathStationSetRDD  = validPathFile.groupByKey().mapValues(v => {
//      val temp_set: mutable.Set[String] = mutable.Set()
//      v.toList.foreach(path => temp_set.++=(path.toSet))
//      temp_set
//    })
//    val validPathStationSet = sc.broadcast(validPathStationSetRDD.collect().toMap)


    // 读取最短路径的时间信息
    val shortestPath = sc.textFile(args(0) + "/liutao/AllInfo/shortpath.txt").map(line => {
      val fields = line.split(' ')
      val sou = stationNoToName.value(fields(0).toInt)
      val des = stationNoToName.value(fields(1).toInt)
      // 换算成秒
      val time = (fields(2).toFloat * 60).toLong
      ((sou, des), time)
    })
    // 转换成map便于查询
    val shortestPathTime = sc.broadcast(shortestPath.collect().toMap)

    // 读取乘客的OD记录
    val personalOD = sc.textFile(args(1)).map(line => {
      val fields = line.split(',')
      val pid = fields(0).drop(1)
      val time = transTimeFormat(fields(1))
      val station = fields(2)
      val tag = fields(3).dropRight(1)
      (pid, (time, station, tag))
    })

    // 挑选OD记录最多的部分ID
//    val countRDD = personalOD.map(x => (x._1, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)
//    val countRDD = personalOD.map(x => (x._1, dayOfMonth_long(x._2._1))).groupByKey().mapValues(_.toSet.size).filter(x => x._2 > 15 && x._2 <= 20)
//    val countRDDSet = sc.broadcast(countRDD.map(x => x._1).collect().toSet)
    // 过滤出这部分ID的OD数据
//    val AFCData = personalOD.filter(x => countRDDSet.value.contains(x._1)).groupByKey().mapValues(_.toList.sortBy(_._1))

    val AFCData = personalOD.groupByKey().mapValues(v => {
      val data = v.toList.sortBy(_._1)
      val daySets = data.map(x => dayOfMonth_long(x._1)).toSet
      (data, daySets)
    }).filter(x => x._2._2.size >= 15)

    // 共享为广播变量
    val broadcastAFCData = sc.broadcast(AFCData.collect())

    /*----------------------------------------------------------------------------------------------------------------*/

    // 读取mac数据
    val macFile = sc.textFile(args(2)).map( line => {
      val fields = line.split(',')
      val macId = fields(0).drop(1)
      val time = transTimeToTimestamp(fields(1))
      val station = fields(2)
      (macId, (time, station))
    })
    val APData = macFile.groupByKey().mapValues(v => {
      val data = v.toList.sortBy(_._1)
      val daySets = data.map(x => dayOfMonth_long(x._1)).toSet
      (data, daySets)
    }).filter(x => x._2._1.length > 20)

    /*----------------------------------------------------------------------------------------------------------------*/
    // 将AFC数据和AP数据融合

    // 通过广播变量和flatMap结合替代shuffle过程，避免OOM
    val AFCAndAP = APData.flatMap(line => {
      for (v <- broadcastAFCData.value) yield {
        (v, line)
      }
    })

    // 过滤掉共同出现天数小于AFC天数一半的组合
    val filterProcess = AFCAndAP.filter(line => {
      val afcDays = line._1._2._2
      val apDays = line._2._2._2
      val inter = afcDays.intersect(apDays).size
      if (inter / afcDays.size.toFloat > 0.7)
        true
      else
        false
    })

    val matchProcessing = filterProcess.map(line => {
      val macId = line._2._1
      val ODId = line._1._1
      val macArray = line._2._2._1
      val ODArray = line._1._2._1
      var score = 0f
      var index = 0
      while (index + 1 < ODArray.length) {
        if (ODArray(index)._3 == "21" && ODArray(index + 1)._3 == "22" && ODArray(index + 1)._1 - ODArray(index)._1 < 10800) {
          val so = ODArray(index)._2
          val sd = ODArray(index + 1)._2
          val to = ODArray(index)._1
          val td = ODArray(index + 1)._1
          val paths = perODMap.value((so, sd))
          val pathStationSet = validPathStationSet.value((so, sd))
          val l = macArray.indexWhere(_._1 > to - 120)
          val r = macArray.lastIndexWhere(_._1 < td + 120)
          val macStationSet: mutable.Set[String] = mutable.Set()
          if (l >= 0 && r >= l) {
            for (i <- l.to(r))
              macStationSet.add(macArray(i)._2)
            if (pathStationSet.union(macStationSet).size == pathStationSet.size) {
              var temp_score = 0f
              var index_mac = l
              for (path <- paths) {
                var path_score = 0f
                val coincideList = new ListBuffer[Int]
                if (path.toSet.union(macStationSet).size == path.length){
                  for (station <- path if index_mac <= r) {
                    if (macArray(index_mac)._2.equals(station)) {
                      index_mac += 1
                      coincideList.append(path.indexWhere(_ == station))
                    }
                  }
                  // 判断所截取Mac片段是否有多余未匹配的点,允许一个点的误差
                  if (coincideList.nonEmpty && r - index_mac <= 1){
                    // 判断所截取并匹配的片段的起始和结束时间是否合理
                    val realTime1 = abs(to - macArray(l)._1)
                    val theoreticalTime1 = shortestPathTime.value((so, path(coincideList.head)))
                    val realTime2 = abs(td - macArray(index_mac-1)._1)
                    val theoreticalTime2 = shortestPathTime.value((path(coincideList.last), sd))
                    // 通过程序统计平均误差时间为450秒，这里放宽为600秒
                    if (abs(realTime1 - theoreticalTime1) < 600 && abs(realTime2 - theoreticalTime2) < 600) {
                      // 计算最大跨度
                      if (coincideList.length == 1)
                        path_score = 1f / path.length
                      else if (coincideList.length >= 2)
                        path_score = (coincideList.last - coincideList.head + 1).toFloat / path.length
                    }
                  }
                }
                temp_score = max(temp_score, path_score)
              }
              score += temp_score
            }
          }
          index += 1
        }
        index += 1
      }
      // 生成每对AFC记录和AP记录的每月相似度
      (ODId, (macId, score.formatted("%.3f").toFloat, ODArray.length / 2))
    }).filter(_._2._2 > 10).groupByKey().mapValues(_.toList.sortBy(_._2).reverse.take(1))


    val result = matchProcessing.flatMap(line => {
      for (v <- line._2) yield
        (line._1, v._1, v._2, v._3)
    }).repartition(1)

    result.saveAsTextFile(args(3))

    sc.stop()
  }

  def transTimeFormat(timeString : String) : Long = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val time = dateFormat.parse(timeString).getTime / 1000
    time
  }
}
