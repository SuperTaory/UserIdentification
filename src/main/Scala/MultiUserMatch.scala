import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.max

object MultiUserMatch {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MultiUserMatch")
    val sc = new SparkContext(conf)

    // 读取地铁站点名和编号映射关系
    val stationFile = sc.textFile(args(0))
    val stationNoToName = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationNo.toInt, stationName)
    }).collect().toMap

//    stationNoToName.foreach(println)

    val stationNameToNo = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationName, stationNo.toInt)
    }).collect().toMap

    // 读取所有有效路径的数据
    val validPathFile = sc.textFile(args(1)).map(line => {
      // 仅保留站点编号信息
      val fields = line.split(' ').dropRight(5)
      val sou = stationNoToName(fields(0).toInt)
      val des = stationNoToName(fields(fields.length-1).toInt)
      val pathStations = new ListBuffer[String]
      fields.foreach(x => pathStations.append(stationNoToName(x.toInt)))
      ((sou, des), pathStations.toList)
    }).cache()

    // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
    val perODMap = validPathFile.groupByKey().mapValues(_.toList).collect().toMap
    // 将OD之间的有效路径涵盖的站点处理为Set集合，OD-pair作为键
    var validPathStationSet  = validPathFile.groupByKey().mapValues(v => {
      val temp_set: mutable.Set[String] = mutable.Set()
      v.toList.foreach(path => temp_set.++=(path.toSet))
      temp_set
    }).collect().toMap


    // 读取乘客的OD记录
    val personalOD = sc.textFile(args(2)).map(line => {
      val fields = line.split(',')
      val pid = fields(0).drop(1)
      val time = transTimeFormat(fields(1))
      val station = fields(2)
      val tag = fields(3).dropRight(1)
      val weeks = getWeek(time)
      ((pid, weeks), (time, station, tag))
    }).cache()

    val countRDD = personalOD.map(x => (x._1._1, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false).take(100)

    val countRDDSet = countRDD.map(_._1).toSet
    val selectedRDD = personalOD.filter(x => countRDDSet.contains(x._1._1))

//    val groupByPid = personalOD.groupBy(_._1._1).mapValues(_.toList.sortBy(_._2._1))
//
//    // 存储去重后的OD-pair
//    val ODPairSet: mutable.Set[(String, String)] = mutable.Set()
//    groupByPid.foreach(line => {
//      var index = 0
//      val ODArray = line._2
//      while (index + 1 < ODArray.length) {
//        if (ODArray(index)._2._3 == "21" && ODArray(index + 1)._2._3 == "22" && ODArray(index + 1)._2._1 - ODArray(index)._2._1 < 10800) {
//          // 把站点名转换为编号
//          val so = ODArray(index)._2._2
//          val sd = ODArray(index + 1)._2._2
//          ODPairSet.add((stationNameToNo(so), stationNameToNo(sd)))
//          index += 1
//        }
//        index += 1
//      }
//    })
//
//    // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
//    var perODMap : mutable.Map[(String, String), List[List[String]]] = mutable.Map()
//    // 将OD之间的有效路径涵盖的站点处理为Set集合，OD-pair作为键
//    var validPathStationSet : mutable.Map[(String, String), mutable.Set[String]] = mutable.Map()
//
//    ODPairSet.foreach(x => {
//      val od = (stationNoToName(x._1), stationNoToName(x._2))
//      val vp = validPathSet(x)
//      val paths = new ListBuffer[List[String]]
//      val temp_set: mutable.Set[String] = mutable.Set()
//      for (p <- vp) {
//        val path = new ListBuffer[String]
//        for (s <- p) {
//          path.append(stationNoToName(s))
//        }
//        paths.append(path.toList)
//        temp_set.++=(path.toSet)
//      }
//      perODMap += (od -> paths.toList)
//      validPathStationSet += (od -> temp_set)
//    })

    val groupByWeeks = selectedRDD.groupByKey().mapValues(_.toList.sortBy(_._1)).map(x => (x._1._2, (x._1._1, x._2))).
      groupByKey().mapValues(_.toList)

    val broadcastODInfo = sc.broadcast(groupByWeeks.collect())

    /*----------------------------------------------------------------------------------------------------------------*/

    // 读取mac数据
    val macFile = sc.textFile(args(3)).map( line => {
      val fields = line.split(',')
      val macId = fields(0).drop(1)
      val time = fields(1).toLong
      val station = fields(2).dropRight(1)
      val weeks = getWeek(time)
      ((macId, weeks), (time, station))
    })

    val groupedMacInfo = macFile.groupByKey().mapValues(_.toList.sortBy(_._1)).map(x => (x._1._2, (x._1._1, x._2)))

    val ODInfo = broadcastODInfo.value.toMap
    val flatenODAndMac = groupedMacInfo.flatMap(line => {
      val ODWeekInfo = ODInfo(line._1)
      for (l <- ODWeekInfo) yield{
        (line._1, (l, line._2))
      }
    })


    val joinedMacAndOD = flatenODAndMac.filter(line => {
      val macArray = line._2._2._2
      val ODArray = line._2._1._2
      var flag = true
      for (a <- ODArray if flag){
        val l = macArray.indexWhere(_._1 >= a._1 - 60)
        val r = macArray.lastIndexWhere(_._1 <= a._1 + 60)
        if (l != -1 && r != -1){
          for (i <- l.to(r) if flag){
            if (macArray(i)._2 == a._2)
              flag = true
            else
              flag = false
          }
        }
      }
      flag
    })

    val rankedScoreOfWeek = joinedMacAndOD.map(line => {
      var score = 0f
      val weeks = line._1
      val macId = line._2._2._1
      val ODId = line._2._1._1
      val macArray = line._2._2._2
      val ODArray = line._2._1._2

      var index = 0
      while (index + 1 < ODArray.length) {
        if (ODArray(index)._3 == "21" && ODArray(index + 1)._3 == "22" && ODArray(index + 1)._1 - ODArray(index)._1 < 10800) {
          val so = ODArray(index)._2
          val sd = ODArray(index + 1)._2
          val to = ODArray(index)._1
          val td = ODArray(index + 1)._1
          val paths = perODMap((so, sd))
          val pathStationSet = validPathStationSet((so, sd))
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
                for (station <- path if index_mac <= r) {
                  if (macArray(index_mac)._2.equals(station)) {
                    index_mac += 1
                    coincideList.append(path.indexWhere(_ == station))
                  }
                }
                // 计算最大跨度
                if (coincideList.length == 1)
                  path_score = 1f / path.length
                else if (coincideList.length >= 2) {
                  path_score = (coincideList.last - coincideList.head + 1).toFloat / path.length
                  // 如果出现路径完整匹配则额外加一分
//                  if (coincideList.head == 0 && coincideList.last == path.length-1)
//                    if (macArray(l)._2 == path.head && macArray(r)._2 == path.last)
//                      path_score += 1
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
      ((ODId, macId), (weeks, score))
    })

    val mergeRDD = rankedScoreOfWeek.groupByKey().mapValues(_.toList.sortBy(_._1)).map(line => {
      val ODId = line._1._1
      val macId = line._1._2
      var total = 0f
      var detail = ""
      line._2.foreach(x => {
        detail += x._1.toString + '-' + x._2.toString + ','
        total += x._2
      })
      (ODId, macId, detail.dropRight(1), total)
    }).filter(_._4 > 0).sortBy(x => (x._1, x._4), ascending = false)

//    val topRDD = mergeRDD.flatMap(line => {
//      for (v <- line._2) yield { v }
//    })

//    var baseID = "null"
//    var count = 50
//    val topRDD = mergeRDD.filter(line => {
//      if (baseID == line._1 && count >= 0){
//        count -= 1
//        true
//      }
//      else if (baseID != line._1){
//        baseID = line._1
//        count = 49
//        true
//      }
//      else
//        false
//    })
    val topRDD = mergeRDD.filter(_._4 > 2)



    topRDD.saveAsTextFile(args(4))

    sc.stop()
  }

  def transTimeFormat(timeString : String) : Long = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val time = dateFormat.parse(timeString).getTime / 1000
    time
  }

  def getWeek(t : Long) : Int = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val timeString = dateFormat.format(t * 1000)
    val time = dateFormat.parse(timeString)
    val calendar = Calendar.getInstance()
    calendar.setTime(time)
    calendar.get(Calendar.WEEK_OF_MONTH)
  }
}
