import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.abs
import GeneralFunctionSets.dayOfMonth_long

object NormalMacData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NormalMacData")
    val sc = new SparkContext(conf)

    val readODTimeInterval = sc.textFile(args(0)).map(line => {
      val p = line.split(',')
      val sou = p(0).drop(1)
      val des = p(1)
      val interval = p(2).dropRight(1).toLong
      ((sou, des), interval)
    })
    val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

    val macFile = sc.textFile(args(1)).map(line => {
      val fields = line.split(',')
      val macId = fields(0).drop(1)
      val time = fields(1).toLong
      val station = fields(2).dropRight(1)
      (macId, (time, station))
    }).groupByKey().mapValues(_.toList.sortBy(_._1))

    // 去除数据量极大和极小的数据
//    val groupedMacData = macFile.groupByKey().filter(v => v._2.size > 5 && v._2.size < 3000).mapValues(_.toList.sortBy(_._1))

    // 去除掉出行片段长度小于m以及出行天数小于n天的AP数据
    val filtteredMacData = macFile.map(line => {
      // 设置出行片段长度阈值
      val m = 1
      val MacId = line._1
      val data = line._2
      val segement = new ListBuffer[(Long, String)]
      val segements = new ListBuffer[(Long, String)]
      // 存储出行的日期
      val daySets: mutable.Set[Int] = mutable.Set()
      for (s <- data) {
        if (segement.isEmpty) {
          segement.append(s)
        }
        else {
          // 遇到前后相邻为同一站点进行划分
          if (s._2 == segement.last._2){
            if (segement.length > m) {
//              segements.append(segement.toList)
              segements ++= segement
              daySets.add(dayOfMonth_long(segement.head._1))
            }
            segement.clear()
          }
          // 前后相邻站点相差时间超过阈值进行划分
          else if (abs(s._1 - segement.last._1) > ODIntervalMap.value((segement.last._2, s._2)) + 1500) {
            if (segement.length > m) {
//              segements.append(segement.toList)
              segements ++= segement
              daySets.add(dayOfMonth_long(segement.head._1))
            }
            segement.clear()
          }
          // 前后相邻站点相差时间小于阈值进行划分
          else if (abs(s._1 - segement.last._1) < ODIntervalMap.value((segement.last._2, s._2)) * 0.6){
            if (segement.length > m) {
//              segements.append(segement.toList)
              segements ++= segement
              daySets.add(dayOfMonth_long(segement.head._1))
            }
            segement.clear()
          }
          segement.append(s)
        }
      }
      if (segement.length > m) {
//        segements.append(segement.toList)
        segements ++= segement
        daySets.add(dayOfMonth_long(segement.head._1))
      }
      (MacId, segements, daySets.size)
    })

    // 过滤掉出行天数小于一定值的数据
    val result = filtteredMacData.filter(_._3 > 3).flatMap(line => {
      val id = line._1
      for (s <- line._2) yield{
        (id, s._1, s._2)
      }
    })
    result.saveAsTextFile(args(2))

//    val flattenMacData = groupedMacData.flatMap(line => {
//
//      val stationSet : mutable.Set[String] = mutable.Set()
//      line._2.foreach(x => stationSet.add(x._2))
//      for (v <- line._2) yield {
//        (line._1, v._1, v._2, stationSet.size)
//      }
//    })
//    val resultRDD = flattenMacData.filter(_._4 > 1).map(line => (line._1, line._2, line._3))


//    val sortedMacData = flattenMacData.sortBy(x => (x._1, x._2), ascending = true)

//    resultRDD.saveAsTextFile(args(2))
    sc.stop()
  }
}
