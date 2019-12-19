import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.abs

object NormalMacData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NormalMacData")
    val sc = new SparkContext(conf)

    val readODTimeInterval = sc.textFile(args(0)).map(line => {
      val p = line.split(',')
      val sou = p(0).drop(1)
      val des = p(1)
      val interval = p(2).toLong
      ((sou, des), interval)
    })
    val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

    val macFile = sc.textFile(args(1)).map(line => {
      val fields = line.split(',')
      val macId = fields(0).drop(1)
      val time = fields(1).toLong
      val station = fields(2).dropRight(1)
      (macId, (time, station))
    })

    // 去除数据量极大和极小的数据
    val groupedMacData = macFile.groupByKey().filter(v => v._2.size > 5 && v._2.size < 3000).mapValues(_.toList.sortBy(_._1))

    val filtteredMacData = groupedMacData.filter(line => {
      var flag = false
      val data = line._2
      val segement = new ListBuffer[(Long, String)]
      val stationSet : mutable.Set[String] = mutable.Set()
      for (s <- data) {
        stationSet.add(s._2)
        if (segement.isEmpty){
          segement.append(s)
        }
        else {
          // 遇到前后相邻为同一站点
          if (s._2 == segement.last._2){
            if (segement.length > 1)
              flag = true
            segement.clear()
          }
          // 前后相邻站点相差时间超过阈值
          else if (abs(s._1 - segement.last._1) > ODIntervalMap.value((segement.last._2, s._2)) + 30) {
            if (segement.length > 1)
              flag = true
            segement.clear()
          }
          segement.append(s)
        }
      }
      if (segement.length > 1)
        flag = true
      flag && stationSet.size > 2
    })

    val result = filtteredMacData.flatMap(line => {
      val id = line._1
      for (v <- line._2) yield{
        (id, v._1, v._2)
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
