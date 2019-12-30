import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.abs

object CountTimeErrorBetweenStations {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CountTimeErrorBetweenStations")
    val sc = new SparkContext(conf)

    // 读取地铁站点名和编号映射关系
    val stationFile = sc.textFile(args(0))
    val stationNoToNameRDD = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationNo.toInt, stationName)
    })
    val stationNoToName = sc.broadcast(stationNoToNameRDD.collect().toMap)

    // 读取最短路径的时间信息
    val shortestPath = sc.textFile(args(1)).map(line => {
      val fields = line.split(' ')
      val sou = stationNoToName.value(fields(0).toInt)
      val des = stationNoToName.value(fields(1).toInt)
      // 换算成秒
      val time = (fields(2).toFloat * 60).toLong
      ((sou, des), time)
    })

    // 转换成map便于查询
    val shortestPathTime = sc.broadcast(shortestPath.collect().toMap)

    val testData = sc.textFile(args(2)).map(line => {
      val fields = line.split(',')
      val cid = fields(0).drop(1)
      val time = transTimeFormat(fields(1))
      val station = fields(2)
      val tag = fields(3).dropRight(1)
      (cid, (time, station, tag))
    }).groupByKey().mapValues(_.toList.sortBy(_._1))

    val countOD = new LongAccumulator()
    sc.register(countOD)
    val totalTimeError = new LongAccumulator()
    sc.register(totalTimeError)

    val computeError = testData.map(line => {
      val ODArray = line._2
      var index = 0
      while (index + 1 < ODArray.length) {
        if (ODArray(index)._3 == "21" && ODArray(index + 1)._3 == "22" && ODArray(index + 1)._1 - ODArray(index)._1 < 10800) {
          countOD.add(1)
          val realTime = abs(ODArray(index)._1 - ODArray(index + 1)._1)
          val theoreticalTime = shortestPathTime.value((ODArray(index)._2, ODArray(index + 1)._2))
          totalTimeError.add(abs(realTime - theoreticalTime).toInt)
          index += 1
        }
        index += 1
      }
      (line._1, 1)
    })

    println(computeError.count())
    println(totalTimeError)
    println(countOD)
    println(totalTimeError.value / countOD.value)

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
