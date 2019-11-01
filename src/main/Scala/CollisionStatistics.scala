import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.math.abs

object CollisionStatistics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CollisionStatistics")
    val sc = new SparkContext(conf)

    val smartCardRDD = sc.textFile(args(0)).filter(_.substring(1, 10) == args(1)).map(line => {
      val fields = line.split(',')
      val time = transTimeFormat(fields(1))
      val stationName = fields(2)
      (time, stationName)
    }).cache()

    val ODStationOnly = smartCardRDD.map(_._2).distinct().collect()
    val ODInfo = smartCardRDD.sortByKey(ascending = true).collect()
    val ODCount = ODInfo.length

    val macRDD = sc.textFile(args(2)).map(line => {
      val fields = line.split(',')
      val macId = fields(0).drop(1)
      val time = fields(1).toLong
      val stationName = fields(2).dropRight(1)
      (macId, (time, stationName))
    }).filter(x => ODStationOnly.contains(x._2._2)).groupByKey().mapValues(v => v.toList.sortBy(_._1))

    val collisionRDD = macRDD.map(line => {
      var count = 0
      var total = 0
      val macInfo = line._2
      val timeDifference = new ListBuffer[String]
      var scIndex = 0
      var macIndex = 0
      while (scIndex < ODInfo.length && macIndex < macInfo.length) {
        if (macInfo(macIndex)._1 < ODInfo(scIndex)._1 - 600)
          macIndex += 1
        else if (macInfo(macIndex)._1 > ODInfo(scIndex)._1 + 600)
          scIndex += 1
        else if (macInfo(macIndex)._2 == ODInfo(scIndex)._2) {
          count += 1
          val diff = abs(ODInfo(scIndex)._1 - macInfo(macIndex)._1)
          total += diff.toInt
          val m = diff / 60
          val s = diff % 60
          timeDifference.append(m.toString + 'm' + s.toString + 's')
          macIndex += 1
          scIndex += 1
        }
        else
          macIndex += 1
      }
      var detailDifference = ""
      if (timeDifference.nonEmpty)
        detailDifference = timeDifference.reduce(_ + ',' + _)
      (line._1, count, total, detailDifference)
    }).filter(_._2 != 0)
      .sortBy(x => (x._2, -x._3), ascending = false)
      .map(line => {
      val totalDifference = (line._3 / 60).toString + 'm' + (line._3 % 60).toString + 's'
      (line._1, line._2.toString + '/' + ODCount.toString, totalDifference, line._4)
    })

    collisionRDD.saveAsTextFile(args(3))
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
