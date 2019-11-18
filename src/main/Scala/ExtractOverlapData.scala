import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object ExtractOverlapData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ExtractOverlapData")
    val sc = new SparkContext(conf)

    val smartCardIdSets = sc.textFile(args(0)).map(line => line.toString).collect().toSet
    val readFile = sc.textFile(args(1)).map(line => {
      val fields = line.split(',')
      val smartCardId = fields.head.drop(1)
      val tag = fields.last.dropRight(1)
      val macIDList = fields.drop(1).dropRight(1)
      (smartCardId, macIDList, tag)
    })

    val filterOverlap = readFile.filter(x => smartCardIdSets.contains(x._1) && x._3 == "C").flatMap(line => {
      val smartCardId = line._1
      val monthData = line._2.last.split('/')
      val temp = new ListBuffer[(String, String, Float)]
      for (v <- line._2.dropRight(1)){
        val week = v.split('/')(0)
        val Id = v.split('/')(1)
        val score = v.split('/')(2).toFloat
        if (score > monthData(2).toFloat / line._2.length && Id != monthData(1).toString) {
          temp.append((week, Id, score))
        }
      }
      temp.append((monthData(0), monthData(1), monthData(2).toFloat))
      for (v <- temp) yield {
        (smartCardId, v._1, v._2, v._3)
      }
    }).cache()

    filterOverlap.repartition(1).sortBy(_._1).saveAsTextFile(args(2) + "/overlapInfo")

    val ODId = filterOverlap.map(_._1).collect().toSet
    val macId = filterOverlap.map(_._3).collect().toSet
    val subwayData = sc.textFile(args(3)).filter(x => ODId.contains(x.split(',')(0).drop(1))).map(line => {
      val fields = line.split(',')
      val pid = fields(0).drop(1)
      val time = fields(1)
      val station = fields(2)
      val tag = fields(3).dropRight(1)
      (pid, time, station, tag)
    }).repartition(1).sortBy(x => (x._1, x._2))

    subwayData.saveAsTextFile(args(2) + "/ODData")

    val macdata = sc.textFile(args(4)).filter(x => macId.contains(x.split(',')(0).drop(1))).map(line => {
      val fields = line.split(',')
      val macid = fields(0).drop(1)
      val time = transTimeToString(fields(1))
      val station = fields(2).dropRight(1)
      (macid, time, station)
    }).repartition(1).sortBy(x => (x._1, x._2))

    macdata.saveAsTextFile(args(2) + "/MacData")

    sc.stop()
  }

  def transTimeToString(time_tamp : String) : String = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val time = dateFormat.format(time_tamp.toLong * 1000)
    time
  }
}
