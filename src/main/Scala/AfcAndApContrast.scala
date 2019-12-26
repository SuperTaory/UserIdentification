import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import GeneralFunctionSets.transTimeToTimestamp
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.abs

// AP和AFC数据乘客出行天数统计
object AfcAndApContrast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AfcAndApContrast")
    val sc = new SparkContext(conf)

    // 读取深圳通卡数据
    val subwayFile = sc.textFile(args(0)).map(line => {
      val fields = line.split(',')
      val id = fields(0).drop(1)
      val time = transTimeToTimestamp(fields(1))
      val station = fields(2)
      val tag = fields(3).dropRight(1).toInt
      (id, (time, station, tag))
    }).groupByKey().mapValues(_.toList.sortBy(_._1))

    // 统计AFC数据的出行片段的时间段分布以及出行次数，出行天数
    val dividedAFC = subwayFile.map(line => {
      val data = line._2
      // 每15min为一个时间区间，tag = 0 表示 0min - 15min，依次类推
      val timeTag = new ListBuffer[Long]
      val daySet : mutable.Set[Int] = mutable.Set()
      var index = 0
      while (index + 1 < data.length) {
        if (data(index)._2 != data(index + 1)._2 && data(index)._3 == 21 && data(index + 1)._3 == 22 && data(index + 1)._1 - data(index)._1 < 10800) {
          timeTag.append((data(index + 1)._1 - data(index)._1) / 900)
          daySet.add(dayOfMonth_long(data(index)._1))
          index += 1
        }
        index += 1
      }
      (line._1, timeTag.toList, timeTag.length, daySet.size)
    }).cache()

    // 统计出行次数分布
    val travelNumAFC = dividedAFC.map(x => (x._3, 1)).reduceByKey(_+_).repartition(1).sortByKey()
    travelNumAFC.saveAsTextFile(args(1) + "/AFC-Num")

    // 统计出行天数分布
    val travelDaysAFC = dividedAFC.map(x => (x._4, 1)).reduceByKey(_+_).repartition(1).sortByKey()
    travelDaysAFC.saveAsTextFile(args(1) + "/AFC-Days")

    // 统计出行片段的时间长度分布
    val travelTimeLengthAFC = dividedAFC.flatMap(line => for (v <- line._2) yield (v, 1)).reduceByKey(_+_).repartition(1).sortByKey()
    travelTimeLengthAFC.saveAsTextFile(args(1) + "/AFC-TimeLength")


    // 读取站间时间间隔
    val ODTimeInterval = sc.textFile(args(2)).map(line => {
      val p = line.split(',')
      val sou = p(0).drop(1)
      val des = p(1)
      val interval = p(2).dropRight(1).toLong
      ((sou, des), interval)
    })
    val ODIntervalMap = sc.broadcast(ODTimeInterval.collect().toMap)

    val macFile = sc.textFile(args(3)).map(line => {
      val fields = line.split(',')
      val macId = fields(0).drop(1)
      val time = transTimeToTimestamp(fields(1))
      val station = fields(2)
      val dur = fields(3).dropRight(1).toLong
      (macId, (time, station, dur))
    }).groupByKey().mapValues(_.toList.sortBy(_._1))

    // 划分为出行片段并统计出行片段长度分布以及出行天数和出行次数
    val dividedAP = macFile.map(line => {
      // 设置出行片段长度阈值
      val m = 2
      var count = 0
      val MacId = line._1
      val data = line._2
      val segment = new ListBuffer[(Long, String, Long)]
      val timeTag = new ListBuffer[Long]
      // 存储出行日期
      val daySets: mutable.Set[Int] = mutable.Set()
      for (s <- data) {
        if (segment.isEmpty) {
          segment.append(s)
        }
        else {
          // 遇到前后相邻为同一站点进行划分
          if (s._2 == segment.last._2){
            if (segment.length > m) {
              count += 1
              // 将出行片段中相邻的两采样点之间的时间差按每5min统计采样时间分布
              for (v <- 1 until segment.length)
                timeTag.append(abs(segment(v)._1 - segment(v-1)._1) / 300)
              daySets.add(dayOfMonth_long(segment.head._1))
            }
            segment.clear()
          }
          // 前后相邻站点相差时间超过阈值进行划分
          else if (abs(s._1 - segment.last._1) > ODIntervalMap.value((segment.last._2, s._2)) + 1500) {
            if (segment.length > m) {
              count += 1
              for (v <- 1 until segment.length)
                timeTag.append(abs(segment(v)._1 - segment(v-1)._1) / 300)
              daySets.add(dayOfMonth_long(segment.head._1))
            }
            segment.clear()
          }
          // 前后相邻站点相差时间小于阈值进行划分
          else if (abs(s._1 - segment.last._1) < ODIntervalMap.value((segment.last._2, s._2)) * 0.5){
            if (segment.length > m) {
              count += 1
              for (v <- 1 until segment.length)
                timeTag.append(abs(segment(v)._1 - segment(v-1)._1) / 300)
              daySets.add(dayOfMonth_long(segment.head._1))
            }
            segment.clear()
          }
          segment.append(s)
        }
      }
      if (segment.length > m) {
        daySets.add(dayOfMonth_long(segment.head._1))
      }
      (MacId, timeTag.toList, count, daySets.size)
    }).filter(_._3 > 0).cache()


    // 统计出行次数分布
    val travelNumAP = dividedAP.map(x => (x._3, 1)).reduceByKey(_+_).repartition(1).sortByKey()
    travelNumAP.saveAsTextFile(args(1) + "/AP-Num")

    // 统计出行天数分布
    val travelDaysAP = dividedAP.map(x => (x._4, 1)).reduceByKey(_+_).repartition(1).sortByKey()
    travelDaysAP.saveAsTextFile(args(1) + "/AP-Days")

    // 统计出行片段的时间长度分布
    val travelTimeLengthAP = dividedAP.flatMap(line => for (v <- line._2) yield (v, 1)).reduceByKey(_+_).repartition(1).sortByKey()
    travelTimeLengthAP.saveAsTextFile(args(1) + "/AP-TimeLength")


//    val sumAFC = countDaysOfAFC.map(_._2).sum()
//    val sumAP = countDaysOfAP.map(_._2).sum()
//
//    println("sumAFC:" + sumAFC.toString)
//    println("sumAP:" + sumAP.toString)

    sc.stop()
  }

  def dayOfMonth_string(timeString : String) : Int = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val time = dateFormat.parse(timeString)
    val calendar = Calendar.getInstance()
    calendar.setTime(time)
    calendar.get(Calendar.DAY_OF_MONTH)
  }

  def dayOfMonth_long(t : Long) : Int = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val timeString = dateFormat.format(t * 1000)
    val time = dateFormat.parse(timeString)
    val calendar = Calendar.getInstance()
    calendar.setTime(time)
    calendar.get(Calendar.DAY_OF_MONTH)
  }
}
