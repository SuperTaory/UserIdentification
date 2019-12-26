import org.apache.spark.{SparkConf, SparkContext}
import GeneralFunctionSets.dayOfMonth_long
import GeneralFunctionSets.transTimeToString
import GeneralFunctionSets.transTimeToTimestamp

import org.apache.spark.rdd.OrderedRDDFunctions
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.{abs, pow}
import scala.util.Random

object RandomSampling {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RandomSampling")
    val sc = new SparkContext(conf)

    // 读取站间时间间隔
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
      val time = transTimeToTimestamp(fields(1))
      val station = fields(2)
      val dur = fields(3).dropRight(1).toLong
      (macId, (time, station, dur))
    }).groupByKey().mapValues(_.toList.sortBy(_._1))

    // 划分为出行片段并标记出行日期
    val dipartition = macFile.map(line => {
      // 设置出行片段长度阈值
      val m = 2
      val MacId = line._1
      val data = line._2
      val segement = new ListBuffer[(Long, String, Long)]
      val segements = new ListBuffer[List[(Long, String, Long)]]
      // 存储出行日期
      val daySets: mutable.Set[Int] = mutable.Set()
      for (s <- data) {
        if (segement.isEmpty) {
          segement.append(s)
        }
        else {
          // 遇到前后相邻为同一站点进行划分
          if (s._2 == segement.last._2){
            if (segement.length > m) {
              segements.append(segement.toList)
              daySets.add(dayOfMonth_long(segement.head._1))
            }
            segement.clear()
          }
          // 前后相邻站点相差时间超过阈值进行划分
          else if (abs(s._1 - segement.last._1) > ODIntervalMap.value((segement.last._2, s._2)) + 1500) {
            if (segement.length > m) {
              segements.append(segement.toList)
              daySets.add(dayOfMonth_long(segement.head._1))
            }
            segement.clear()
          }
          // 前后相邻站点相差时间小于阈值进行划分
          else if (abs(s._1 - segement.last._1) < ODIntervalMap.value((segement.last._2, s._2)) * 0.5){
            if (segement.length > m) {
              segements.append(segement.toList)
              daySets.add(dayOfMonth_long(segement.head._1))
            }
            segement.clear()
          }
          segement.append(s)
        }
      }
      if (segement.length > m) {
        segements.append(segement.toList)
        daySets.add(dayOfMonth_long(segement.head._1))
      }
      (MacId, segements, daySets)
    })

//    val partitions = dipartition.repartition(1).flatMap(line => {
//      for (v <- line._2) yield {
//        val temp = new ListBuffer[(String, String)]
//        v.foreach(x => temp.append((transTimeToString(x._1), x._2)))
//        (line._1, temp)
//      }
//    })
//    partitions.saveAsTextFile(args(2))

    // 按天进行采样
    val samplingByDay = dipartition.map(line => {
      val l = line._3.size / 2
      val chosenDays = Random.shuffle(line._3).take(l)
      val sampledData = new ListBuffer[List[(Long, String, Long)]]
      for (s <- line._2){
        if (chosenDays.contains(dayOfMonth_long(s.head._1))) {
          sampledData.append(s)
        }
      }
      (line._1, sampledData, chosenDays)
    })

    // 对出行片段采样
    val samplingOnPartitions = samplingByDay.map(line => {
      val sampledData = new ListBuffer[(Long, String, Long)]
      val timeTag = new ListBuffer[Long]
      var count = line._2.length
      val days = line._3
      for (s <- line._2) {
        val tempData = new ListBuffer[((Long, String, Long), Double)]
        // 设置随机数种子seed
        val r = new Random(System.currentTimeMillis())
        var sum = 0F
        for (v <- s){
          if (v._3 < 30)
            sum += 30
          else{
            sum += v._3
          }
        }
        for (v <- s) {
          if (v._3 < 30)
            tempData.append(((v._1, v._2, v._3), pow(r.nextFloat(), sum / 30)))
          else {
            tempData.append(((v._1, v._2, v._3), pow(r.nextFloat(), sum / v._3)))
          }
        }
        val temp = tempData.sortBy(_._2).takeRight(2).toList
        temp.foreach(x => sampledData.append(x._1))
        timeTag.append(abs(temp.last._1._1 - temp.head._1._1) / 300)
      }
      (line._1, sampledData.sortBy(_._1), timeTag.toList, count, days.size)
    }).cache()

    val results = samplingOnPartitions.flatMap(line => {
      for (v <- line._2) yield
        (line._1, transTimeToString(v._1), v._2, v._3)
    }).repartition(100).sortBy(x => (x._1, x._2))

    results.saveAsTextFile(args(2))

    // 统计出行次数分布
    val travelNumAP = samplingOnPartitions.map(x => (x._4, 1)).reduceByKey(_+_).repartition(1).sortByKey()
    travelNumAP.saveAsTextFile(args(3) + "/AP-Num")

    // 统计出行天数分布
    val travelDaysAP = samplingOnPartitions.map(x => (x._5, 1)).reduceByKey(_+_).repartition(1).sortByKey()
    travelDaysAP.saveAsTextFile(args(3) + "/AP-Days")

    // 统计出行片段的时间长度分布
    val travelTimeLengthAP = samplingOnPartitions.flatMap(line => for (v <- line._3) yield (v, 1)).reduceByKey(_+_).repartition(1).sortByKey()
    travelTimeLengthAP.saveAsTextFile(args(3) + "/AP-TimeLength")

    sc.stop()
  }
}
