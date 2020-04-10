import GeneralFunctionSets.{transTimeToTimestamp, dayOfMonth_long}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * AP和AFC数据统计对比
 * 包括出行天数、出行次数、花费时间三部分对比
 * AFC:(292870821,2019-06-15 21:12:46,科学馆,2019-06-15 21:27:04,市民中心)
 * AP:(1C151FD38BD0,2019-06-19 22:56:37,老街,378,2019-06-19 23:31:22,下沙,121)
 */
object AfcAndApContrast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AfcAndApContrast")
    val sc = new SparkContext(conf)

    // 读取深圳通卡数据
    val subwayFile = sc.textFile(args(0)).map(line => {
      val fields = line.split(',')
      val id = fields(0).drop(1)
      val ot = transTimeToTimestamp(fields(1))
      val dt = transTimeToTimestamp(fields(3))
      val dur = dt - ot
      val day = dayOfMonth_long(ot)
      (id, (dur, day))
    }).groupByKey().mapValues(_.toList)

    val processingAFC = subwayFile.map(line => {
      val daySets : mutable.Set[Int] = mutable.Set()
      val durs = new ListBuffer[Long]
      line._2.foreach(x => {
        daySets.add(x._2)
        durs.append(x._1)
      })
      // (id，出行花费时间序列，出行次数，出行天数)
      (line._1, durs.toList, durs.length, daySets.size)
    }).cache()

    // 统计出行片段的时间长度分布,10s为一个单位
    val travelTimeLengthAFC = processingAFC.flatMap(line => for (v <- line._2) yield (v / 120, 1))
      .reduceByKey(_+_)
      .repartition(1)
      .sortByKey()
      .map(x => x._1.toString + "," + x._2.toString)
    travelTimeLengthAFC.saveAsTextFile(args(1) + "/AFC-TimeLength")

    // 统计出行次数分布
    val travelNumAFC = processingAFC.map(x => (x._3, 1))
      .reduceByKey(_+_)
      .repartition(1)
      .sortByKey()
    travelNumAFC.saveAsTextFile(args(1) + "/AFC-Num")

    // 统计出行天数分布
    val travelDaysAFC = processingAFC.map(x => (x._4, 1))
      .reduceByKey(_+_)
      .repartition(1)
      .sortByKey()
    travelDaysAFC.saveAsTextFile(args(1) + "/AFC-Days")

    // AP数据格式:(000000000000,2019-06-01 10:38:05,布吉,0,2019-06-01 10:43:50,上水径,15)
    val macFile = sc.textFile(args(2)).map(line => {
      val fields = line.split(",")
      val id = fields(0).drop(1)
      val ot = transTimeToTimestamp(fields(1))
      val o_stay = fields(3).toInt
      val dt = transTimeToTimestamp(fields(4))
      val dur = dt - ot
      val day = dayOfMonth_long(ot)
      (id, (dur, day))
    })

    // 过滤掉出行片段时间超过3小时和小于0的数据
    val filteringData = macFile.filter(x => x._2._1 > 0 && x._2._1 < 10800)
      .groupByKey()
      .mapValues(_.toList)

    val processingAP = filteringData.map(line => {
      val daySets : mutable.Set[Int] = mutable.Set()
      val durList = new ListBuffer[Long]
      line._2.foreach(x => {
        daySets.add(x._2)
        durList.append(x._1)
      })
      // (id，出行花费时间序列，出行次数，出行天数)
      (line._1, durList.toList, durList.length, daySets.size)
    }).cache()


    // 统计出行片段的时间长度分布
    val travelTimeLengthAP = processingAP.flatMap(line => for (v <- line._2) yield (v / 120, 1))
      .reduceByKey(_+_)
      .repartition(1)
      .sortByKey()
      .map(x => x._1.toString + "," + x._2.toString)
    travelTimeLengthAP.saveAsTextFile(args(1) + "/AP-TimeLength")

    // 统计出行次数分布
    val travelNumAP = processingAP.map(x => (x._3, 1))
      .reduceByKey(_+_)
      .repartition(1)
      .sortByKey()
    travelNumAP.saveAsTextFile(args(1) + "/AP-Num")

    // 统计出行天数分布
    val travelDaysAP = processingAP.map(x => (x._4, 1))
      .reduceByKey(_+_)
      .repartition(1)
      .sortByKey()
    travelDaysAP.saveAsTextFile(args(1) + "/AP-Days")

//    val sumAFC = countDaysOfAFC.map(_._2).sum()
//    val sumAP = countDaysOfAP.map(_._2).sum()
//
//    println("sumAFC:" + sumAFC.toString)
//    println("sumAP:" + sumAP.toString)

    sc.stop()
  }
}
