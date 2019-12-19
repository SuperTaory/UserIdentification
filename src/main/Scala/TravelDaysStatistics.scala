import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.spark.{SparkConf, SparkContext}

// AP和AFC数据乘客出行天数统计
object TravelDaysStatistics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TravelDaysStatistics")
    val sc = new SparkContext(conf)

    // 读取深圳通卡数据
    val subwayFile = sc.textFile(args(0)).map(line => {
      val fields = line.split(',')
      val id = fields(0).drop(1)
      val time = fields(1)
      (id, dayOfMonth_string(time))
    })

    // 统计每个ID的出行天数
    val travelDaysOfAFC = subwayFile.groupByKey().mapValues(v => v.toSet.size)
    val countDaysOfAFC = travelDaysOfAFC.map(x => (x._2, 1)).reduceByKey(_ + _).repartition(1).sortByKey().cache()
    countDaysOfAFC.saveAsTextFile(args(1))


    // 读取AP数据
    val macFile = sc.textFile(args(2)).map(line => {
      val fields = line.split(',')
      val id = fields(0).drop(1)
      val time = fields(1).toLong
      (id, dayOfMonth_long(time))
    })

    // 统计每个ID出行的天数
    val travelDaysOfAP = macFile.groupByKey().mapValues(v => v.toSet.size)
    val countDaysOfAP = travelDaysOfAP.map(x => (x._2, 1)).reduceByKey(_ + _).repartition(1).sortByKey().cache()
    countDaysOfAP.saveAsTextFile(args(3))

    val sumAFC = countDaysOfAFC.map(_._2).sum()
    val sumAP = countDaysOfAP.map(_._2).sum()

    println("sumAFC" + sumAFC.toString)
    println("sumAP" + sumAP.toString)

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
