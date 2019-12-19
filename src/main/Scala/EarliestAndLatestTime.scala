import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.spark.{SparkConf, SparkContext}

object EarliestAndLatestTime {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("EarliestAndLatestTime")
    val sc = new SparkContext(conf)

    val dataFile = sc.textFile(args(0)).map(line => {
      val fields = line.split(",")
      val time = getDayTime(fields(1))
      (time, 1)
    }).reduceByKey(_+_)

    dataFile.collect().foreach(println)

    sc.stop()
  }


  def getDayTime(timeString : String) : Int = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val time = dateFormat.parse(timeString)
    val calendar = Calendar.getInstance()
    calendar.setTime(time)
    calendar.get(Calendar.HOUR_OF_DAY)
  }
}
