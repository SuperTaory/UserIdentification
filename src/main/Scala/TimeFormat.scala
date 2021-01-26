import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.{SparkConf, SparkContext}

object TimeFormat {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("TimeFormat")
        val sc = new SparkContext(conf)

        val macFile = sc.textFile(args(0)).map(line => {
            val fields = line.split(',')
            val macId = fields(0).drop(1)
            val time = transTimeToString(fields(1).toLong)
            val station = fields(2).dropRight(1)
            (macId, time, station)
        }).sortBy(x => (x._1, x._2), ascending = true)

        macFile.saveAsTextFile(args(1))
        sc.stop()
    }

    def transTimeToString(time_tamp: Long): String = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.format(time_tamp * 1000)
        time
    }
}
