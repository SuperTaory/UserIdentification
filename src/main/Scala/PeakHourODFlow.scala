import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.spark.sql.SparkSession

object PeakHourODFlow {
    def main(args: Array[String]): Unit = {
        /**
         * 统计早高峰od之间每 5 min的流量
         */
        //        val l = "2019-06-04 08:42:22"
        //        print(time_flag(l))
        val spark = SparkSession.builder()
            .appName("PeakHourODFlow")
            .getOrCreate()
        val sc = spark.sparkContext

        // (667979926,2019-06-04 08:42:22,坪洲,21,2019-06-04 08:55:23,宝安中心,22)
        val afcData = sc.textFile(args(0) + "Destination/subway-pair/part-*").map(line => {
            val fields = line.split(",")
            val os = fields(2)
            val ds = fields(5)
            val ot = time_flag(fields(1))
            ((os, ds), ot)
        }).filter(_._2 >= 0).groupByKey().mapValues(_.toList.groupBy(x => x).mapValues(_.size))

        val res = afcData.map(line => {
            val flows = Array.ofDim[Int](720)
            for (i <- 0.until(720)) {
                flows(i) = line._2.getOrElse(i, 0)
            }
            val s = flows.sum
            (line._1._1 + "," + line._1._2 + "," + flows.mkString(","), s)
        })
        res.repartition(1).sortBy(_._2, ascending = false).map(_._1).saveAsTextFile(args(0) + "zlt/UI/PeakHourODFlow")
        sc.stop()
    }

    def time_flag(timeString: String): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        val d = calendar.get(Calendar.DAY_OF_MONTH)
        val h = calendar.get(Calendar.HOUR_OF_DAY)
        val m = calendar.get(Calendar.MINUTE)
        if (h != 7 & h != 8)
            -1
        else {
            m / 5 + (h - 7) * 12 + 24 * (d - 1)
        }
    }
}
