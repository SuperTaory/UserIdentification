import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.math.{abs, min}

object BuildMacPath {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("BuildMacPath")
        val sc = new SparkContext(conf)
        val macIDSet = Set("4404441BB8D9", "38295AF24259", "E0A3AC6A91CA", "E44790737D0E", "9CFBD574E912",
            "B0E235243A91", "94D029DB6E99", "808A8B7BB903", "8C4500B05D5F", "88D50C791F57", "6C72E73DE051")

        val macFile = sc.textFile(args(0))
        val macRDD = macFile.filter(line => macIDSet.contains(line.substring(1, 13))).repartition(1)
            .map(line => {
                val fields = line.split(',')
                val macID = fields(0).drop(1)
                val time = fields(1)
                val station = fields(2).dropRight(1)
                (macID, (time, station))
            }).sortBy(x => (x._1, x._2._1), ascending = true)
        macRDD.saveAsTextFile(args(1))

        val processedRDD = macRDD.groupByKey().mapValues(_.toList).map(line => {
            val macId = line._1
            val cluster1 = new ListBuffer[(Long, String)]
            val cluster2 = new ListBuffer[(Long, String)]
            val cluster3 = new ListBuffer[(Long, String)]
            line._2.foreach(x => {
                val re = classification(x._1)
                if (re._1 == 1) {
                    cluster1.append((re._2, x._2))
                } else if (re._1 == 2) {
                    cluster2.append((re._2, x._2))
                } else {
                    cluster3.append((re._2, x._2))
                }
            })
            val new_records = new ListBuffer[String]
            var total = 0
            var n = 0
            for (i <- 1.until(4)) {
                var records: Map[String, List[(Long, String)]] = Map()
                if (i == 1) {
                    records = cluster1.toList.groupBy(_._2)
                } else if (i == 2) {
                    records = cluster2.toList.groupBy(_._2)
                } else {
                    records = cluster3.toList.groupBy(_._2)
                }
                total += records.size
                for (x <- records) {
                    if (x._2.length >= 5) {
                        n += 1
                        val station = x._1
                        val count = x._2.length
                        val averageTime = AverageTime(x._2)
                        new_records.append(averageTime + '/' + station + '/' + count.toString + ',')
                    }
                }
            }
            (macId, total, n, new_records.toList.sorted.reduce(_ + _).dropRight(1))
        })

        processedRDD.repartition(1).saveAsTextFile(args(2))

        sc.stop()
    }

    def classification(timeString: String): (Int, Long) = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        val daytime = (calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE)) * 60 + calendar.get(Calendar.SECOND)
        val t1 = abs(daytime - 28800) // 8h
        val t2 = abs(daytime - 46800) // 13h
        val t3 = abs(daytime - 68400) // 19h
        var x = 0
        val m = min(t1, min(t2, t3))
        if (m == t1) {
            x = 1
        } else if (m == t2) {
            x = 2
        } else {
            x = 3
        }
        (x, daytime)
    }


    def AverageTime(tuples: List[(Long, String)]): String = {
        // "1559318400"为"2019/06/01 00:00:00"对应的时间戳
        val baseTime = 1559318400
        val temp = new ListBuffer[Long]
        tuples.foreach(x => temp.append(x._1))
        val average_time = (temp.sum - temp.min - temp.max) / (temp.length - 2) + baseTime
        transTimeToString(average_time)
    }

    def transTimeToString(time_tamp: Long): String = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.format(time_tamp * 1000)
        time
    }
}
