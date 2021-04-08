import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.abs
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

object SplitAPData {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .appName("SplitAPData")
            .getOrCreate()
        val sc = spark.sparkContext

        // args(0)设置：hdfs://compute-5-2:8020/user/zhaojuanjuan/
        val readODTimeInterval = sc.textFile(args(0) + "/zlt/UI/AllODTimeInterval/ShortPathTime/part-*").map(line => {
            val p = line.split(',')
            val sou = p(0).drop(1)
            val des = p(1)
            val interval = p(2).dropRight(1).toLong
            ((sou, des), interval)
        })
        val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

        // 读取地铁站点名和编号映射关系 "1,机场东,22.647011,113.8226476,1268036000,268"
        val stationFile = sc.textFile(args(0) + "/zlt/AllInfo/stationInfo-UTF-8.txt")
        val stationNo2NameRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationNo.toInt, stationName)
        })
        val stationNo2Name = sc.broadcast(stationNo2NameRDD.collect().toMap)

        // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"
        val validPathFile = sc.textFile(args(0) + "/zlt/AllInfo/allpath.txt").map(line => {
            val fields = line.split(' ').dropRight(5)
            val sou = stationNo2Name.value(fields(0).toInt)
            val des = stationNo2Name.value(fields(fields.length - 1).toInt)
            val path = fields.map(x => stationNo2Name.value(x.toInt))
            ((sou, des), path)
        }).groupByKey().mapValues(_.toArray)
        val validPathMap = sc.broadcast(validPathFile.collect().toMap)

        // AP数据格式：(4C49E3376FFF,2019-06-28 19:09:48,留仙洞,1)
        // AP数据路径：/zlt/UI/NormalMacData/
        val macFile = sc.textFile(args(0) + args(1)).map(line => {
            val fields = line.split(',')
            val macId = fields(0).drop(1)
            val time = transTimeToTimestamp(fields(1))
            val station = fields(2)
            // 停留时间
            val dur = fields(3).dropRight(1).toLong
            (macId, (time, station, dur))
        }).filter(x => x._2._3 < 900 & hourOfDay_long(x._2._1) >= 6  & x._1 != "000000000000")
            .groupByKey()
            .mapValues(_.toArray.sortBy(_._1))
            .filter(_._2.length > 5)

        // 划分为出行片段
        val APSegments = macFile.map(line => {
            // 设置出行片段长度阈值
            val m = 1
            val MacId = line._1
            val data = line._2
            val segment = new ListBuffer[(Long, String, Long)]
            val segments = new ListBuffer[List[(Long, String, Long)]]
            for (s <- data) {
                if (segment.isEmpty) {
                    segment.append(s)
                }
                else {
                    if (s._2 == segment.last._2) {
                        if (segment.length > m) {
                            segments.append(segment.toList)
                        }
                        segment.clear()
                    }
                    else {
                        // 根据OD时间长度设置额外容忍时间误差
                        var extra = 0
                        val odInterval = ODIntervalMap.value((segment.last._2, s._2))
                        odInterval / 1800 match {
                            case 0 => extra = 600 //10min
                            case 1 => extra = 900 //15min
                            case _ => extra = 1200 // 20min
                        }
                        val realInterval = abs(s._1 - segment.last._1 - segment.last._3)
                        if (realInterval > odInterval + extra) {
                            if (segment.length > m) {
                                segments.append(segment.toList)
                            }
                            segment.clear()
                        }
                    }
                    segment.append(s)
                }
            }
            if (segment.length > m) {
                segments.append(segment.toList)
            }
            (MacId, segments.toList)
        })

        val filterPath = APSegments.map(line => {
            val validSegments = new ArrayBuffer[Array[(Long, String, Long)]]()
            for (seg <- line._2) {
                val stations = seg.map(_._2)
                val paths = validPathMap.value((stations.head, stations.last))
                var flag = true
                // 逐条有效路径比较
                for (path <- paths if flag) {
                    var i = 0
                    var j = 0
                    while (i < stations.length & j < path.length) {
                        if (stations(i) == path(j)){
                            i += 1
                            j += 1
                        }
                        else
                            j += 1
                    }
                    if (i == stations.length)
                        flag = false
                }

                if (!flag)
                    validSegments.append(seg.toArray)
            }
            (line._1, validSegments.toArray)
        }).filter(_._2.nonEmpty)

        val results = filterPath.flatMap(line => {
            for (seg <- line._2) yield {
                val seg_new = seg.sortBy(_._1).map(x => transTimeToString(x._1) + "," + x._2 + "," + x._3)
                line._1 + ";" + seg_new.mkString(";")
            }
        })

        results.repartition(10).sortBy(x => x).saveAsTextFile(args(0) + args(2))
        sc.stop()
    }

    def hourOfDay_long(t: Long): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.HOUR_OF_DAY)
    }

    def transTimeToTimestamp(timeString: String): Long = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString).getTime / 1000
        time
    }

    def transTimeToString(timeStamp: Long): String = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.format(timeStamp * 1000)
        time
    }
}
