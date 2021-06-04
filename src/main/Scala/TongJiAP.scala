import GeneralFunctionSets.{hourOfDay_long, transTimeToString, transTimeToTimestamp, dayOfMonth_long}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.abs

object TongJiAP {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("TongJiAP")
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
        val macFile = sc.textFile(args(0) + "/zlt/UI/NormalMacData/*").map(line => {
            val fields = line.split(',')
            val macId = fields(0).drop(1)
            val time = transTimeToTimestamp(fields(1))
            val station = fields(2)
            // 停留时间
            val dur = fields(3).dropRight(1).toLong
            val day = dayOfMonth_long(time)
            (macId, (time, station, dur, day))
        }).filter(x => x._2._3 < 900 & hourOfDay_long(x._2._1) >= 6  & x._1 != "000000000000")
            .filter(_._2._4 <= 15)
            .groupByKey()
            .mapValues(_.toArray.sortBy(_._1))
            .filter(_._2.length > 5)

        // 划分为出行片段
        val APSegments = macFile.map(line => {
            // 设置出行片段长度阈值
            val m = 2
            val MacId = line._1
            val data = line._2
            val segment = new ListBuffer[(Long, String, Long, Int)]
            val segments = new ListBuffer[List[(Long, String, Long, Int)]]
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
            val validSegments = new ArrayBuffer[Array[(Long, String, Long, Int)]]()
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
        }).filter(_._2.nonEmpty).cache()

        val passengers = filterPath.count()
        val trips = filterPath.map(x => (x._2.length, 1)).reduceByKey(_+_).collect()
        val totalTrips = trips.map(x => x._1 * x._2).sum
        val active_days = filterPath.map(x => (x._2.map(y => y.head._4).toSet.size, 1)).reduceByKey(_+_).collect()

        println(passengers)
        println(totalTrips)
        println(trips.sortBy(_._1).mkString("Array(", ", ", ")"))
        println(active_days.sortBy(_._1).mkString("Array(", ", ", ")"))

        sc.stop()
    }
}
