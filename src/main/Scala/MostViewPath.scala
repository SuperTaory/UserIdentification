import GeneralFunctionSets.{hourOfDay_long, transTimeToTimestamp}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.abs

object MostViewPath {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .appName("MostViewPath")
          .getOrCreate()
        val sc = spark.sparkContext

        // 读取地铁站点名和编号映射关系 "1,机场东,22.647011,113.8226476,1268036000,268"
        val stationFile = sc.textFile(args(0) + "zlt/AllInfo/stationInfo-UTF-8.txt")
        val stationNo2NameRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationNo.toInt, stationName)
        })
        val stationNo2Name = sc.broadcast(stationNo2NameRDD.collect().toMap)

        // (龙华,深圳北站,508)
        val ODTimeInterval = sc.textFile(args(0) + "/zlt/UI/AllODTimeInterval/ShortPathTime/part-*").map(line => {
            val p = line.split(',')
            val sou = p(0).drop(1)
            val des = p(1)
            val interval = p(2).dropRight(1).toLong
            ((sou, des), interval)
        })
        val ODIntervalMap = sc.broadcast(ODTimeInterval.collect().toMap)

        // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"
        val allPaths = sc.textFile(args(0) + "zlt/AllInfo/allpath.txt").map(line => {
            val fields = line.split(' ')
            val stations = fields.dropRight(5)
            val info = fields.takeRight(5)
            val time = (info.last.toFloat * 60).toLong
            val trans = info(1).toInt
            val path = stations.map(x => stationNo2Name.value(x.toInt))
            val sou = path.head
            val des = path.last
            ((sou, des), (path, time, trans))
        }).groupByKey().mapValues(_.toArray)

        val allPathsMap = sc.broadcast(allPaths.collect().toMap)

        // (4C49E3376FFF,2019-06-28 19:09:48,留仙洞,1)
        val APData = sc.textFile(args(0) + "/zlt/UI/NormalMacData/part*").map(line => {
            val fields = line.split(',')
            val id = fields(0).drop(1)
            val time = transTimeToTimestamp(fields(1))
            val station = fields(2)
            val dur = fields(3).dropRight(1).toLong
            (id, (time, station, dur))
        }).filter(x => x._2._3 < 1000 & hourOfDay_long(x._2._1) >= 6 & x._1 != "000000000000")
          .groupByKey()
          .mapValues(_.toArray.sortBy(_._1))


        // 划分为出行片段并标记出行日期
        val APTrips = APData.filter(_._2.length > 1).map(line => {
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
                        // 设置容忍时间误差
                        var extraInterval = 0
                        val odInterval = ODIntervalMap.value((segment.last._2, s._2))
                        odInterval / 1800 match {
                            case 0 => extraInterval = 600 //10min
                            case 1 => extraInterval = 900 //15min
                            case _ => extraInterval = 1200 // 20min
                        }
                        val realInterval = abs(s._1 - segment.last._1 - segment.last._3)
                        if (realInterval > odInterval + extraInterval) {
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
        }).flatMap(line => {
            for (trip <- line._2) yield
                (line._1, trip)
        })

        val preparation = APTrips.flatMap(line => {
            val trip = line._2
            val so = trip.head._2
            val sd = trip.last._2
            val paths = allPathsMap.value((so, sd))
            // 保存[((so, sd, path_no), to)]
            val output = new ArrayBuffer[((String, String), Int)]()
            // 确定trip通过的哪条路径
            val path_number = new ArrayBuffer[Int]()
            for (i <- paths.indices) {
                val path = paths(i)._1
                var index = 0
                for (s <- path if index < trip.length) {
                    if (s == trip(index)._2)
                        index += 1
                }
                if (index == trip.length)
                    path_number.append(i)
            }

            // 仅统计唯一确定经过的路径
            if (path_number.length == 1)
                output.append(((so, sd), path_number(0)))
            for (line <- output.toArray) yield
                line
        })

        val haveMostViewPath = preparation.groupByKey()
          .mapValues(x => x.toArray.groupBy(y => y).maxBy(_._2.length)._1)
          .map(line => {
              val path = allPathsMap.value(line._1)(line._2)._1
              (line._1, path.mkString(","))
          })
        val haveMostViewPathMap = sc.broadcast(haveMostViewPath.collect().toMap)

        val allMostViewPath = ODTimeInterval.map(line => {
            val od = line._1
            if (haveMostViewPathMap.value.contains(od)) {
                haveMostViewPathMap.value(od)
            }
            else {
                val paths = allPathsMap.value(od)
                val selectPath = paths.minBy(x => (x._3, abs(x._2 - ODIntervalMap.value(od))))._1
                selectPath.mkString(",")
            }
        })

        allMostViewPath.repartition(1).saveAsTextFile(args(0) + "/zlt/UI-2021/MostViewPath")
        sc.stop()
    }
}
