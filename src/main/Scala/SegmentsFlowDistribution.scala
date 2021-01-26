import GeneralFunctionSets.{hourOfDay_long, transTimeToTimestamp}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.abs

object SegmentsFlowDistribution {
    def main(args: Array[String]): Unit = {
        /**
         * 根据AP数据统计每个片段的流量情况 以出发时间划分每2h一个时段
         * 同时排除部分有效路径
         */
        val spark = SparkSession
            .builder()
            .appName("SegmentsFlowDistribution")
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
        val readODTimeInterval = sc.textFile(args(0) + "/zlt/UI/AllODTimeInterval/ShortPathTime/part-*").map(line => {
            val p = line.split(',')
            val sou = p(0).drop(1)
            val des = p(1)
            val interval = p(2).dropRight(1).toLong
            ((sou, des), interval)
        })
        val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

        // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"
        val allPaths = sc.textFile(args(0) + "zlt/AllInfo/allpath.txt").map(line => {
            val fields = line.split(' ')
            val stations = fields.dropRight(5)
            val info = fields.takeRight(5)
            val time = info.last.toFloat
            val path = stations.map(x => stationNo2Name.value(x.toInt))
            val sou = path.head
            val des = path.last
            ((sou, des), (path, time))
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
            val id = line._1
            val trip = line._2
            val so = trip.head._2
            val sd = trip.last._2
            val to = hourOfDay_long(trip.head._1) / 2
            val paths = allPathsMap.value((so, sd))
            // 保存[((so, sd, path_no), to)]
            val output = new ArrayBuffer[((String, String, Int), Int)]()
            // 确定trip通过的哪条路径
            val path_no = new ArrayBuffer[(Int, Float)]()
            for (i <- paths.indices) {
                val path = paths(i)._1
                val time = paths(i)._2
                var index = 0
                for (s <- path if index < trip.length) {
                    if (s == trip(index)._2)
                        index += 1
                }
                if (index == trip.length)
                    path_no.append((i, time))
            }

            if (path_no.length == 1)
                output.append(((so, sd, path_no(0)._1), to))
//            else {
//                output.append(((so, sd, path_no.minBy(_._2)._1), to))
//            }
            for (line <- output.toArray) yield
                line
        })

//        preparation.repartition(1).saveAsTextFile(args(0) + "/zlt/UI-2021/debug")

        val tripFlow = preparation.groupByKey().mapValues(line => {
            val data = line.toArray
            val countByOt = Array.ofDim[Int](12)
            for (i <- 0.until(12))
                countByOt(i) = data.count(_ == i)
            countByOt
        })

        // 切分每个trip的所有覆盖的片段
        val segmentsFlow = tripFlow.flatMap(line => {
            val od = (line._1._1, line._1._2)
            val path_no = line._1._3
            val path = allPathsMap.value(od)(path_no)._1
            val countByOt = line._2
            val segments = new ArrayBuffer[(String, String)]
            if (path.nonEmpty) {
                val len = path.length
                for (i <- 0.until(len - 1)) {
                    for (j <- (i + 1).until(len)) {
                        segments.append((path(i), path(j)))
                    }
                }
            }
            for (s <- segments) yield
                (s, countByOt)
        })

        val res = segmentsFlow.groupByKey().mapValues(v => {
            val data = v.toArray
            data.reduce((x, y) => x.zip(y).map(x => x._1 + x._2))
        }).map(line => line._1._1 + "," + line._1._2 + "," + line._2.mkString(","))

        res.repartition(1).saveAsTextFile(args(0) + "/zlt/UI-2021/SegmentsFlowDistribution_1")
        sc.stop()
    }
}
