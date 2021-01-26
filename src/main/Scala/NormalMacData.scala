import GeneralFunctionSets.{dayOfMonth_long, transTimeToString}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.abs

/**
 * 通过划分片段尽可能的将异常数据去除
 */
object NormalMacData {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("NormalMacData")
        val sc = new SparkContext(conf)

        val readODTimeInterval = sc.textFile(args(0)).map(line => {
            val p = line.split(',')
            val sou = p(0).drop(1)
            val des = p(1)
            val interval = p(2).dropRight(1).toLong
            ((sou, des), interval)
        })
        val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

        val macFile = sc.textFile(args(1)).map(line => {
            val fields = line.split(',')
            val macId = fields(0).drop(1)
            val time = fields(1).toLong
            val station = fields(2)
            val dur = fields(3).dropRight(1).toInt
            (macId, (time, station, dur))
        }).filter(_._2._3 < 1500).groupByKey().mapValues(_.toArray.sortBy(_._1))

        // 去除掉出行片段长度小于m以及出行天数小于n天的AP数据
        val filteringMacData = macFile.map(line => {
            // 设置出行片段长度阈值
            val m = 1
            val MacId = line._1
            val data = line._2
            val segment = new ListBuffer[(Long, String, Int)]
            val segments = new ListBuffer[List[(Long, String, Int)]]
            val daySets: mutable.Set[Int] = mutable.Set()
            for (s <- data) {
                if (segment.isEmpty) {
                    segment.append(s)
                }
                else {
                    // 遇到前后相邻为同一站点进行划分
                    if (s._2 == segment.last._2) {
                        if (segment.length > m) {
                            segments.append(segment.toList)
                            daySets.add(dayOfMonth_long(segment.head._1))
                        }
                        segment.clear()
                    }
                    else {
                        // 设置容忍时间误差
                        var attachInterval = 0
                        val odInterval = ODIntervalMap.value((segment.last._2, s._2))
                        odInterval / 1800 match {
                            case 0 => attachInterval = 600
                            case 1 => attachInterval = 1200
                            case _ => attachInterval = 1800
                        }
                        val realInterval = abs(s._1 - segment.last._1 - segment.last._3)
                        // 间隔过大或者过小则进行切分
                        if (realInterval > odInterval + attachInterval || (odInterval > 900 && realInterval < odInterval * 0.5)) {
                            if (segment.length > m) {
                                segments.append(segment.toList)
                                daySets.add(dayOfMonth_long(segment.head._1))
                            }
                            segment.clear()
                        }
                    }
                    segment.append(s)
                }
            }
            if (segment.length > m) {
                segments.append(segment.toList)
                daySets.add(dayOfMonth_long(segment.head._1))
            }
            (MacId, segments.toList, daySets.size)
        })


        val result = filteringMacData.flatMap(line => {
            val id = line._1
            for (s <- line._2; v <- s) yield
                (id, transTimeToString(v._1), v._2, v._3)
        })
            .repartition(100)
            .sortBy(x => (x._1, x._2))

        result.saveAsTextFile(args(2))

        sc.stop()
    }
}
