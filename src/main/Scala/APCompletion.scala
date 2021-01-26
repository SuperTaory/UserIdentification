import GeneralFunctionSets.{dayOfMonth_long, secondsOfDay, transTimeToString, transTimeToTimestamp}
import Model.{RBF, z_score}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.abs

object APCompletion {

    case class distAndKinds(var d: Long, var k: Int)

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("AP be completed by Afc")
            .getOrCreate()
        val sc = spark.sparkContext

        // 读取地铁站点名和编号映射关系
        val stationFile = sc.textFile(args(0) + "/liutao/AllInfo/stationInfo-UTF-8.txt")
        val stationNoToNameRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationNo.toInt, stationName)
        })
        val stationNoToName = sc.broadcast(stationNoToNameRDD.collect().toMap)

        // 读取所有有效路径的数据
        val validPathFile = sc.textFile(args(0) + "/liutao/AllInfo/allpath.txt").map(line => {
            // 仅保留站点编号信息
            val fields = line.split(' ').dropRight(5)
            val sou = stationNoToName.value(fields(0).toInt)
            val des = stationNoToName.value(fields(fields.length - 1).toInt)
            val pathStations = new ListBuffer[String]
            fields.foreach(x => pathStations.append(stationNoToName.value(x.toInt)))
            ((sou, des), pathStations.toList)
        }).groupByKey().mapValues(_.toList).cache()

        // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
        val validPathMap = sc.broadcast(validPathFile.collect().toMap)

        // 读取站间时间间隔
        val readODTimeInterval = sc.textFile(args(0) + "/liutao/UI/AllODTimeInterval/ShortPathTime/part-00000").map(line => {
            val p = line.split(',')
            val sou = p(0).drop(1)
            val des = p(1)
            val interval = p(2).dropRight(1).toLong
            ((sou, des), interval)
        })
        val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

        // (669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
        val AFCFile = sc.textFile(args(0) + "/liutao/UI/GroundTruth/afcData/part-*").map(line => {
            val fields = line.split(',')
            val id = fields(0).drop(1)
            val ot = transTimeToTimestamp(fields(1))
            val os = fields(2)
            val dt = transTimeToTimestamp(fields(4))
            val ds = fields(5)
            val o_day = dayOfMonth_long(ot)
            val d_day = dayOfMonth_long(dt)
            val day = if (o_day == d_day) o_day else 0
            (id, (ot, os, dt, ds, day))
        })

        // 划分AFC,仅保留出行天数大于5天的数据
        val AFCPartitions = AFCFile.groupByKey().map(line => {
            val dataArray = line._2.toList.sortBy(_._1)
            val daySets = dataArray.map(_._5).toSet
            (line._1, dataArray, daySets)
        })

        // AFC模式提取-基于核密度估计的聚类
        val AFCPatterns = AFCPartitions.map(line => {
            val pairs = line._2
            val daySets = line._3

            // 统计主要站点-进出站出现次数最多的站点
            val stationCount = new ArrayBuffer[String]()
            pairs.foreach(x => {
                stationCount.append(x._2)
                stationCount.append(x._4)
            })
            // 控制保存主要站点的个数
            val Q = 1
            val topStations = stationCount
                .groupBy(x => x)
                .mapValues(_.size)
                .toArray
                .sortBy(_._2)
                .takeRight(Q)
                .map(_._1)

            // 提取时间戳对应当天的秒数用于聚类
            val stampBuffer = new ArrayBuffer[Long]()
            pairs.foreach(v => {
                stampBuffer.append(secondsOfDay(v._1))
                stampBuffer.append(secondsOfDay(v._3))
            })
            val timestamps = stampBuffer.toArray.sorted
            // 设置带宽h，单位为秒
            val h = 1800
            // 计算局部密度
            val density_stamp_Buffer = new ArrayBuffer[(Double, Long)]()
            for (t <- timestamps) {
                var temp = 0D
                for (v <- timestamps) {
                    temp += RBF(v, t, h)
                }
                density_stamp_Buffer.append((temp / (timestamps.length * h), t))
            }
            val density_stamp = density_stamp_Buffer.toArray.sortBy(_._2)

            // 判断是否存在聚类中心，若返回为空则不存在，否则分类
            val cluster_center = z_score(density_stamp)

            // 设置类边界距离并按照聚类中心分配数据
            val dc = 5400
            // 初始化类簇,结构为[所属类，出行片段]
            val clusters = new ArrayBuffer[(Int, (Long, String, Long, String, Int))]
            for (v <- pairs) {
                if (cluster_center.nonEmpty) {
                    val o_stamp = secondsOfDay(v._1)
                    val d_stamp = secondsOfDay(v._3)
                    val o_to_c = distAndKinds(Long.MaxValue, 0)
                    val d_to_c = distAndKinds(Long.MaxValue, 0)
                    for (c <- cluster_center) {
                        if (abs(o_stamp - c._2) < dc && abs(o_stamp - c._2) < o_to_c.d) {
                            o_to_c.k = c._1
                            o_to_c.d = abs(o_stamp - c._2)
                        }
                        if (abs(d_stamp - c._2) < dc && abs(d_stamp - c._2) < d_to_c.d) {
                            d_to_c.k = c._1
                            d_to_c.d = abs(d_stamp - c._2)
                        }
                    }
                    if (o_to_c.k == d_to_c.k && o_to_c.k != 0)
                        clusters.append((o_to_c.k, v))
                    else
                        clusters.append((0, v))
                }
                else
                    clusters.append((0, v))
            }
            // 按照所属类别分组
            val grouped = clusters.groupBy(_._1).toArray.filter(x => x._1 > 0)
            // 存储出行模式集合
            val afc_patterns = new ArrayBuffer[(String, String)]()
            pairs.groupBy(x => (x._2, x._4)).foreach(g => {
                if (g._2.length > pairs.length / 3)
                    afc_patterns.append(g._1)
            })
            if (grouped.nonEmpty) {
                grouped.foreach(g => {
                    // 同一类中数据按照进出站分组
                    val temp_data = g._2.toArray.groupBy(x => (x._2._2, x._2._4))
                    temp_data.foreach(v => {
                        // 超过总出行天数的1/2则视为出行模式
                        if (v._2.length >= 5 || v._2.length > daySets.size / 2) {
                            afc_patterns.append(v._1)
                        }
                    })
                })
            }

            // id、出行片段集合、出行模式集合、主要站点集合、出行日期集合
            (line._1, (pairs, afc_patterns.toSet))
        })

        val AFCBroadcast = sc.broadcast(AFCPatterns.collect().toMap)

        /** *************************************************************************/

        // 读取AP数据:(000000000000,2019-06-01 10:38:05,布吉,0,2019-06-01 10:43:50,上水径,15)
        val APFile = sc.textFile(args(0) + "/liutao/UI/GroundTruth/apData/part*").map(line => {
            val fields = line.split(",")
            val id = fields(0).drop(1)
            val ot = transTimeToTimestamp(fields(1))
            val os = fields(2)
            val o_stay = fields(3).toInt
            val dt = transTimeToTimestamp(fields(4))
            val ds = fields(5)
            val d_stay = fields.last.dropRight(1).toInt
            val o_day = dayOfMonth_long(ot)
            val d_day = dayOfMonth_long(dt)
            val day = if (o_day == d_day) o_day else 0
            // id、（起始时间、起始站点、停留时间、到达时间、目的站点、 停留时间、出行日期）
            (id, (ot, os, o_stay, dt, ds, d_stay, day))
        })

        // 划分AP
        val APPartitions = APFile.groupByKey().map(line => {
            val dataArray = line._2.toList.sortBy(_._1)
            val daySets = dataArray.map(_._7).toSet
            (line._1, dataArray)
        })

        //    val APBroadcast = sc.broadcast(APPartitions.collect().toMap)

        // 读取AP和AFC的ID映射关系 (020362809,CC2D830C8130,30.0,30,1.0)
        val IdMap = sc.textFile(args(0) + "/liutao/UI/GroundTruth/IdMap/part*").map(line => {
            val fields = line.split(",")
            val afcId = fields.head.drop(1)
            val apId = fields(1)
            (apId, afcId)
        })
        val IdMapBroadcast = sc.broadcast(IdMap.collect().toMap)

        val counter = new LongAccumulator
        sc.register(counter)

        val combineAfcAp = APPartitions.map(line => {
            val afcId = IdMapBroadcast.value(line._1)
            val afcData = AFCBroadcast.value(afcId)._1
            val afcPattern = AFCBroadcast.value(afcId)._2
            val apData = line._2
            val completedAPData = new ListBuffer[(Long, String, Int, Long, String, Int, Int)]
            var a = 0
            var b = 0
            while (a < afcData.length && b < apData.length) {
                val afc = afcData(a)
                val ap = apData(b)
                if (afcPattern.contains((afc._2, afc._4))) {
                    if (ap._1 > afc._1 - 300 && ap._4 < afc._3 + 300) {
                        val paths = validPathMap.value((afc._2, afc._4))
                        var flag = true
                        for (p <- paths if flag) {
                            if (p.indexOf(ap._2) >= 0 && p.indexOf(ap._5) > p.indexOf(ap._2)) {
                                completedAPData.append((afc._1, afc._2, 0, afc._3, afc._4, 0, afc._5))
                                flag = false
                            }
                        }
                        if (flag)
                            completedAPData.append(ap)
                        b += 1
                    }
                    else if (ap._1 > afc._3) {
                        a += 1
                    }
                    else {
                        completedAPData.append(ap)
                        b += 1
                    }
                }
                else {
                    a += 1
                }
            }
            if (a >= afcData.length) {
                while (b < apData.length) {
                    completedAPData.append(apData(b))
                    b += 1
                }
            }

            if (completedAPData.length != apData.length)
                counter.add(1)

            (line._1, completedAPData.toList)
        })

        val res = combineAfcAp.flatMap(line => {
            for (v <- line._2) yield {
                (line._1, transTimeToString(v._1), v._2, v._3, transTimeToString(v._4), v._5, v._6)
            }
        })

        res.repartition(1).saveAsTextFile(args(0) + "/liutao/UI/GroundTruth/completedApData")

        println("counter:" + counter.value.toString)
        sc.stop()
    }
}
