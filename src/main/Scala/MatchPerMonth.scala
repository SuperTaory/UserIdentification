import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import GeneralFunctionSets.transTimeToTimestamp
import GeneralFunctionSets.dayOfMonth_long
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.{abs, max}

object MatchPerMonth {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MatchPerMonth")
        val sc = new SparkContext(conf)

        // 读取地铁站点名和编号映射关系
        val stationFile = sc.textFile(args(0) + "/zlt/AllInfo/stationInfo-UTF-8.txt")
        val stationNoToNameRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationNo.toInt, stationName)
        })
        val stationNoToName = sc.broadcast(stationNoToNameRDD.collect().toMap)

        // 读取所有有效路径的数据
        val validPathFile = sc.textFile(args(0) + "/zlt/AllInfo/allpath.txt").map(line => {
            val fields = line.split(' ').dropRight(5)
            val sou = stationNoToName.value(fields(0).toInt)
            val des = stationNoToName.value(fields.last.toInt)
            val path = fields.map(x => stationNoToName.value(x.toInt))
            ((sou, des), path)
        }).groupByKey().mapValues(_.toArray).cache()

        // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
        val perODMap = sc.broadcast(validPathFile.collect().toMap)

        // 将OD之间的有效路径涵盖的站点处理为Set集合，OD-pair作为键
        val validPathStationSetRDD = validPathFile.map(v => {
            val temp_set: mutable.Set[String] = mutable.Set()
            v._2.foreach(path => temp_set.++=(path.toSet))
            (v._1, temp_set)
        })
        val validPathStationSet = sc.broadcast(validPathStationSetRDD.collect().toMap)

        // 读取最短路径的时间
        val shortestPath = sc.textFile(args(0) + "/zlt/AllInfo/shortpath.txt").map(line => {
            val fields = line.split(' ')
            val sou = stationNoToName.value(fields(0).toInt)
            val des = stationNoToName.value(fields(1).toInt)
            // 换算成秒
            val time = (fields(2).toFloat * 60).toLong
            ((sou, des), time)
        })
        val shortestPathTime = sc.broadcast(shortestPath.collect().toMap)

        // 读取afc数据 (669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
        val AFCFile = sc.textFile(args(1)).map(line => {
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
        }).filter(_._2._5 > 0)

        // 根据id聚合
        val AFCData = AFCFile.groupByKey().map(line => {
            val dataArray = line._2.toArray.sortBy(_._1)
            val daySets = dataArray.map(_._5).toSet
            (line._1, dataArray, daySets)
        }).filter(_._3.size > 15)

        // 共享为广播变量
        val broadcastAFCData = sc.broadcast(AFCData.collect())

        /*----------------------------------------------------------------------------------------------------------------*/

        // 读取ap数据
        val macFile = sc.textFile(args(2)).map(line => {
            val fields = line.split(',')
            val macId = fields(0).drop(1)
            val time = transTimeToTimestamp(fields(1))
            val station = fields(2)
            (macId, (time, station))
        })
        val APData = macFile.groupByKey().map(line => {
            val data = line._2.toArray.sortBy(_._1)
            val daySets = data.map(x => dayOfMonth_long(x._1)).toSet
            (line._1, data, daySets)
        }).filter(x => x._3.size > 15)

        /*----------------------------------------------------------------------------------------------------------------*/

        // 将AFC数据和AP数据融合
        val AFCAndAP = APData.flatMap(ap => {
            for (afc <- broadcastAFCData.value) yield {
                (afc, ap)
            }
        })

        // 过滤不太可能匹配的配对
        val filterProcess = AFCAndAP.filter(line => {
            val afcDays = line._1._3
            val apDays = line._2._3
            val inter = afcDays.intersect(apDays).size
//            val dif = apDays.diff(afcDays).size
            if (inter / afcDays.size.toFloat > 0.7)
                true
            else
                false
        })

        val matchResult = filterProcess.map(line => {
            val apId = line._2._1
            val afcId = line._1._1
            val AP = line._2._2
            val AFC = line._1._2
            var score = 0f
            var index = 0
            var conflict = true
            while (index < AFC.length & conflict) {
                val so = AFC(index)._2
                val sd = AFC(index)._4
                val to = AFC(index)._1
                val td = AFC(index)._3
                val paths = perODMap.value((so, sd))
                val pathStationSet = validPathStationSet.value((so, sd))
                val l = AP.indexWhere(_._1 > to - 120)
                val r = AP.lastIndexWhere(_._1 < td + 120)
                if (l >= 0 && r >= l) {
                    val macStationSet = AP.slice(l, r+1).map(_._2).toSet
                    if (pathStationSet.union(macStationSet).size == pathStationSet.size) {
                        var temp_score = 0f
                        var index_mac = l
                        var flag = true
                        for (path <- paths if flag) {
                            var path_score = 0f
                            val coincideList = new ListBuffer[Int]
                            if (path.toSet.union(macStationSet).size == path.length) {
                                for (s <- path.indices if index_mac <= r) {
                                    if (AP(index_mac)._2 == path(s)) {
                                        index_mac += 1
                                        coincideList.append(s)
                                    }
                                }
                                // 判断所截取Mac片段中出现的站点是否全部匹配
                                if (coincideList.nonEmpty && r == index_mac - 1) {
                                    // 判断所截取并匹配的片段的起始和结束时间是否合理
                                    val realTime1 = abs(to - AP(l)._1)
                                    val theoreticalTime1 = shortestPathTime.value((so, path(coincideList.head)))
                                    val realTime2 = abs(td - AP(index_mac - 1)._1)
                                    val theoreticalTime2 = shortestPathTime.value((path(coincideList.last), sd))
                                    // 通过程序统计平均误差时间为450秒，这里放宽为600秒
                                    if (abs(realTime1 - theoreticalTime1) < 600 && abs(realTime2 - theoreticalTime2) < 600) {
                                        // 计算最大跨度
                                        if (coincideList.length == 1)
                                            path_score = 1f / path.length
                                        else if (coincideList.length >= 2)
                                            path_score = (coincideList.last - coincideList.head + 1).toFloat / path.length
                                    }
                                }
                            }
                            temp_score = path_score
                            flag = false
                        }
                        score += temp_score
                    }
                    else
                        conflict = false
                }
                index += 1
            }
            // 生成每对AFC记录和AP记录的每月相似度
            (afcId, (apId, score.formatted("%.3f").toFloat, AFC.length, score / AFC.length))
        }).filter(_._2._2 > 10).groupByKey().mapValues(_.toList.maxBy(_._2))

        matchResult.repartition(1).sortBy(_._2._4, ascending = false).saveAsTextFile(args(3))

        sc.stop()
    }
}
