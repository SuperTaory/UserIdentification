import AMPI_1.{RBF, distAndKinds, z_score}
import GeneralFunctionSets.{dayOfMonth_long, hourOfDay_long, secondsOfDay, transTimeToTimestamp}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.abs

object TripType {
    def main(args: Array[String]): Unit = {
        /**
         * 统计不同匹配类型的出行数量
         */
        val spark = SparkSession
            .builder()
            .appName("TripType")
            .getOrCreate()
        val sc = spark.sparkContext

        // 读取groundTruth (251449740,ECA9FAE07B4F,26.857,43,0.6245814)
        val groundTruthData = sc.textFile(args(0) + "/zlt_hdfs/UI-2021/GroundTruth/IdMap/part-*").map(line => {
            val fields = line.split(",")
            val afcId = fields(0).drop(1)
            val apId = fields(1)
            (afcId, apId)
        })
        val groundTruthMap = sc.broadcast(groundTruthData.collect().toMap)

        // 读取地铁站点名和编号映射关系 "1,机场东,22.647011,113.8226476,1268036000,268"
        val stationFile = sc.textFile(args(0) + "/zlt_hdfs/AllInfo/stationInfo-UTF-8.txt")
        val stationNo2NameRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationNo.toInt, stationName)
        })
        val stationNo2Name = sc.broadcast(stationNo2NameRDD.collect().toMap)

        // 读取站间时间间隔，单位：秒 "(龙华,清湖,133)"
        val readODTimeInterval = sc.textFile(args(0) + "/zlt_hdfs/UI/AllODTimeInterval/ShortPathTime/part-00000").map(line => {
            val p = line.split(',')
            val sou = p(0).drop(1)
            val des = p(1)
            val interval = p(2).dropRight(1).toLong
            ((sou, des), interval)
        })
        val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

        // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"
        val validPathFile = sc.textFile(args(0) + "/zlt_hdfs/AllInfo/allpath.txt").map(line => {
            val fields = line.split(' ').dropRight(5)
            val sou = stationNo2Name.value(fields(0).toInt)
            val des = stationNo2Name.value(fields(fields.length - 1).toInt)
            val path = fields.map(x => stationNo2Name.value(x.toInt))
            ((sou, des), path)
        }).groupByKey().mapValues(x => (x.toArray, x.minBy(_.length).length))

        val validPathMap = sc.broadcast(validPathFile.collect().toMap)

        // (4C49E3376FFF,2019-06-28 19:09:48,留仙洞,1)
        val apFile = sc.textFile(args(0) + "/zlt_hdfs/UI-2021/GroundTruth/APData/part*").map(line => {
            val fields = line.split(',')
            val macId = fields(0).drop(1)
            val time = transTimeToTimestamp(fields(1))
            val station = fields(2)
            val dur = fields(3).dropRight(1).toLong
            (macId, (time, station, dur))
        }).filter(x => hourOfDay_long(x._2._1) >= 6  & x._1 != "000000000000")
            .groupByKey()
            .mapValues(_.toArray.sortBy(_._1))

        // 划分为出行片段并标记出行日期
        val APSegments = apFile.map(line => {
            // 设置出行片段长度阈值
            val m = 1
            val apID = line._1
            val data = line._2
            val segment = new ListBuffer[(Long, String, Long)]
            val segments = new ListBuffer[List[(Long, String, Long)]]
            val daySets: mutable.Set[Int] = mutable.Set()
            for (s <- data) {
                if (segment.isEmpty) {
                    segment.append(s)
                }
                else {
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
                            case 0 => attachInterval = 600 //10min
                            case 1 => attachInterval = 900 //15min
                            case _ => attachInterval = 1200 // 20min
                        }
                        val realInterval = abs(s._1 - segment.last._1 - segment.last._3)
                        if (realInterval > odInterval + attachInterval) {
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
            val trips = new ArrayBuffer[(Long, String, Long, String)]()
            for (seg <- segments.toList) {
                if (seg.length >= 2) {
                    val O = seg.head
                    val D = seg.last
                    trips.append((O._1, O._2, D._1, D._2))
                }
            }
            (apID, trips.toArray)
        })

//        /**
//         * 读取AP数据:(00027EF9CD6F,2019-06-01 08:49:11,固戍,452,2019-06-01 09:16:29,洪浪北,150,2019-06-01 08:49:11,固戍,2019-06-01 08:54:39,坪洲)
//         */
//        val APFile = sc.textFile(args(0) + "/zlt_hdfs/UI-2021/GroundTruth/SampledAPData-100%/*").map(line => {
//            val fields = line.split(",")
//            val id = fields(0).drop(1)
//            val ot = transTimeToTimestamp(fields(1))
//            val os = fields(2)
//            val dt = transTimeToTimestamp(fields(4))
//            val ds = fields(5)
//            val o_day = dayOfMonth_long(ot)
//            val d_day = dayOfMonth_long(dt)
//            val day = if (o_day == d_day) o_day else 0
//            val samp_ot = transTimeToTimestamp(fields(7))
//            val samp_os = fields(8)
//            val samp_dt = transTimeToTimestamp(fields(9))
//            val samp_ds = fields(10).dropRight(1)
//            (id, ((ot, os, dt, ds, day), (samp_ot, samp_os, samp_dt, samp_ds)))
//        }).filter(_._2._1._5 > 0)
//
//        val APPartitions = APFile.groupByKey().map(line => {
//            val apId = line._1
//            val data = line._2.toArray.sortBy(_._1._1)
//            val trips = data.map(_._1)
//            val sampledTrips = data.map(_._2)
//            val daySets = trips.map(_._5).toSet
//            (apId, sampledTrips)
//        })

        val APData = sc.broadcast(APSegments.collect().toMap)

        /**
         * AFC data: (669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
         */
        val AFCFile = sc.textFile(args(0) + "/zlt_hdfs/UI-2021/GroundTruth/AFCData/part-00000").map(line => {
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

        val AFCPartitions = AFCFile.groupByKey().map(line => {
            val dataArray = line._2.toList.sortBy(_._1)
            (line._1, dataArray)
        })
        
        // 将AP和AFC数据按照IdMap结合
        val mergeData = AFCPartitions.map(line => {
            val apID = groundTruthMap.value(line._1)
            val AP = APData.value.getOrElse(apID, Array.empty)
            val AFC = line._2
            (line._1, apID, AFC, AP)
        }).filter(_._4.nonEmpty)

        val result = mergeData.map(line => {
            val afcID = line._1
            val apID = line._2
            val AFC = line._3
            val AP = line._4

            val tr_ap_afc = new ArrayBuffer[(Int, Int)]()
            val tr_ap = new ArrayBuffer[Int]()
            val tr_afc = new ArrayBuffer[Int]()
            var index_ap = 0
            var index_afc = 0
            val conflict_1 = new ListBuffer[(Int, Int)]
            val conflict_2 = new ListBuffer[(Int, Int)]
            var ff = false

            while (index_ap < AP.length && index_afc < AFC.length) {
                val cur_ap = AP(index_ap)
                val cur_afc = AFC(index_afc)
                if (cur_ap._3 < cur_afc._1) {
                    tr_ap.append(index_ap)
                    index_ap += 1
                }
                else if (cur_ap._1 > cur_afc._3) {
                    tr_afc.append(index_afc)
                    index_afc += 1
                }
                else if (cur_ap._1 > cur_afc._1 - 600 && cur_ap._3 < cur_afc._3 + 600) {
                    val paths = validPathMap.value((cur_afc._2, cur_afc._4))._1
                    var flag = true
                    for (path <- paths if flag) {
                        if (path.indexOf(cur_ap._2) >= 0 && path.indexOf(cur_ap._4) > path.indexOf(cur_ap._2)) {
                            ff = true
                            val interval1 = ODIntervalMap.value(path.head, cur_ap._2)
                            val headGap = cur_ap._1 - cur_afc._1
                            val interval2 = ODIntervalMap.value(cur_ap._4, path.last)
                            val endGap = cur_afc._3 - cur_ap._3
                            if (headGap < 600 + interval1) {
                                flag = false
                                tr_ap_afc.append((index_ap, index_afc))
                            }
                        }
                    }
                    if (flag) {
                        if (ff) {
                            conflict_2.append((index_afc, index_ap))
                            ff = false
                        } else
                            conflict_1.append((index_afc, index_ap))
                    }
                    index_afc += 1
                    index_ap += 1
                }
                else {
                    conflict_2.append((index_afc, index_ap))
                    index_afc += 1
                    index_ap += 1
                }
            }
            (afcID, apID, tr_ap_afc.length, tr_afc.length, tr_ap.length, conflict_1.length, conflict_2.length, AFC.length, AP.length, conflict_1.toList, conflict_2.toList)
        })


        result.filter(_._10.nonEmpty).repartition(1).saveAsTextFile(args(0) + "/zlt_hdfs/UI-2021/TripTypeCount_f1")
        val total = result.map(xu => (xu._3, xu._4, xu._5, xu._6, xu._7, xu._8, xu._9))
            .reduce((x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6, x._7 + y._7))
        println("******")
        println(total)
        println("******")
        sc.stop()
    }
}
