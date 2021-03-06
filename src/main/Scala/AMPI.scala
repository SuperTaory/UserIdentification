import GeneralFunctionSets.{dayOfMonth_long, hourOfDay_long, secondsOfDay, transTimeToTimestamp}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math._

object AMPI {

    case class distAndKinds(var d: Long, var k: Int)

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("AMPI")
            .getOrCreate()
        val sc = spark.sparkContext

        // 读取地铁站点名和编号映射关系 "1,机场东,22.647011,113.8226476,1268036000,268"
        val stationFile = sc.textFile(args(0) + "/zlt/AllInfo/stationInfo-UTF-8.txt")
        val stationNo2NameRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationNo.toInt, stationName)
        })
        val stationNo2Name = sc.broadcast(stationNo2NameRDD.collect().toMap)

        // 读取站间时间间隔，单位：秒 "(龙华,清湖,133)"
        val readODTimeInterval = sc.textFile(args(0) + "/zlt/UI/AllODTimeInterval/ShortPathTime/part-00000").map(line => {
            val p = line.split(',')
            val sou = p(0).drop(1)
            val des = p(1)
            val interval = p(2).dropRight(1).toLong
            ((sou, des), interval)
        })
        val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

        // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"
        val validPathFile = sc.textFile(args(0) + "/zlt/AllInfo/allpath.txt").map(line => {
            val fields = line.split(' ').dropRight(5)
            val sou = stationNo2Name.value(fields(0).toInt)
            val des = stationNo2Name.value(fields(fields.length - 1).toInt)
            val path = fields.map(x => stationNo2Name.value(x.toInt))
            ((sou, des), path)
        }).groupByKey().mapValues(x => (x.toArray, x.minBy(_.length).length))

        val validPathMap = sc.broadcast(validPathFile.collect().toMap)

        // 读取groundTruth计算Accuracy (251449740,ECA9FAE07B4F,26.857,43,0.6245814)
        val groundTruthData = sc.textFile(args(0) + "/zlt/UI-2021/GroundTruth/IdMap/part-*").map(line => {
            val fields = line.split(",")
            val afcId = fields(0).drop(1)
            val apId = fields(1)
            (apId, afcId)
        })
        val groundTruthMap = sc.broadcast(groundTruthData.collect().toMap)

        // 读取flow distribution "蛇口港,黄贝岭,0,0,0,259,193,173,223,350,821,903,338,114"
        val flowDistribution = sc.textFile(args(0) + "/zlt/UI-2021/SegmentsFlowDistribution/part-00000").map(line => {
            val fields = line.split(",")
            val os = fields(0)
            val ds = fields(1)
            val flow = fields.takeRight(12).map(_.toInt)
            ((os, ds), flow)
        })
        val flowMap = sc.broadcast(flowDistribution.collect().toMap)


        /**
         * AFC data: (669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
         */
        val AFCFile = sc.textFile(args(0) + "/Destination/subway-pair/part-000[0-6]*").map(line => {
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

        // 根据id聚合,仅保留出行天数大于5天的乘客数据
        val AFCPartitions = AFCFile.groupByKey().map(line => {
            val dataArray = line._2.toList.sortBy(_._1)
            val daySets = dataArray.map(_._5).toSet
            (line._1, dataArray, daySets)
        }).filter(_._3.size > 5)

        // AFC模式提取-基于核密度估计的聚类
        val AFCPatterns = AFCPartitions.map(line => {
            // 将每次出行的索引信息记录
            val pairs = new ArrayBuffer[(Long, String, Long, String, Int)]()
            for (i <- line._2.indices) {
                val trip = line._2(i)
                pairs.append((trip._1, trip._2, trip._3, trip._4, i))
            }
            val daySets = line._3

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

            // 存储所有pattern的出行索引信息
            val afc_patterns = new ListBuffer[List[Int]]()

            // 按照所属类别分组
            val grouped = clusters.groupBy(_._1).toArray.filter(x => x._1 > 0)
            if (grouped.nonEmpty) {
                grouped.foreach(g => {
                    // 同一类中数据按照进出站分组
                    val temp_data = g._2.toArray.groupBy(x => (x._2._2, x._2._4))
                    temp_data.foreach(v => {
                        // 超过总出行天数的1/2则视为出行模式
                        if (v._2.length >= 5 || v._2.length > daySets.size / 2) {
                            // 存储当前pattern中所有出行的索引信息
                            val temp_patterns = new ListBuffer[Int]()
                            v._2.foreach(x => temp_patterns.append(x._2._5))
                            afc_patterns.append(temp_patterns.toList)
                        }
                    })
                })
            }

            // id、出行片段集合、出行模式数组(包含出行索引信息)、出行日期集合
            (line._1, pairs.toArray, afc_patterns.toList, daySets)
        })

        /**
         * 读取AP数据:(00027EF9CD6F,2019-06-01 08:49:11,固戍,452,2019-06-01 09:16:29,洪浪北,150,2019-06-01 08:49:11,固戍,2019-06-01 08:54:39,坪洲)
         */
        val APFile = sc.textFile(args(0) + "/zlt/UI-2021/GroundTruth/SampledAPData-20%_old/part-*").map(line => {
            val fields = line.split(",")
            val id = fields(0).drop(1)
            val ot = transTimeToTimestamp(fields(1))
            val os = fields(2)
            val dt = transTimeToTimestamp(fields(4))
            val ds = fields(5)
            val o_day = dayOfMonth_long(ot)
            val d_day = dayOfMonth_long(dt)
            val day = if (o_day == d_day) o_day else 0
            val samp_ot = transTimeToTimestamp(fields(7))
            val samp_os = fields(8)
            val samp_dt = transTimeToTimestamp(fields(9))
            val samp_ds = fields(10).dropRight(1)
            (id, ((ot, os, dt, ds, day), (samp_ot, samp_os, samp_dt, samp_ds)))
        }).filter(_._2._1._5 > 0)

        // 划分AP
        val APPartitions = APFile.groupByKey().map(line => {
            val apId = line._1
            val data = line._2.toArray.sortBy(_._1._1)
            val trips = data.map(_._1)
            val sampledTrips = data.map(_._2)
            val daySets = trips.map(_._5).toSet
            (daySets.size, (apId, trips, daySets, sampledTrips))
        })

        val APData = sc.broadcast(APPartitions.groupByKey().mapValues(_.toArray).collect().toMap)

        // 将AP和AFC数据按照天数结合
        val mergeData = AFCPatterns.flatMap(afc => {
            // 允许ap天数比afc天数多的天数限制
            val extra = 5
            val limit = afc._4.size + extra
            val candidateDays = APData.value.keys.toSet.filter(x => x <= limit)
            for (i <- candidateDays; ap <- APData.value(i)) yield {
                (ap, afc)
            }
        })

//        val gama_1 = args(1).toDouble
//        val gama_2 = args(2).toDouble
//        val gama_3 = args(3).toDouble
//        val diff = args(4).toFloat
        val matchData = mergeData.map(line => {
            //  Array[(Long, String, Long, String, Int)]
            val AP = line._1._2
            //  Array[(Long, String, Long, String)]
            val sampledAP = line._1._4
            //  Array[(Long, String, Long, String, Int)]
            val AFC = line._2._2
            val tr_ap_afc = new ArrayBuffer[(Int, Int)]()
            val tr_ap = new ArrayBuffer[Int]()
            val tr_afc = new ArrayBuffer[Int]()
            var index_ap = 0
            var index_afc = 0
            var conflict = 0

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
                else if (cur_ap._1 > cur_afc._1 - 300 && cur_ap._3 < cur_afc._3 + 300) {
                    val paths = validPathMap.value((cur_afc._2, cur_afc._4))._1
                    var flag = true
                    for (path <- paths if flag) {
                        if (path.indexOf(cur_ap._2) >= 0 && path.indexOf(cur_ap._4) > path.indexOf(cur_ap._2)) {
                            val interval1 = ODIntervalMap.value(path.head, cur_ap._2)
                            val headGap = cur_ap._1 - cur_afc._1
                            val interval2 = ODIntervalMap.value(cur_ap._4, path.last)
                            val endGap = cur_afc._3 - cur_ap._3
                            if (headGap < 600 + interval1) {
                                if (0.5 * interval2 < endGap  & endGap < 600 + interval2) {
                                    flag = false
                                    tr_ap_afc.append((index_ap, index_afc))
                                }
                            }
                        }
                    }
                    if (flag) {
                        conflict += 1
                    }
                    index_afc += 1
                    index_ap += 1
                }
                else {
                    conflict += 1
                    index_afc += 1
                    index_ap += 1
                }
            }
            val conflictRatio = conflict.toDouble / (AP.length + AFC.length)

            // key:afc_index, value:(ap_index, score)
            var OL: Map[Int, (Int, (Double, Double))] = Map()
//            val debug = new ListBuffer[(Float, Float)]()
            val afc_pattern = line._2._3
            val Q = tr_ap_afc.length
            val P = tr_afc.length
            val R = tr_ap.length
            val records = new ListBuffer[List[(Double, Double)]]()
            val score = new ListBuffer[Double]()
            var Similarity = 0d
            var maybeMatch = true
            if (conflictRatio <= 0.1) {
                if (tr_ap_afc.nonEmpty) {
                    for (pair <- tr_ap_afc) {
                        val sampled_ap = sampledAP(pair._1)
                        val trip_afc = AFC(pair._2)
                        val ol_1 = min((sampled_ap._3 - sampled_ap._1).toFloat / (trip_afc._3 - trip_afc._1), 1)
                        val ot_ap = hourOfDay_long(sampled_ap._1) / 2
                        val flow_ap = flowMap.value((sampled_ap._2, sampled_ap._4))(ot_ap)
                        val ot_afc = hourOfDay_long(trip_afc._1) / 2
                        val flow_afc = flowMap.value((trip_afc._2, trip_afc._4))(ot_afc)
                        val ol_2 = flow_afc.toFloat / flow_ap
                        OL += (pair._2 -> (pair._1, (ol_1, ol_2)))
//                        if (ol_2 <= 1.0 & abs(ol_1 - ol_2) < diff){
//                            OL += (pair._2 -> (pair._1, ol_1 * gama_1 + ol_2 * (1 - gama_1)))
//                        }
//                        else{
//                            OL += (pair._2 -> (pair._1, ol_1))
//                        }
//                        val ol_3 = if (ol_2 >= 1) 0 else ol_2
//                        OL += (pair._2 -> (pair._1, ol_1 * gama_1))
                    }
                    // 首先处理存在pattern的tr_ap_afc；根据afc_pattern聚合
                    var index = Set[Int]() // 记录有对应pattern的tr_ap_afc中afc的index
                    for (pattern <- afc_pattern) {
                        val ap_seg = new ArrayBuffer[Int]()
//                        val group_scores = new ArrayBuffer[Double]()
                        val record = new ArrayBuffer[(Double, Double)]()
                        for (i <- pattern) {
                            if (OL.contains(i)) {
                                index += i
                                ap_seg.append(OL(i)._1)
                                record.append(OL(i)._2)
//                                group_scores.append(OL(i)._2)
                            }
                        }
                        // 计算每个group的得分
                        if (ap_seg.nonEmpty) {
                            val agg_trip = ap_seg.maxBy(x => AP(x)._3 - AP(x)._1)
                            val agg_ap = AP(agg_trip)
                            val trip_afc = AFC(pattern.head)
                            val v_1 = min((agg_ap._3 - agg_ap._1).toFloat / (trip_afc._3 - trip_afc._1), 1)
                            val ot_afc = hourOfDay_long(trip_afc._1) / 2
                            val fl_afc = flowMap.value((trip_afc._2, trip_afc._4))(ot_afc)
                            val ot_ap = hourOfDay_long(agg_ap._1) / 2
                            val fl_ap = flowMap.value((agg_ap._2, agg_ap._4))(ot_ap)
                            val v_2 = fl_afc.toFloat / fl_ap
                            record.append((v_1, v_2))
//                            var v = 0d
//                            if (v_2 <= 1.0 & abs(v_1 - v_2) < diff){
//                                v = v_1 * gama_1 + v_2 * (1 - gama_1)
//                            }
//                            else{
//                                v = v_1
//                            }

                            // 衰减
//                            var group_score = 0d
//                            val sort_a = group_scores.sorted
//                            for (i  <- group_scores.indices) {
//                                group_score += (gama_2 * sort_a(i) + (1 - gama_2) * v) / Math.exp(gama_3 * i)
//                            }
//                            score.append(group_score)
                            records.append(record.toList)
                        }
                    }
                    // 无pattern
                    records.append(OL.filter(x => !index.contains(x._1)).map(_._2._2).toList)
//                    score.append(OL.filter(x => !index.contains(x._1)).map(_._2._2).sum)
                }
//                Similarity = score.sum / (Q + P + R)
            }
//            val OL_str = OL.map(x=>(x._1, (x._2._1, x._2._2.formatted("%.2f"))))
//            val score_str = score.toList.map(_.formatted("%.2f"))
            else
                maybeMatch = false
            (line._1._1, (line._2._1, records.toList, Q, P, R, maybeMatch))
        }).filter(_._2._6)

        val results = new ListBuffer[(Double, Double, Double, Double)]()
        val gama_1_array = Range(9, 10, 1).map(_/10.0)
        for (gama_1 <- gama_1_array) {
            val gama_2 = 1
            val gama_3 = 0.1d
            val diff = 0.5
            val test = matchData.map(line => {
                val apID = line._1
                val afcID = line._2._1
                val data = line._2._2
                val Q = line._2._3
                val P = line._2._4
                val R = line._2._5
                var similarity = 0d
                val score = data.map(line => line.map(x => {
                    if (x._2 <= 1.0 & abs(x._2 - x._1)  <= diff) {
                        x._1 * gama_1 + x._2 * (1 - gama_1)
                    }
                    else {
                        x._1
                    }
                }))
                for (i <- score.indices) {
                    val group = score(i)
                    if (i != score.length - 1){
                        val agg_trip = group.last
                        val sample_trip = group.dropRight(1).sorted
                        for (j <- sample_trip.indices)
                            similarity += (gama_2 * sample_trip(j) + (1 - gama_2) * agg_trip) / Math.exp(gama_3 * j)
                    }
                    else {
                        similarity += group.sum
                    }
                }
                (apID, (afcID, similarity / (Q + P + R)))
            })

            val result = test.groupByKey().mapValues(_.toArray.maxBy(_._2)).map(line => {
                var flag = 0
                if (groundTruthMap.value(line._1) == line._2._1)
                    flag = 1
                (flag, 1)
            }).reduceByKey(_ + _).repartition(1).map(x => (x._1, x._2, gama_1, gama_2, gama_3, diff))

            result.saveAsTextFile(args(0) + "/zlt/UI-2021/ForLoopResult-#/" + gama_1.toString)

//            val resultMap = result.collect().toMap
//            val t = resultMap(1)
//            val f = resultMap(0)
//            results.append((gama_1, gama_2, gama_3, t.toDouble/ (t + f)))
        }


//        matchData
//            .map(x => (x._2._3, (x._1, x._2._1, x._2._4)))
//            .groupByKey()
//            .mapValues(_.toList)
//            .filter(x => x._1 > 5)
//            .collect()
//            .foreach(x => {
//                println(x._1)
//                x._2.foreach(println(_))
//            })
//        matchData.repartition(1).sortBy(_._2._3, ascending = true).saveAsTextFile(args(0) + "zlt/UI-2021/WrongMatch")

//
//        val matchResult = matchData.map(line => (line._1, line._2._1)).groupByKey().mapValues(_.toSet)
//        val result = matchResult.map(line => {
//            var flag = 0
//            if (line._2.contains(groundTruthMap.value(line._1)))
//                flag = 1
//            (flag, 1)
//        }).reduceByKey(_+_)

//        val matchResult = matchData.groupByKey().map(line => {
//            val data = line._2.toArray
//            val mostMatchTwo = data.sortBy(_._2).reverse.take(2).toList
////            val mostMatch = data.maxBy(_._2)
////            val i = data.indexWhere(x => x._1 == groundTruthMap.value(line._1))
////            val realOne = if (i >= 0 )  data(i) else mostMatch
//            (line._1, mostMatchTwo.head, "-------", mostMatchTwo.last)
//        }).filter(x => x._2._1 == groundTruthMap.value(x._1))
//        matchResult.repartition(1).saveAsTextFile(args(0) + "/zlt/UI-2021/CorrectMatchAnalysis")

//        val resultMap = matchData.groupByKey().mapValues(_.toArray.maxBy(_._2)).map(line => {
//            var flag = 0
//            if (groundTruthMap.value(line._1) == line._2._1) {
//                flag = 1
//            }
//            (flag, 1)
//        }).reduceByKey(_ + _).collect().toMap
//        println(gama_1, gama_2, gama_3, diff, resultMap(1).toFloat / (resultMap(0) + resultMap(1)))
//        println(resultMap)

        sc.stop()
    }

    // 高斯核函数
    def RBF(l: Long, x: Long, h: Int): Double = {
        1 / sqrt(2 * Pi) * exp(-pow(x - l, 2) / (2 * pow(h, 2)))
    }

    // 计算z_score自动选取聚类中心
    def z_score(dens_pos: Array[(Double, Long)]): Array[(Int, Long)] = {
        val dist_r = compute_dist(dens_pos)
        val dist_l = compute_dist(dens_pos.reverse).reverse
        val dist_dens_pos = new ArrayBuffer[(Long, Double, Long)]()
        for (i <- dist_r.indices) {
            if (dist_r(i) == -1 && dist_l(i) == -1)
                dist_dens_pos.append((dens_pos.last._2 - dens_pos.head._2, dens_pos(i)._1, dens_pos(i)._2))
            else if (dist_r(i) != -1 && dist_l(i) != -1)
                dist_dens_pos.append((min(dist_r(i), dist_l(i)), dens_pos(i)._1, dens_pos(i)._2))
            else if (dist_l(i) != -1)
                dist_dens_pos.append((dist_l(i), dens_pos(i)._1, dens_pos(i)._2))
            else
                dist_dens_pos.append((dist_r(i), dens_pos(i)._1, dens_pos(i)._2))
        }
        var sum_dist = 0L
        var sum_dens = 0d
        dist_dens_pos.foreach(x => {
            sum_dist += x._1
            sum_dens += x._2
        })
        val avg_dist = sum_dist / dist_dens_pos.length
        val avg_dens = sum_dens / dist_dens_pos.length
        var total = 0d
        for (v <- dist_dens_pos) {
            total += pow(abs(v._1 - avg_dist), 2) + pow(abs(v._2 - avg_dens), 2)
        }
        val sd = sqrt(total / dist_dens_pos.length)
        val z_score = new ArrayBuffer[((Long, Double, Long), Double)]()
        var z_value = 0d
        for (v <- dist_dens_pos) {
            z_value = sqrt(pow(abs(v._1 - avg_dist), 2) + pow(abs(v._2 - avg_dens), 2)) / sd
            z_score.append((v, z_value))
        }
        val result = new ArrayBuffer[(Int, Long)]()
        // z-score大于3认为是类簇中心
        val clustersInfo = z_score.toArray.filter(_._2 >= 3)
        for (i <- clustersInfo.indices) {
            result.append((i + 1, clustersInfo(i)._1._3))
        }
        result.toArray
    }

    // 计算相对距离
    def compute_dist(info: Array[(Double, Long)]): Array[Long] = {
        val result = new Array[Long](info.length)
        val s = mutable.Stack[Int]()
        s.push(0)
        var i = 1
        var index = 0
        while (i < info.length) {
            if (s.nonEmpty && info(i)._1 > info(s.top)._1) {
                index = s.pop()
                result(index) = abs(info(i)._2 - info(index)._2)
            }
            else {
                s.push(i)
                i += 1
            }
        }
        while (s.nonEmpty) {
            result(s.pop()) = -1
        }
        result
    }
}
