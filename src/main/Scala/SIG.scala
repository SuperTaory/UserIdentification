// import org.apache.log4j.{Level, Logger}

import GeneralFunctionSets.transTimeToTimestamp
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.math.{max, min, pow}

object SIG {
    def main(args: Array[String]): Unit = {
        //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

        val spark = SparkSession.builder().appName("SIG").getOrCreate()
        val sc = spark.sparkContext

        /*-----------------------------------------------------------------------------------------*/

        // 开始处理OD数据集,统计每个站点的客流量和提取某个乘客的OD信息
        // (669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
        val ODFileRDD = sc.textFile(args(0) + "/zlt/UI-2021/GroundTruth/AFCData/part-*").map(line => {
            val fields = line.split(",")
            val pid = fields(0).drop(1)
            val stations = new ListBuffer[String]
            stations.append(fields(2))
            stations.append(fields(5))
            (pid, stations.toList)
        }).flatMap(x => for (v <- x._2) yield ((x._1, v), 1))

        //按照站点和用户ID分组，统计一个站点内该ID出现的次数, 生成(station, id, cnt)
        val OD_cntRDD = ODFileRDD.reduceByKey(_+_)
            .map(line => (line._1._2, (line._1._1, line._2)))
            .groupByKey()
            .mapValues(_.toList)

        val OD_cntMap = sc.broadcast(OD_cntRDD.collect().toMap)

        //    OD_cntRDD.repartition(1).saveAsTextFile(args(2))

        // 读取站点客流量文件
        // (车公庙,1617160)
        val PassengerFlowOfStation = sc
            .textFile(args(0) + "/zlt/UI/PassengerFlow/part-00000")
            .map(x => (x.split(",").head.drop(1), x.split(",").last.dropRight(1).toInt))
            .cache()

        val MinFlow = PassengerFlowOfStation.collect().minBy(_._2)._2.toDouble

        // 对各个站点根据其客流量设置打分参数:rsz
        val RankScoreOfStation = PassengerFlowOfStation.map(line => {
            val score = MinFlow / line._2
            (line._1, score)
        })

        // 声明为广播变量用于查询
        val scoreInfo = sc.broadcast(RankScoreOfStation.collect().toMap)

        /*-----------------------------------------------------------------------------------------*/

        // 读取mac数据
        // (1C48CE5E485B,2019-06-12 11:34:26,高新园,202,2019-06-12 12:08:37,老街,201)
        // (00027EF9CD6F,2019-06-01 08:49:11,固戍,452,2019-06-01 09:16:29,洪浪北,150,2019-06-01 08:49:11,固戍,2019-06-01 08:58:50,宝安中心)
        val macFile = sc.textFile(args(0) + "/zlt/UI-2021/GroundTruth/SampledAPData-" + args(1) + "%/*").map(line => {
            val fields = line.split(',')
            val macId = fields(0).drop(1)
            val samp_os = fields(8)
            val samp_ds = fields(10).dropRight(1)
            val stations = new ListBuffer[String]
            stations.append(samp_os)
            stations.append(samp_ds)
            (macId, stations.toList)
        }).flatMap(x => for (v <- x._2) yield ((x._1, v), 1))


        // 按照站点和macID分组，统计一个站点内该macID出现的次数, 生成(station, id, cnt)
        val macID_cntRDD = macFile.reduceByKey(_+_).map(line => (line._1._2, (line._1._1, line._2)))

        //macID_cntRDD.saveAsTextFile(args(3))

        /*-----------------------------------------------------------------------------------------*/

        // 生成(ap.id, (afc.id, station, cnt))
        val mergedStaCnt = macID_cntRDD.flatMap(line => {
            for (v <- OD_cntMap.value(line._1)) yield {
                (line._2._1, (v._1, line._1, min(v._2, line._2._2)))
            }
        })

        // Increase the ranking score of TB.id by (rsz * o)
        val CandidateSet = mergedStaCnt.map(line => {
            val station = line._2._2
            val rsz = scoreInfo.value(station)
            val score = line._2._3 * rsz
            ((line._1, line._2._1), (line._2._2, line._2._3, score))
        })

        //val topQ_Candidates = sc.makeRDD(CandidateSet)
        //topQ_Candidates.repartition(1).saveAsTextFile(args(3))
        //    CandidateSet.saveAsTextFile(args(4))

        /*-----------------------------------------------------------------------------------------*/
        // 读取站点经纬度信息
        // 1,机场东,22.647011,113.8226476,1268036000,268
        val stationFile = sc.textFile(args(0) + "/zlt/AllInfo/stationInfo-UTF-8.txt")
        val stationRDD = stationFile.map(line => {
            val fields = line.split(',')
            val stationName = fields(1)
            val lat = fields(2).toDouble
            val lon = fields(3).toDouble
            (stationName, (lon, lat))
        })

        // 声明为广播变量，用于根据站点名查询经纬度
        val stationInfo = sc.broadcast(stationRDD.collect().toMap)

        // 读取groundTruth计算Accuracy
        // (251449740,ECA9FAE07B4F,26.857,43,0.6245814)
        val groundTruthData = sc.textFile(args(0) + "/zlt/UI-2021/GroundTruth/IdMap/part-*").map(line => {
            val fields = line.split(",")
            val afcId = fields(0).drop(1)
            val apId = fields(1)
            (apId, afcId)
        })
        val groundTruthMap = sc.broadcast(groundTruthData.collect().toMap)

        // 转换为(TA.id, TB.id, list(station, cnt, score)),并过滤出参与计算的候选集合
        val groupedRDD = CandidateSet.groupByKey().mapValues(_.toList)

        // 转换为(TA.id, TB.id, SIG)
        val transformedRDD = groupedRDD.map(line => {
            val apID = line._1._1
            val afcID = line._1._2
            val co_occurrences = line._2
            var ob: Map[String, Double] = Map()
            var st: Map[String, Double] = Map()
            val stationMap = stationInfo.value
            val K = new ListBuffer[String]
            var md: Map[String, Double] = Map()
            for (x <- co_occurrences.indices) {
                ob += (co_occurrences(x)._1 -> sigmoidFunction(co_occurrences(x)._2, 16, 0.2))

                // 初始化第一个站点的st值
                if (x == 0)
                    st += (co_occurrences(x)._1 -> ob(co_occurrences(x)._1))
                else {
                    if (K.isEmpty)
                        st += (co_occurrences(x)._1 -> max(0.0, ob(co_occurrences(x)._1)))
                    else {
                        var tempResult = 0.0
                        var min_distance = 10000.0
                        val gps_x = stationMap.get(co_occurrences(x)._1).get
                        for (l <- K.indices) {
                            val gps_l = stationMap.get(K(l)).get
                            val dist_l_x = getDistance(gps_l._1, gps_l._2, gps_x._1, gps_x._2)
                            min_distance = min(min_distance, dist_l_x)
                            tempResult += (st(K(l)) * pow(0.4, dist_l_x))
                        }
                        md += (co_occurrences(x)._1 -> min_distance)
                        st += (co_occurrences(x)._1 -> max(0.0, ob(co_occurrences(x)._1) - tempResult))
                    }
                }
                if (st(co_occurrences(x)._1) > 0)
                    K.append(co_occurrences(x)._1)
            }

            var SIG = 0.0
            SIG = st(co_occurrences.head._1)
            if (K.nonEmpty) {
                for (x <- K.indices) {
                    if (K(x) != co_occurrences.head._1)
                        SIG += (st(K(x)) * (1 + sigmoidFunction(md(K(x)), 50, 1.0 / 4000)))
                }
            }
            (apID, (afcID, SIG))
        })
        val rank = transformedRDD.groupByKey().mapValues(_.toArray.sortBy(_._2).takeRight(20).map(_._1).mkString("-"))
        rank.repartition(1).saveAsTextFile(args(0) + "zlt/UI-2021/SIG_rank/" + args(1) + "%")

//        val result = transformedRDD.groupByKey().mapValues(x => x.toList.maxBy(_._2)).map(line => {
//            var flag = 0
//            if (groundTruthMap.value(line._1).equals(line._2._1))
//                flag = 1
//            (flag, 1)
//        }).reduceByKey(_ + _).repartition(1).map(x => (x._1, x._2, args(1).toFloat.formatted("%.0f%%")))
//        result.saveAsTextFile(args(0) + "zlt/UI-2021/SIG/" + args(1) + "%")

        sc.stop()
    }

    def sigmoidFunction(v: Double, para1: Double, para2: Double): Double = {
        val denominator = 1 + math.exp(-v * para2)
        para1 / denominator - para1 / 2
    }

    // 计算GPS距离，单位为km
    def getDistance(lng1: Double, lat1: Double, lng2: Double, lat2: Double): Double = {
        val EARTH_RADIUS = 6378.137
        val radLat1 = lat1 * Math.PI / 180.0
        val radLat2 = lat2 * Math.PI / 180.0
        val a = radLat1 - radLat2
        val b = lng1 * Math.PI / 180.0 - lng2 * Math.PI / 180.0
        var s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
        s = s * EARTH_RADIUS
        s
    }

}
