import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.math.{max, min, pow}


object Verification {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Verification")
        val sc = new SparkContext(conf)

        val CandidatesFile = sc.textFile(args(0))
        val candidatesRDD = CandidatesFile.map(line => {
            val fields = line.split(',')
            val TAID = fields(0).drop(1)
            val TBID = fields(1).drop(1)
            val station = fields(2)
            val cnt = fields(3).toDouble
            val score = fields(4).dropRight(2).toDouble
            (TAID, TBID, station, cnt, score)
        })

        val stationFile = sc.textFile(args(1))
        val stationRDD = stationFile.map(line => {
            val fields = line.split(',')
            val stationName = fields(1)
            val lat = fields(2).toDouble
            val lon = fields(3).toDouble
            (stationName, (lon, lat))
        })

        // 声明为广播变量，用于根据站点名查询经纬度
        val stationInfo = sc.broadcast(stationRDD.collect())


        // 转换为(TA.id, TB.id, list(TA.id, TB.id, station, cnt, score))
        val groupedRDD = candidatesRDD.groupBy(x => (x._1, x._2)).mapValues(_.toList).filter(_._2.length >= 3)

        // 转换为(TA.id, TB.id, SIG, WJS)
        val transformedRDD = groupedRDD.map(line => {
            val TAID = line._1._1
            val TBID = line._1._2
            val co_occurrences = line._2
            var ob: Map[String, Double] = Map()
            var st: Map[String, Double] = Map()
            val stationMap = stationInfo.value.toMap
            val K = new ListBuffer[String]
            var md: Map[String, Double] = Map()
            for (x <- co_occurrences.indices) {
                ob += (co_occurrences(x)._3 -> sigmoidFunction(co_occurrences(x)._4, 16, 0.2))

                // 初始化第一个站点的st值
                if (x == 0)
                    st += (co_occurrences(x)._3 -> ob(co_occurrences(x)._3))
                else {
                    if (K.isEmpty)
                        st += (co_occurrences(x)._3 -> max(0.0, ob(co_occurrences(x)._3)))
                    else {
                        var tempResult = 0.0
                        var min_distance = 10000.0
                        val gps_x = stationMap.get(co_occurrences(x)._3).get
                        for (l <- K.indices) {
                            val gps_l = stationMap.get(K(l)).get
                            val dist_l_x = getDistance(gps_l._1, gps_l._2, gps_x._1, gps_x._2)
                            min_distance = min(min_distance, dist_l_x)
                            tempResult += (st(K(l)) * pow(0.4, dist_l_x))
                        }
                        md += (co_occurrences(x)._3 -> min_distance)
                        st += (co_occurrences(x)._3 -> max(0.0, ob(co_occurrences(x)._3) - tempResult))
                    }
                }
                if (st(co_occurrences(x)._3) > 0)
                    K.append(co_occurrences(x)._3)
            }

            var SIG = 0.0
            SIG = st(co_occurrences.head._3)
            if (K.nonEmpty) {
                for (x <- K.indices) {
                    if (K(x) != co_occurrences.head._3)
                        SIG += (st(K(x)) * (1 + sigmoidFunction(md(K(x)), 50, 1.0 / 4000)))
                }
            }
            (TAID, TBID, SIG)
        }).sortBy(_._3, ascending = false)

        transformedRDD.saveAsTextFile(args(2))
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
