import GeneralFunctionSets.{dayOfMonth_long, secondsOfDay, transTimeToTimestamp}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.math.abs

object OverlapCount {
    def main(args: Array[String]): Unit = {
        /**
         * 统计高峰期经过某个OD的其他OD的重叠情况
         */
        val spark = SparkSession.builder()
            .appName("OverlapCount")
            .getOrCreate()
        val sc = spark.sparkContext

        // 读取地铁站点名和编号映射关系
        val stationFile = sc.textFile(args(0) + "/zlt/AllInfo/stationInfo-UTF-8.txt")
        val stationName2NoRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationName, stationNo.toInt)
        })
        val stationInfo = stationName2NoRDD.collect()
        val stationName2No = sc.broadcast(stationInfo.toMap)
        val stationNo2Name = sc.broadcast(stationInfo.map(x => (x._2, x._1)).toMap)

        // 读取所有有效路径的数据
        val validPathFile = sc.textFile(args(0) + "/zlt/AllInfo/allpath.txt").map(line => {
            // 仅保留站点编号信息
            val fields = line.split(' ').dropRight(5)
            val sou = fields(0).toInt
            val des = fields(fields.length - 1).toInt
            val pathStations = new ListBuffer[Int]
            fields.foreach(x => pathStations.append(x.toInt))
            ((sou, des), pathStations.toList)
        }).groupByKey().mapValues(_.toList)

        // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
        val validPathMap = sc.broadcast(validPathFile.collect().toMap)

        // 读取站间时间间隔
        val readODTimeInterval = sc.textFile(args(0) + "/zlt/UI/AllODTimeInterval/ShortPathTime/part-00000").map(line => {
            val p = line.split(',')
            val sou = stationName2No.value(p(0).drop(1))
            val des = stationName2No.value(p(1))
            val interval = p(2).dropRight(1).toLong // sec
            ((sou, des), interval)
        })
        val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

        val s = "坪洲,深大,8,9,16,7,7,12,13,7,24,17,22,25,30,37,44,37,46,30,25,31,37,23,24,33,3,5,5,5,4,6,5,13,5,6,8,8,8,14,11,13,10,13,4,10,9,7,6,11,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,11,13,36,54,74,72,135,157,192,180,168,169,214,221,187,164,148,131,140,148,154,116,121,90,11,20,40,43,92,66,111,150,196,178,186,152,218,182,206,156,144,131,130,139,121,146,127,115,11,16,37,59,59,83,118,151,180,171,172,166,184,195,161,165,123,118,123,129,140,126,122,85,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,8,3,3,8,5,8,7,3,3,1,7,8,5,7,6,5,10,9,3,4,3,8,3,9,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,21,30,63,73,90,112,152,186,164,176,179,168,194,202,173,129,112,130,127,111,123,98,68,10,16,37,63,62,84,120,156,164,182,170,165,173,201,187,178,129,118,98,125,121,134,156,99,14,21,36,63,62,78,131,173,192,177,163,159,174,194,179,182,130,150,140,133,143,153,128,116,10,20,30,66,58,68,120,155,161,168,112,100,122,123,117,128,147,137,153,156,175,171,167,143,7,20,38,49,68,85,110,158,217,169,154,172,160,203,212,164,134,150,140,133,133,146,116,97,10,5,13,14,21,26,25,23,28,43,24,31,38,44,48,48,49,35,33,34,30,44,33,25,5,2,6,7,9,6,4,8,9,6,8,13,16,15,8,11,16,8,7,10,6,11,9,3,12,19,40,63,69,107,146,151,176,181,171,175,178,206,212,178,131,145,126,144,138,133,100,92,18,19,37,58,76,87,136,160,185,187,167,143,196,197,194,162,149,146,137,115,149,128,104,101,11,13,45,54,66,81,123,168,186,188,175,161,172,181,195,201,139,146,113,139,139,120,121,86,12,15,37,60,67,85,136,147,189,172,134,170,172,163,204,171,165,144,149,153,157,152,92,100,12,14,45,56,73,79,117,165,175,176,169,169,163,179,196,183,142,150,127,109,149,141,123,76,5,9,15,11,9,13,16,17,22,34,37,34,21,34,41,45,41,39,20,25,30,27,36,36,4,4,6,11,7,6,14,7,8,14,18,9,10,8,11,6,10,11,8,4,13,6,10,9,10,13,40,38,72,95,112,154,205,180,161,203,200,206,192,162,141,126,131,120,149,130,112,81,10,23,35,59,66,84,127,158,198,184,169,163,205,194,223,155,148,141,124,128,153,127,119,94,3,13,37,54,77,84,91,156,187,201,172,171,201,190,213,172,142,138,130,121,140,152,99,136,8,21,33,54,71,89,117,166,180,190,170,155,173,181,203,192,161,134,143,136,135,120,122,90,11,15,35,49,71,89,110,154,189,182,169,155,172,189,182,179,154,133,123,125,140,138,124,84,9,9,10,11,6,15,14,19,24,25,37,37,42,44,45,45,40,39,20,33,36,27,30,28,7,3,6,5,6,2,8,3,6,10,15,9,8,11,16,15,4,10,5,11,10,8,11,9"
        val fields = s.split(",")
        val os = stationName2No.value(fields(0))
        val ds = stationName2No.value(fields(1))
        val flow = fields.drop(2).map(_.toInt)
        val max_flow_index = flow.indexWhere(x => x == flow.max)
        val ot_day = max_flow_index / 24
        val ot_min = max_flow_index % 24 * 5 * 60 + 3600 * 7


        // (667979926,2019-06-04 08:42:22,坪洲,21,2019-06-04 08:55:23,宝安中心,22)
        val afcData = sc.textFile(args(0) + "Destination/subway-pair/part-*").map(line => {
            val fields = line.split(",")
            val os = stationName2No.value(fields(2))
            val ds = stationName2No.value(fields(5))
            val ot = transTimeToTimestamp(fields(1))
            val day = dayOfMonth_long(ot)
            val sec = secondsOfDay(ot)
            ((os, ds), day, sec)
        }).filter(_._2 == ot_day).filter(line => {
            val paths = validPathMap.value(line._1)
            var flag_1 = true
            for (path <- paths if flag_1) {
                val index_o = path.indexWhere(_ == os)
                val index_d = path.indexWhere(_ == ds)
                if (index_o >= 0 & index_d > index_o)
                    flag_1 = false
            }
            var flag_2 = false
            if (abs(line._3 + ODIntervalMap.value((line._1._1, os)) - ot_min) <= 300)
                flag_2 = true
            !flag_1 & flag_2
        }).map(x => ((stationNo2Name.value(x._1._1), stationNo2Name.value(x._1._2)), 1)).reduceByKey(_ + _)

        afcData.repartition(1).sortBy(_._2, ascending = false).saveAsTextFile(args(0) + "zlt/UI/OverlapCount")
        sc.stop()
        //        print(flow.max, "**********************")
    }
}
