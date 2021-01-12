import org.apache.spark.sql.SparkSession
import GeneralFunctionSets.hourOfDay

import scala.collection.mutable.ListBuffer

object FlowDistributionOfOverlapSection {
    def main(args: Array[String]): Unit = {
        /**
         * 统计每个可能存在重叠的片段的流量情况 以出发时间划分每2h一个时段
         */
        val spark = SparkSession
            .builder()
            .appName("FlowDistributionOfOverlapSection")
            .getOrCreate()
        val sc = spark.sparkContext

        // 读取地铁站点名和编号映射关系 "1,机场东,22.647011,113.8226476,1268036000,268"
        val stationFile = sc.textFile(args(0) + "zlt/AllInfo/stationInfo-UTF-8.txt")
        val stationNoToNameRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationNo.toInt, stationName)
        })
        val stationNoToName = sc.broadcast(stationNoToNameRDD.collect().toMap)

        // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"
        // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
        val shortestPath = sc.textFile(args(0) + "zlt/AllInfo/allpath.txt").map(line => {
            val fields = line.split(' ')
            val stations = fields.dropRight(5)
            val info = fields.takeRight(5)
            val sou = stationNoToName.value(stations(0).toInt)
            val des = stationNoToName.value(stations(stations.length-1).toInt)
            val time = info.last.toFloat
            val pathStations = new ListBuffer[String]
            stations.foreach(x => pathStations.append(stationNoToName.value(x.toInt)))
            ((sou, des), (pathStations.toList, time))
        }).groupByKey().mapValues(_.toList.minBy(_._2)._1)

        val shortestPathMap = sc.broadcast(shortestPath.collect().toMap)

        // (323564395,2019-06-20 21:40:31,莲花村,21,2019-06-20 21:52:29,益田,22)
        val AFCFile = sc.textFile(args(0) + "Destination/subway-pair/part-*").map(line => {
            val fields = line.split(",")
            val ot = hourOfDay(fields(1)) / 2
            val os = fields(2)
            val ds = fields(5)
            ((os, ds), ot)
        }).groupByKey().mapValues(v => {
            val data = v.toList
            val countOt = Array.ofDim[Int](12)
            for (i <- 0.until(12))
                countOt(i) = data.count(_==i)
            countOt
        })

        // 切分每个afc trip的所有覆盖的片段
        val segmentsFlow = AFCFile.flatMap(line => {
            val path = shortestPathMap.value.getOrElse(line._1, List.empty)
            val otArray = line._2
            val segments = new ListBuffer[(String, String)]
            if (path.nonEmpty){
                val len = path.length
                for (i <- 0.until(len-1)){
                    for (j <- (i+1).until(len)){
                        segments.append((path(i), path(j)))
                    }
                }
            }
            for (s <- segments) yield
                (s, otArray)
        })


        val res = segmentsFlow.groupByKey().mapValues(v => {
            val data = v.toArray
            val merge = data.reduce((x,y) => x.zip(y).map(x=>x._1 + x._2))
            merge
        }).map(line=>{
            line._1._1 + "," + line._1._2 + "," + line._2.mkString(",")
        })

        res.repartition(1).saveAsTextFile(args(0) + "zlt/UI/FlowDistributionOfOverlapSection")
        sc.stop()
    }
}
