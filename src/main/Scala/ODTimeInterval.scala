import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.math.abs

object ODTimeInterval {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("ODTimeInterval")
        val sc = new SparkContext(conf)


        // 读取地铁站点名和编号映射关系
        val stationFile = sc.textFile(args(0))
        val stationNoToNameRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationNo.toInt, stationName)
        })
        val stationNoToName = sc.broadcast(stationNoToNameRDD.collect().toMap)

        // 读取最短路径的时间信息
        val shortestPath = sc.textFile(args(1)).map(line => {
            val fields = line.split(' ')
            val sou = stationNoToName.value(fields(0).toInt)
            val des = stationNoToName.value(fields(1).toInt)
            // 换算成秒
            val time = (fields(2).toFloat * 60).toLong
            (sou, des, time)
        }).cache()

        // 保存最短路径时间
        shortestPath.repartition(1).saveAsTextFile(args(2) + "/ShortPathTime")
        // 给最短路径时间设置标志0，为了后面补充统计实际最短时间时不完整的情况
        val shortestPathTag = shortestPath.map(x => ((x._1, x._2), (x._3, 0)))
        // 转换成map便于查询
        val shortestPathTime = sc.broadcast(shortestPathTag.collect().toMap)


        val readODFile = sc.textFile(args(3)).map(line => {
            val fields = line.split(',')
            val cardid = fields(0).drop(1)
            val time = transTimeFormat(fields(1))
            val station = fields(2)
            val tag = fields(3).dropRight(1)
            (cardid, (time, station, tag))
        }).groupByKey().mapValues(_.toList.sortBy(_._1))

        val extractOD = readODFile.flatMap(line => {
            val ODArray = line._2
            val ODList = new ListBuffer[((String, String), Long)]
            var index = 0
            while (index + 1 < ODArray.length) {
                val stationO = ODArray(index)
                val stationD = ODArray(index + 1)
                if (stationO._3 == "21" && stationD._3 == "22" && stationD._1 - stationO._1 < shortestPathTime.value((stationO._2, stationD._2))._1 + 1200) {
                    val realTime = abs(ODArray(index)._1 - ODArray(index + 1)._1)
                    ODList.append(((ODArray(index)._2, ODArray(index + 1)._2), realTime))
                    index += 1
                }
                index += 1
            }

            for (v <- ODList) yield {
                v
            }
        })

        // 保存全部由刷卡时间统计的OD时间间隔及频次
        val groupByOD = extractOD.groupByKey().mapValues(_.toList).map(line => {
            val count = line._2.length
            val sum = line._2.sum
            (line._1._1, line._1._2, sum / count, count)
        }).repartition(1).sortBy(_._4, ascending = false).cache()

        groupByOD.saveAsTextFile(args(2) + "/AFCStatistics")

        val unionData = groupByOD.filter(_._4 > 200).map(line => ((line._1, line._2), (line._3, 1))).union(shortestPathTag).groupByKey().map(line => {
            var time = 0L
            var tag = ""
            val detail = line._2.toList.sortBy(_._2)
            // tag = "0"表示采用的最短路径时间作为OD时间间隔
            if (detail.length == 1 && detail.head._2 == 0) {
                time = line._2.head._1
                tag = "0"
            }
            // tag = "1"表示采用的统计刷卡时间得出的OD时间间隔
            else if (detail.length == 2 && detail.head._2 == 0 && detail.last._2 == 1) {
                time = detail.last._1
                tag = "1"
            }
            (line._1._1, line._1._2, time, tag)
        }).repartition(1).sortBy(_._1)

        unionData.saveAsTextFile(args(2) + "/CompleteTimeInterval")
        sc.stop()
    }

    def transTimeFormat(timeString: String): Long = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString).getTime / 1000
        time
    }
}
