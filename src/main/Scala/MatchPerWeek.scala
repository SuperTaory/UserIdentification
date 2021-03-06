import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.max

object MatchPerWeek {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MatchPerWeek")
        val sc = new SparkContext(conf)

        // 读取地铁站点名和编号映射关系
        val stationFile = sc.textFile(args(0))
        val stationNoToName = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationNo, stationName)
        }).collect().toMap

        val stationNameToNo = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationName, stationNo)
        }).collect().toMap

        // 读取所有有效路径的数据
        val validPathFile = sc.textFile(args(1)).map(line => {
            // 仅保留站点编号信息
            val fields = line.split(' ').dropRight(5)
            val sou = fields(0)
            val des = fields(fields.length - 1)
            ((sou, des), fields)
        })

        // 键-OD站点编号，值-OD之间的所有有效路径，由编号组成
        val validPathSet = validPathFile.groupByKey().mapValues(_.toList).collect().toMap

        // 读取乘客的OD记录
        val personalOD = sc.textFile(args(2)).filter(line => line.substring(1, 10) == args(3)).map(line => {
            val fields = line.split(',')
            val pid = fields(0).drop(1)
            val time = transTimeFormat(fields(1))
            val station = fields(2)
            val tag = fields(3).dropRight(1)
            val weeks = getWeek(time)
            (weeks, (pid, time, station, tag))
        }).groupByKey().mapValues(_.toList.sortBy(_._2))

        val ODInfo = personalOD.collect().toMap


        // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
        var perODMap: mutable.Map[(String, String), List[List[String]]] = mutable.Map()
        // 将OD之间的有效路径涵盖的站点处理为Set集合，OD-pair作为键
        var validPathStationSet: mutable.Map[(String, String), mutable.Set[String]] = mutable.Map()
        // 存储去重后的OD-pair
        val ODPairSet: mutable.Set[(String, String)] = mutable.Set()

        // 循环处理每一周的数据
        for (weekInfo <- ODInfo) {
            val ODArray = weekInfo._2

            var index = 0
            while (index + 1 < ODArray.length) {
                if (ODArray(index)._4 == "21" && ODArray(index + 1)._4 == "22") {
                    // 把站点名转换为编号
                    val so = ODArray(index)._3
                    val sd = ODArray(index + 1)._3
                    ODPairSet.add((stationNameToNo(so), stationNameToNo(sd)))
                    index += 1
                }
                index += 1
            }
        }

        ODPairSet.foreach(x => {
            val od = (stationNoToName(x._1), stationNoToName(x._2))
            val vp = validPathSet(x)
            val paths = new ListBuffer[List[String]]
            val temp_set: mutable.Set[String] = mutable.Set()
            for (v <- vp) {
                val path = new ListBuffer[String]
                for (p <- v) {
                    path.append(stationNoToName(p))
                }
                paths.append(path.toList)
                temp_set.++=(path.toSet)
            }
            perODMap += (od -> paths.toList)
            validPathStationSet += (od -> temp_set)
        })


        // 读取mac数据
        val macFile = sc.textFile(args(4)).map(line => {
            val fields = line.split(',')
            val macId = fields(0).drop(1)
            val time = fields(1).toLong
            val station = fields(2).dropRight(1)
            val weeks = getWeek(time)
            ((macId, weeks), (time, station))
        })

        val groupedMacInfo = macFile.groupByKey().mapValues(_.toList.sortBy(_._1))

        //    println(groupedMacInfo.count())

        // 过滤掉与smartcard的OD时间地点冲突的MacID
        val removeConflict = groupedMacInfo.filter(line => {
            var flag = true
            val weeks = line._1._2
            val macArray = line._2
            if (ODInfo.keys.toSet.contains(weeks)) {
                val ODArray = ODInfo(weeks)
                for (a <- ODArray if flag) {
                    val l = macArray.indexWhere(_._1 >= a._2 - 60)
                    val r = macArray.lastIndexWhere(_._1 <= a._2 + 60)
                    if (l != -1 && r != -1) {
                        for (i <- l.to(r) if flag) {
                            if (macArray(i)._2 == a._3)
                                flag = true
                            else
                                flag = false
                        }
                    }
                }
            }
            flag
        })
        //    println(removeConflict.count())


        val rankedScoreOfWeek = removeConflict.map(line => {
            var score = 0f
            val macArray = line._2
            val weeks = line._1._2

            if (ODInfo.keys.toSet.contains(weeks)) {
                var index = 0
                val ODArray = ODInfo(weeks)
                while (index + 1 < ODArray.length) {
                    if (ODArray(index)._4 == "21" && ODArray(index + 1)._4 == "22" && ODArray(index + 1)._2 - ODArray(index)._2 < 10800) {
                        val so = ODArray(index)._3
                        val sd = ODArray(index + 1)._3
                        val to = ODArray(index)._2
                        val td = ODArray(index + 1)._2
                        val paths = perODMap((so, sd))
                        val pathStationSet = validPathStationSet((so, sd))
                        val l = macArray.indexWhere(_._1 > to - 60)
                        val r = macArray.lastIndexWhere(_._1 < td + 60)
                        val macStationSet: mutable.Set[String] = mutable.Set()
                        if (l >= 0 && r >= l) {
                            for (i <- l.to(r))
                                macStationSet.add(macArray(i)._2)
                            if (pathStationSet.union(macStationSet).size == pathStationSet.size) {
                                var temp_score = 0f
                                var index_mac = l
                                for (path <- paths) {
                                    var path_score = 0f
                                    val coincideList = new ListBuffer[Int]
                                    for (station <- path if index_mac <= r) {
                                        if (macArray(index_mac)._2.equals(station)) {
                                            index_mac += 1
                                            coincideList.append(path.indexWhere(_ == station))
                                        }
                                    }
                                    // 计算最大跨度
                                    if (coincideList.length == 1)
                                        path_score = 1f / path.length
                                    else if (coincideList.length >= 2)
                                        path_score = (coincideList.last - coincideList.head + 1).toFloat / path.length
                                    temp_score = max(temp_score, path_score)
                                }
                                score += temp_score
                            }
                        }
                        index += 1
                    }
                    index += 1
                }
            }
            (line._1._1, (weeks, score))
        })

        val mergeWeekScore = rankedScoreOfWeek.groupByKey().mapValues(_.toList.sortBy(_._1))

        val monthScore = mergeWeekScore.map(line => {
            var total = 0f
            //      val score = Array(0f, 0f, 0f, 0f, 0f)
            var detail = ""
            line._2.foreach(x => {
                detail += x._1.toString + '-' + x._2.toString + ','
                total += x._2
            })
            //      (line._1, score(0), score(1), score(2), score(3), score(4), total)
            (line._1, detail.dropRight(1), total)
        }).sortBy(_._3, ascending = false).take(50)

        val result = sc.parallelize(monthScore, 1)

        result.saveAsTextFile(args(5))
        sc.stop()
    }

    def transTimeFormat(timeString: String): Long = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString).getTime / 1000
        time
    }

    def getWeek(t: Long): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.WEEK_OF_MONTH)
    }
}