import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.math.abs

// 对AP数据划分出行片段，将出行片段包含站点数量小于3个的出行片段去除，然后再flatMap
object DivisionAndSampling {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("DivisionAndSampling")
        val sc = new SparkContext(conf)

        val readMacFile = sc.textFile(args(0)).map(line => {
            val fields = line.split(',')
            val macId = fields(0).drop(1)
            val time = fields(1).toLong
            val station = fields(2).dropRight(1)
            (macId, (time, station))
        }).groupByKey().mapValues(_.toList.sortBy(_._1))

        val ODTimeInterval = sc.textFile(args(1)).map(line => {
            val p = line.split(',')
            val sou = p(0).drop(1)
            val des = p(1)
            val interval = p(2).dropRight(1).toLong
            ((sou, des), interval)
        })
        val ODIntervalMap = sc.broadcast(ODTimeInterval.collect().toMap)

        // 先划分出行片段并过滤掉出行片段过小的数据
        val divisionRDD = readMacFile.flatMap(line => {
            val MacId = line._1
            val data = line._2
            val segement = new ListBuffer[(Long, String)]
            val segements = new ListBuffer[List[(Long, String)]]
            for (s <- data) {
                if (segement.isEmpty) {
                    segement.append(s)
                }
                else {
                    // 遇到前后相邻为同一站点进行划分
                    if (s._2 == segement.last._2) {
                        segements.append(segement.toList)
                        segement.clear()
                    }
                    // 前后相邻站点相差时间超过阈值进行划分
                    else if (abs(s._1 - segement.last._1) > ODIntervalMap.value((segement.last._2, s._2)) + 1500) {
                        segements.append(segement.toList)
                        segement.clear()
                    }
                    // 前后相邻站点相差时间小于阈值进行划分
                    else if (abs(s._1 - segement.last._1) < ODIntervalMap.value((segement.last._2, s._2)) * 0.6) {
                        segements.append(segement.toList)
                        segement.clear()
                    }
                    segement.append(s)
                }
            }
            segements.append(segement.toList)
            for (seg <- segements) yield {
                (MacId, seg)
            }
        }).filter(x => x._2.length > 3)
        //    divisionRDD.sortBy(x => (x._1, x._2.head._1)).saveAsTextFile(args(2))

        // 生成NormalMacData
        val result = divisionRDD.flatMap(line => {
            for (v <- line._2) yield {
                (line._1, v._1, v._2)
            }
        })

        result.saveAsTextFile(args(2))

        sc.stop()

    }
}
