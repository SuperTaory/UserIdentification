import GeneralFunctionSets.{dayOfMonth_string, transTimeToTimestamp}
import org.apache.spark.sql.SparkSession

import scala.math.abs

object FindOverlapAFC {
    def main(args: Array[String]): Unit = {
        /**
         * 找出ground truth中存在overlap的AFC ID
         */
        val spark = SparkSession.builder()
            .appName("FindOverlapAFC")
            .getOrCreate()
        val sc = spark.sparkContext

        // (668367478,ECD09FC6C6C5,24.0,24,1.0)
        // 过滤出score>=1的部分afc乘客
        val parts = sc.textFile(args(0) + "zlt/UI/GroundTruth/IdMap/part-00000")
            .map(line => {
                val fields = line.split(',')
                val afcID = fields(0).drop(1)
                val num = fields(3).toInt
                val score = fields(4).dropRight(1).toFloat
                (afcID, num, score)
            }).filter(_._3 >= 1.0)

        val idSets = parts.map(_._1).collect().toSet
        val min_num = parts.map(_._2).min()

        // (667979926,2019-06-04 08:42:22,坪洲,21,2019-06-04 08:55:23,宝安中心,22)
        val afcData = sc.textFile(args(0) + "Destination/subway-pair/part-*")
            .map(line => {
                val fields = line.split(",")
                val id = fields(0).drop(1)
                val ot = transTimeToTimestamp(fields(1))
                val os = fields(2)
                val dt = transTimeToTimestamp(fields(4))
                val ds = fields(5)
                val day = dayOfMonth_string(fields(1))
                (id, (ot, os, dt, ds, day))
            }).groupByKey().map(line => {
            val id = line._1
            val data = line._2.toList.sortBy(_._1)
            val daySets = data.map(_._5).toSet
            val pairs = data.map(x => (x._2, x._4)).toSet
            (id, data, daySets, pairs)
        }).filter(_._2.length >= min_num - 5).cache()

        val partAFC = afcData.filter(x => idSets.contains(x._1))
        val partAFCData = sc.broadcast(partAFC.collect())
        val joinRDD = afcData.flatMap(line => {
            for (p <- partAFCData.value) yield {
                (line, p)
            }
        }).filter(x => x._1._1 != x._2._1 & x._1._3.intersect(x._2._3).nonEmpty & x._1._4.intersect(x._2._4).nonEmpty)

        val overlap = joinRDD.map(line => {
            val p1 = line._1._2
            val p2 = line._2._2
            var score = 0
            var i = 0
            var j = 0
            while (i < p1.length & j < p2.length) {
                if (p1(i)._2 == p2(j)._2 & p1(i)._4 == p2(j)._4) {
                    if (abs(p1(i)._1 - p2(j)._1) < 300 & abs(p1(i)._3 - p2(j)._3) < 300) {
                        score += 1
                        i += 1
                        j += 1
                    }
                    else if (p1(i)._1 < p2(j)._1)
                        i += 1
                    else
                        j += 1
                }
                else if (p1(i)._1 < p2(j)._1)
                    i += 1
                else
                    j += 1
            }
            (line._2._1, line._1._1, line._2._2.length, line._1._2.length, score)
        }).groupBy(_._1).mapValues(_.toList.sortBy(_._5).reverse.take(5))

        val res = overlap.flatMap(line => {
            for (tuple <- line._2) yield
                tuple
        }).repartition(1).sortBy(x => x._5, ascending = false)

        res.saveAsTextFile(args(0) + "/zlt/UI/OverlapAFC-1")
        sc.stop()
    }
}
