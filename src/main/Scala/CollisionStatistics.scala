import GeneralFunctionSets.transTimeToTimestamp
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.abs

object CollisionStatistics {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("CollisionStatistics")
        val sc = new SparkContext(conf)

        // (669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
        val AFCRDD = sc.textFile(args(0) + "/Destin/subway-pair/part-*").map(line => {
            val fields = line.split(',')
            val afcID = fields(0).drop(1)
            val ot = transTimeToTimestamp(fields(1))
            val os = fields(2)
            val dt = transTimeToTimestamp(fields(4))
            val ds = fields(5)
            (afcID, (ot, os, dt, ds))
        })

        val AFCByID = AFCRDD.groupByKey().mapValues(line => {
            val pointList = new ListBuffer[(Long, String)]()
            val stationSet = mutable.Set[String]()
            line.toList.sortBy(_._1).foreach(x => {
                pointList.append((x._1, x._2))
                pointList.append((x._3, x._4))
                stationSet.add(x._2)
                stationSet.add(x._4)
            })
            (pointList.toList, stationSet)
        })
        //    val AFCDataBroadcast = sc.broadcast(AFCByID.collect())


        // (1C48CE5E485B,2019-06-12 11:34:26,高新园,202,2019-06-12 12:08:37,老街,201)
        val APRDD = sc.textFile(args(0) + "/liutao/UI/GroundTruth/apData/part*").map(line => {
            val fields = line.split(',')
            val apId = fields(0).drop(1)
            val ot = transTimeToTimestamp(fields(1))
            val os = fields(2)
            val dt = transTimeToTimestamp(fields(4))
            val ds = fields(5)
            (apId, (ot, os, dt, ds))
        })

        val APByID = APRDD.groupByKey().mapValues(line => {
            val pointList = new ListBuffer[(Long, String)]()
            val stationSet = mutable.Set[String]()
            line.toList.sortBy(_._1).foreach(x => {
                pointList.append((x._1, x._2))
                pointList.append((x._3, x._4))
                stationSet.add(x._2)
                stationSet.add(x._4)
            })
            (pointList.toList, stationSet)
        })
        val APDataBroadcast = sc.broadcast(APByID.collect())

        val mergeAPAndAFC = AFCByID.flatMap(line => {
            for (v <- APDataBroadcast.value) yield {
                (v, line)
            }
        }).filter(x => {
            val apStationSet = x._1._2._2
            val afcStationSet = x._2._2._2
            if (apStationSet.intersect(afcStationSet).size > 3)
                true
            else
                false
        })

        val collisionRDD = mergeAPAndAFC.map(line => {
            var count = 0
            var total = 0
            val apData = line._1._2._1
            val afcData = line._2._2._1
            var afcIndex = 0
            var apIndex = 0
            while (afcIndex < afcData.length && apIndex < apData.length) {
                if (apData(apIndex)._1 < afcData(afcIndex)._1 - 600)
                    apIndex += 1
                else if (apData(apIndex)._1 > afcData(afcIndex)._1 + 600)
                    afcIndex += 1
                else if (apData(apIndex)._2 == afcData(afcIndex)._2) {
                    count += 1
                    val diff = abs(afcData(afcIndex)._1 - apData(apIndex)._1)
                    total += diff.toInt
                    afcIndex += 1
                    apIndex += 1
                }
                else {
                    if (apData(apIndex)._1 > afcData(afcIndex)._1)
                        afcIndex += 1
                    else
                        apIndex += 1
                }
            }
            (line._1._1, (line._2._1, count))
        }).groupByKey().mapValues(v => v.toList.maxBy(v => v._2))

        val groundTruthData = sc.textFile(args(0) + "/liutao/UI/GroundTruth/IdMap/part-*").map(line => {
            val fields = line.split(",")
            val afcId = fields(0).drop(1)
            val apId = fields(1)
            (apId, afcId)
        })
        val groundTruthMap = sc.broadcast(groundTruthData.collect().toMap)

        val result = collisionRDD.map(line => {
            var flag = 0
            if (groundTruthMap.value(line._1).equals(line._2._1))
                flag = 1
            (flag, 1)
        }).reduceByKey(_ + _)
        val resultMap = result.collect().toMap
        println(resultMap(1).toFloat / (resultMap(0) + resultMap(1)))
        println(resultMap)

        sc.stop()
    }
}
