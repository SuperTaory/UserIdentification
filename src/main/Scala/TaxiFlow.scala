import GeneralFunctionSets.{halfHourOfDay, transTimeToTimestamp}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object TaxiFlow {
    def main(args: Array[String]): Unit = {
        /**
         * 统计深圳市某grid内taxi的流入和流出流量
         */
        //        val spark = SparkSession.builder()
        //            .appName("TaxiFlow")
        //            .getOrCreate()
        //        val sc = spark.sparkContext
        val conf = new SparkConf().setAppName("TaxiFlow")
        val sc = new SparkContext(conf)

        // 001A0A6EBF1BD107,2016-06-26 11:51:12,113.813515,22.623617,0,1
        // 读取taxi的gps的记录
        val file = sc.textFile(args(0) + "/SZODA/gps/2016-" + args(3) + "/part*.gz").map(line => {
            val fields = line.split(",")
            val id = fields.head
            val time = transTimeToTimestamp(fields(1))
            val lon = fields(2)
            val lat = fields(3)
            (id, (time, lon.toFloat, lat.toFloat))
        })

        val maxLon = 114.2774203
        val minLon = 113.7981906

        val maxLat = 22.78449632
        val minLat = 22.47662421

        val m = args(1).toInt
        val n = args(2).toInt

        def gridNum(lon: Float, lat: Float): Int = {
            val width = (maxLon - minLon) / m
            val height = (maxLat - minLat) / n
            val w = ((lon - minLon) / width).toInt
            val h = ((lat - minLat) / height).toInt
            w * m + h
        }

        // map为(id,(时间戳, 网格编号, 时间段编号))
        // 网格编号：0 ~ m*n-1
        // 时间段编号：0 ～ 当月天数*48-1
        val groupByID = file.map(x => (x._1, (x._2._1, gridNum(x._2._2, x._2._3), halfHourOfDay(x._2._1))))
            .groupByKey()
            .mapValues(value => {
                val data = value.toList.sortBy(_._1)
                val compressedData = new ListBuffer[(Long, Int, Long, Int)]
                for (i <- 0.until(data.length - 1)) {
                    // 下一个位置跟前一个位置所在网格不同则记录
                    if (data(i)._2 != data(i + 1)._2) {
                        compressedData.append((data(i)._3, data(i)._2, data(i + 1)._3, data(i + 1)._2))
                    }
                }
                compressedData.toList
            })
            .flatMap(line => {
                val data = line._2
                for (pair <- data) yield
                    pair
            })


        val totalInFLow = groupByID.map(x => (x._3, x._4))
        val eachGridInFlow = totalInFLow.groupByKey().map(line => {
            val period = line._1
            val flow = line._2.toList.groupBy(x => x).map(x => (x._1, x._2.length))
            val res = new ListBuffer[Int]
            for (i <- 0.until(m * n)) {
                if (flow.get(i).isEmpty)
                    res.append(0)
                else
                    res.append(flow(i))
            }
            (period, res.toList)
        })
            .repartition(1)
            .sortBy(x => x._1).map(line => line._1.toString + "," + line._2.mkString(","))

        eachGridInFlow.saveAsTextFile(args(0) + "/zlt/gjj/TaxiOutFlow/" + args(3))

        val totalOuFlow = groupByID.map(x => (x._1, x._2))
        val eachGridOutFlow = totalOuFlow.groupByKey().map(line => {
            val period = line._1
            val flow = line._2.toList.groupBy(x => x).map(x => (x._1, x._2.length))
            val res = new ListBuffer[Int]
            for (i <- 0.until(m * n)) {
                if (flow.get(i).isEmpty)
                    res.append(0)
                else
                    res.append(flow(i))
            }
            (period, res.toList)
        })
            .repartition(1)
            .sortBy(x => x._1).map(line => line._1.toString + "," + line._2.mkString(","))

        eachGridOutFlow.saveAsTextFile(args(0) + "/zlt/gjj/TaxiInFlow/" + args(3))


        sc.stop()
    }

}
