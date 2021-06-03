import GeneralFunctionSets.{dayOfMonth_long, hourOfDay_long, transTimeToString, transTimeToTimestamp}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.{abs, max}
import scala.util.Random

object SamplingAPData {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("SamplingAPData")
            .getOrCreate()
        val sc = spark.sparkContext

        val readODTimeInterval = sc.textFile(args(0) + "/zlt_hdfs/UI/AllODTimeInterval/ShortPathTime/part-*").map(line => {
            val p = line.split(',')
            val sou = p(0).drop(1)
            val des = p(1)
            val interval = p(2).dropRight(1).toLong
            ((sou, des), interval)
        })
        val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

        val mostViewPathFile = sc.textFile(args(0) + "/zlt_hdfs/UI-2021/MostViewPath/part-00000").map(line => {
            val path = line.split(",")
            val so = path.head
            val sd = path.last
            ((so, sd), path)
        })
        val mostViewPathMap = sc.broadcast(mostViewPathFile.collect().toMap)

        // (4C49E3376FFF,2019-06-28 19:09:48,留仙洞,1)
        val macFile = sc.textFile(args(0) + "/zlt_hdfs/UI-2021/GroundTruth/APData/part*").map(line => {
            val fields = line.split(',')
            val macId = fields(0).drop(1)
            val time = transTimeToTimestamp(fields(1))
            val station = fields(2)
            val dur = fields(3).dropRight(1).toLong
            (macId, (time, station, dur))
        }).filter(x => x._2._3 < 900 & hourOfDay_long(x._2._1) >= 6  & x._1 != "000000000000")
            .groupByKey()
            .mapValues(_.toArray.sortBy(_._1))
            .filter(_._2.length > 5)

        // 划分为出行片段并标记出行日期
        val APSegments = macFile.map(line => {
            // 设置出行片段长度阈值
            val m = 1
            val MacId = line._1
            val data = line._2
            val segment = new ListBuffer[(Long, String, Long)]
            val segments = new ListBuffer[List[(Long, String, Long)]]
            val daySets: mutable.Set[Int] = mutable.Set()
            for (s <- data) {
                if (segment.isEmpty) {
                    segment.append(s)
                }
                else {
                    if (s._2 == segment.last._2) {
                        if (segment.length > m) {
                            segments.append(segment.toList)
                            daySets.add(dayOfMonth_long(segment.head._1))
                        }
                        segment.clear()
                    }
                    else {
                        // 设置容忍时间误差
                        var attachInterval = 0
                        val odInterval = ODIntervalMap.value((segment.last._2, s._2))
                        odInterval / 1800 match {
                            case 0 => attachInterval = 600 //10min
                            case 1 => attachInterval = 900 //15min
                            case _ => attachInterval = 1200 // 20min
                        }
                        val realInterval = abs(s._1 - segment.last._1 - segment.last._3)
                        if (realInterval > odInterval + attachInterval) {
                            if (segment.length > m) {
                                segments.append(segment.toList)
                                daySets.add(dayOfMonth_long(segment.head._1))
                            }
                            segment.clear()
                        }
                    }
                    segment.append(s)
                }
            }
            if (segment.length > m) {
                segments.append(segment.toList)
                daySets.add(dayOfMonth_long(segment.head._1))
            }
            (MacId, segments.toList, daySets)
        })

        val samplingData = APSegments.flatMap(line => {
            val id = line._1
            val data = line._2
            val ratio = args(1).toDouble / 10
            val num = (ratio * data.length).toInt
            val preserve = Random.shuffle(data).take(num)
            val completeTrips = new ListBuffer[((Long, String, Long), (Long, String, Long))]()
            val sampledTrips = new ListBuffer[(String, String, String, String)]()
            for (segment <- preserve) {
                val od = (segment.head._2, segment.last._2)
                val samplingTime = max(300, (segment.last._1 - segment.head._1) / 3)
                val path = mostViewPathMap.value(od)
                var flag = true
                for (i <- 1.until(path.length) if flag) {
                    val tempTime = ODIntervalMap.value(path.head, path(i))
                    if ( tempTime >= samplingTime){
                        flag = false
                        completeTrips.append((segment.head, segment.last))
                        sampledTrips.append((transTimeToString(segment.head._1), segment.head._2, transTimeToString(segment.head._1 + tempTime), path(i)))
                    }
                }
            }

            val trips = completeTrips.map(line =>
                (transTimeToString(line._1._1), line._1._2, line._1._3.toString,
                    transTimeToString(line._2._1), line._2._2, line._2._3.toString)).toList

            for (i <- trips.indices) yield {
                (id, trips(i), sampledTrips(i))
            }
        }).repartition(5)
            .sortBy(x => (x._1, x._2._1))
            .map(line => (line._1, line._2.productIterator.mkString(","), line._3.productIterator.mkString(",")))

        val filePath = args(0) + "/zlt_hdfs/UI-2021/GroundTruth/SampledAPData-" + (args(1).toDouble * 10).formatted("%.0f%%")
        val hadoopConf = sc.hadoopConfiguration
        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
        val path = new Path(filePath)
        if(hdfs.exists(path))
            hdfs.delete(path,true)
        samplingData.saveAsTextFile(filePath)

//        val samplingData = APSegments.flatMap(line => {
//            val id = line._1
//            val data = line._2
//            val ratio = args(1).toDouble
//            val num = (ratio * data.length).toInt
//            val preserve = Random.shuffle(data).take(num)
//            val completeTrips = new ListBuffer[((Long, String, Long), (Long, String, Long))]()
//            val sampledTrips = new ListBuffer[(String, String, String, String)]()
//            for (seg <- preserve) {
//                val time = seg.last._1 - seg.head._1
//                var samplingTime = 0L
//                time / 1800 match {
//                    case 0 => samplingTime = time / 2
//                    case 1 => samplingTime = time / 3
//                    case _ => samplingTime = 1800
//                }
//                if (seg.length == 2) {
//                    sampledTrips.append((transTimeToString(seg.head._1), seg.head._2, transTimeToString(seg.last._1), seg.last._2))
//                } else{
//                    Random.setSeed(seg.head._1)
//                    val index = Random.nextInt(seg.length - 1)
//                    var flag = true
//                    for (j <- (index + 1).until(seg.length) if flag){
//                        if (seg(j)._1 - seg(index)._1 >= samplingTime){
//                            sampledTrips.append((transTimeToString(seg(index)._1), seg(index)._2, transTimeToString(seg(j)._1), seg(j)._2))
//                            flag = false
//                        }
//                    }
//                    if (flag){
//                        for(k <- (index-1).until(-1, step = -1) if flag) {
//                            if (seg.last._1 - seg(k)._1 >= samplingTime){
//                                sampledTrips.append((transTimeToString(seg(k)._1), seg(k)._2, transTimeToString(seg.last._1), seg.last._2))
//                                flag = false
//                            }
//                        }
//                    }
//                }
//                completeTrips.append((seg.head, seg.last))
//            }
//            val trips = completeTrips.map(line =>
//                (transTimeToString(line._1._1), line._1._2, line._1._3.toString,
//                    transTimeToString(line._2._1), line._2._2, line._2._3.toString)).toList
//
//            for (i <- trips.indices) yield {
//                (id, trips(i), sampledTrips(i))
//            }
//        }).repartition(5).filter(x => x._3._2 != x._3._4 & x._2._2 != x._2._5)
//            .sortBy(x => (x._1, x._2._1))
//            .map(line => (line._1, line._2.productIterator.mkString(","), line._3.productIterator.mkString(",")))
//        samplingData.saveAsTextFile(args(0) + "/zlt_hdfs/UI-2021/GroundTruth/SampledAPData-20%_old")

//        // 按天进行采样
//        val samplingByDay = partition.map(line => {
//            val l = line._3.size / 2
//            val chosenDays = Random.shuffle(line._3).take(l)
//            val sampledData = new ListBuffer[List[(Long, String, Long)]]
//            for (s <- line._2) {
//                if (chosenDays.contains(dayOfMonth_long(s.head._1))) {
//                    sampledData.append(s)
//                }
//            }
//            (line._1, sampledData, chosenDays)
//        })

//        // 对出行片段采样
//        val samplingOnPartitions = samplingByDay.map(line => {
//            val sampledData = new ListBuffer[((Long, String, Long), (Long, String, Long))]
//            for (s <- line._2) {
//                val tempData = new ListBuffer[((Long, String, Long), Double)]
//                // 设置随机数种子seed
//                val r = new Random(System.currentTimeMillis())
//                var sum = 0F
//                for (v <- s) {
//                    if (v._3 < 30)
//                        sum += 30
//                    else {
//                        sum += v._3
//                    }
//                }
//                for (v <- s) {
//                    if (v._3 < 30)
//                        tempData.append((v, pow(r.nextFloat(), sum / 30)))
//                    else {
//                        tempData.append((v, pow(r.nextFloat(), sum / v._3)))
//                    }
//                }
//                val temp = tempData.sortBy(_._2).takeRight(2).toList.sortBy(_._1._1)
//                sampledData.append((temp.head._1, temp.last._1))
//            }
//            (line._1, sampledData.toList)
//        })

//        val results = sampling.flatMap(line => {
//            for (v <- line._2) yield
//                (line._1, transTimeToString(v._1._1), v._1._2, v._1._3, transTimeToString(v._2._1), v._2._2, v._2._3)
//        }).repartition(5).sortBy(x => (x._1, x._2))

//        results.saveAsTextFile(args(0) + "/zlt_hdfs/UI-2021/GroundTruth/SampledAPData-20%")
        sc.stop()
    }
}
