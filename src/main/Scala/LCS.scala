import GeneralFunctionSets.{dayOfMonth_long, transTimeToTimestamp}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object LCS {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("LCS")
            .getOrCreate()
        val sc = spark.sparkContext
        val hadoopConf = sc.hadoopConfiguration
        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

        // 读取地铁站点名和编号映射关系 "1,机场东,22.647011,113.8226476,1268036000,268"
        val stationFile = sc.textFile(args(0) + "/zlt/AllInfo/stationInfo-UTF-8.txt")
        val stationNo2NameRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationNo.toInt, stationName)
        })
        val stationNo2Name = sc.broadcast(stationNo2NameRDD.collect().toMap)

        // 读取站间时间间隔，单位：秒 "(龙华,清湖,133)"
        val readODTimeInterval = sc.textFile(args(0) + "/zlt/UI/AllODTimeInterval/ShortPathTime/part-00000").map(line => {
            val p = line.split(',')
            val sou = p(0).drop(1)
            val des = p(1)
            val interval = p(2).dropRight(1).toLong
            ((sou, des), interval)
        })
        val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

        // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"
        val validPathFile = sc.textFile(args(0) + "/zlt/AllInfo/allpath.txt").map(line => {
            val fields = line.split(' ').dropRight(5)
            val sou = stationNo2Name.value(fields(0).toInt)
            val des = stationNo2Name.value(fields(fields.length - 1).toInt)
            val path = fields.map(x => stationNo2Name.value(x.toInt))
            ((sou, des), path)
        }).groupByKey().mapValues(x => (x.toArray, x.minBy(_.length).length))

        val validPathMap = sc.broadcast(validPathFile.collect().toMap)

        // 读取groundTruth计算Accuracy (251449740,ECA9FAE07B4F,26.857,43,0.6245814)
        val groundTruthData = sc.textFile(args(0) + "/zlt/UI-2021/GroundTruth/IdMap/part-*").map(line => {
            val fields = line.split(",")
            val afcId = fields(0).drop(1)
            val apId = fields(1)
            (apId, afcId)
        })
        val groundTruthMap = sc.broadcast(groundTruthData.collect().toMap)

        /**
         * AFC data: (669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
         */
        val AFCFile = sc.textFile(args(0) + "/Destination/subway-pair/part-000[0-6]*").flatMap(line => {
            val fields = line.split(',')
            val id = fields(0).drop(1)
            val ot = transTimeToTimestamp(fields(1))
            val os = fields(2)
            val dt = transTimeToTimestamp(fields(4))
            val ds = fields(5)
            val o_day = dayOfMonth_long(ot)
            val d_day = dayOfMonth_long(dt)
            val day = if (o_day == d_day) o_day else 0
            val positions = new ArrayBuffer[(Long, String)]()
            positions.append((ot, os))
            positions.append((dt, ds))
            for (po <- positions) yield {
                (id, (po, day))
            }
        }).filter(_._2._2 > 0)

        // 根据id聚合,仅保留出行天数大于5天的乘客数据
        val AFCPartitions = AFCFile.groupByKey().map(x => {
            val dataArray = x._2.toList.map(_._1).sortBy(_._1)
            val daySets = x._2.toList.map(_._2).toSet
            (x._1, dataArray, daySets)
        }).filter(_._3.size > 5)

        /**
         * 读取AP数据:(00027EF9CD6F,2019-06-01 08:49:11,固戍,452,2019-06-01 09:16:29,洪浪北,150,2019-06-01 08:49:11,固戍,2019-06-01 08:54:39,坪洲)
         */
        val APFile = sc.textFile(args(0) + "/zlt/UI-2021/GroundTruth/SampledAPData-" + args(1) + "%/*").flatMap(line => {
            val fields = line.split(",")
            val id = fields(0).drop(1)
            val samp_ot = transTimeToTimestamp(fields(7))
            val samp_os = fields(8)
            val samp_dt = transTimeToTimestamp(fields(9))
            val samp_ds = fields(10).dropRight(1)
            val o_day = dayOfMonth_long(samp_ot)
            val d_day = dayOfMonth_long(samp_dt)
            val day = if (o_day == d_day) o_day else 0
            val positions = new ArrayBuffer[(Long, String)]()
            positions.append((samp_ot, samp_os))
            positions.append((samp_dt, samp_ds))
            for (po <- positions) yield {
                (id, (po, day))
            }
        }).filter(_._2._2 > 0)

        // 划分AP
        val APPartitions = APFile.groupByKey().map(x => {
            val apId = x._1
            val trips = x._2.toArray.map(_._1).sortBy(_._1)
            val daySets = x._2.toArray.map(_._2).toSet
            (daySets.size, (apId, trips, daySets))
        })
        val APData = sc.broadcast(APPartitions.groupByKey().mapValues(_.toArray).collect().toMap)

        // 将AP和AFC数据按照天数结合
        val mergeData = AFCPartitions.flatMap(afc => {
            val extra = 5
            val limit = afc._3.size + extra
            val candidateDays = APData.value.keys.toSet.filter(x => x <= limit)
            for (i <- candidateDays; ap <- APData.value(i)) yield {
                (ap, afc)
            }
        })

        val matchData = mergeData.map(line => {
            val APId = line._1._1
            val AFCId = line._2._1
            val AP = line._1._2
            val AFC = line._2._2
            var index_ap = 0
            var index_afc = 0
            var count = 0

            while (index_ap < AP.length && index_afc < AFC.length) {
                val cur_ap = AP(index_ap)
                val cur_afc = AFC(index_afc)
                if (cur_ap._1 < cur_afc._1 - 180){
                    index_ap += 1
                }
                else if (cur_afc._1 < cur_ap._1 + 180) {
                    index_afc += 1
                }
                else{
                    if (cur_ap._2 == cur_afc._2) {
                        count += 1
                    }
                    index_afc += 1
                    index_ap += 1
                }
            }

            (APId, (AFCId,  count))
        }).filter(_._2._2 > 0)

        val resultMap = matchData.groupByKey().mapValues(_.toArray.maxBy(_._2)).map(line => {
            var flag = 0
            if (groundTruthMap.value(line._1) == line._2._1) {
                flag = 1
            }
            (flag, 1)
        }).reduceByKey(_ + _).repartition(1).map(x => (x._1, x._2, args(1) + "%"))

        val filePath = args(0) + "/zlt/UI-2021/LCS/" + args(1) + "%"
        val path = new Path(filePath)
        if(hdfs.exists(path))
            hdfs.delete(path,true)
        resultMap.saveAsTextFile(filePath)
        sc.stop()
    }
}
