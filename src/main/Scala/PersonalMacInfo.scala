import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object PersonalMacInfo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("PersonalMacInfo")
        //    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER")
        //    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable","true")
        val sc = new SparkContext(conf)
        val sq = SparkSession.builder().config(conf).getOrCreate()

        //    val sq = new SQLContext(sc)

        // 读取要统计的macID
        val macIDFile = sc.textFile(args(0))
            .map(line => {
                "'" + line.split(',')(0).drop(1) + "'"
            })
            .collect()

        // 转换成字符串便于构造sql语句
        val macIdString = "(" + macIDFile.reduce(_ + ',' + _) + ")"


        // 读取mac数据并转换为数据库Table格式
        val macFile = sq.read.parquet(args(1))
        macFile.printSchema()
        val macSchema = macFile.schema
        macFile.createOrReplaceTempView("MacTable")
        //    macFile.registerTempTable("MacTable")
        // 1559318400是指2019/06/01 00:00:00对应的unix时间戳
        val macRDD = sq.sql("select * from MacTable where " + macSchema.fields(0).name + " in " + macIdString).rdd

        val transformedMacRDD = macRDD.map(line => {
            val fields = line.toString().split(',')
            val macId = fields(0).drop(1)
            val time = fields(1).toLong
            val station = fields(2)
            (macId, (time, station))
        }).groupByKey().mapValues(v => v.toList.sortBy(_._1))

        val compressedMacRDD = transformedMacRDD.map(line => {
            var tempStation = "null"
            var tempTime = 0L
            var interval = 0L
            var preTime = 0L
            var personalInfo = ""
            for (x <- line._2) {
                if (x._2.equals(tempStation) && x._1 - preTime < 1800) {
                    interval = x._1 - tempTime
                    preTime = x._1
                }
                else {
                    if (tempStation != "null") {
                        val temp = transTimeToString(tempTime.toString) + '/' + tempStation
                        val m = interval / 60
                        val s = interval % 60
                        interval = 0
                        personalInfo += temp + '/' + m + 'm' + s + 's' + ','
                    }
                    tempStation = x._2
                    tempTime = x._1
                    preTime = x._1
                }
            }
            val temp = transTimeToString(tempTime.toString) + '/' + tempStation
            val m = interval / 60
            val s = interval % 60
            personalInfo += temp + '/' + m + 'm' + s + 's'
            (line._1, personalInfo)
        })

        compressedMacRDD.saveAsTextFile(args(2))
        sc.stop()
    }

    def transTimeToString(time_tamp: String): String = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.format(time_tamp.toLong * 1000)
        time
    }
}
