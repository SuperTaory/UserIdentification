import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object MacCompression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MacCompression")
    val sc = new SparkContext(conf)
    // val sq = SparkSession.builder().config(conf).getOrCreate()
    val sq = new SQLContext(sc)

    /*-----------------------------------------------------------------------------------------*/

    // 将mac数据文件格式由parquet转换为普通格式，并对mac数据进行处理压缩；而OD数据不需要进行压缩处理
    // 原Mac数据格式：[000000000140,1559177788,八卦岭,114.09508148721412,22.56193897883047,9]
    // 转换后Mac格式：000000000140,1559177788,八卦岭

    /*-----------------------------------------------------------------------------------------*/

    // 开始处理Mac数据集
    // 读取mac数据并转换为数据库Table格式
    val macFile = sq.read.parquet(args(0))
    macFile.printSchema()
    val macSchema = macFile.schema
    //    macFile.createOrReplaceTempView("MacTable")
    macFile.registerTempTable("MacTable")
    // 1559318400是指2019/06/01 00:00:00对应的unix时间戳
    val macRDD = sq.sql("select * from MacTable where " + macSchema.fieldNames(1) + " > 1559318400").rdd

    val transformedMacRDD = macRDD.map(line => {
      val fields = line.toString().split(',')
      val macId = fields(0).drop(1)
      val time = fields(1)
      val station = fields(2)
      (macId, time, station)
    }).sortBy(line => (line._1, line._2), ascending = true)
    //println("before compression:" + transformedMacRDD.count())
    //transformedMacRDD.saveAsTextFile(args(1))


    // 过滤掉同一站点相邻时刻重复检测到的mac信息
    var baseTime = "0"
    var baseStation = "null"
    val limitTime = 600 // 10min = 600sec
    val compressionRDD = transformedMacRDD.filter(line =>{
      if (line._3 == baseStation && line._2.toInt < baseTime.toInt + limitTime)
        false
      else{
        baseStation = line._3
        baseTime = line._2
        true
      }
    }).map(line => {
      val time = transTimeToString(line._2)
      (line._1, time, line._3)
    })

    compressionRDD.saveAsTextFile(args(1))
    sc.stop()
  }

  def transTimeToString(time_tamp : String) : String = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    val time = dateFormat.format(time_tamp.toLong * 1000)
    time
  }
}
