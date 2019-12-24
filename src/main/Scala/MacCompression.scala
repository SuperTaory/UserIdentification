import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.catalyst.plans.logical.LeafNode

import scala.collection.mutable.ListBuffer

object MacCompression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MacCompression")
    val sc = new SparkContext(conf)
    val sq = SparkSession.builder().config(conf).getOrCreate()
//    val sq = new SQLContext(sc)

    /*-----------------------------------------------------------------------------------------*/

    // 将mac数据文件格式由parquet转换为普通格式，并对mac数据进行处理压缩；而OD数据不需要进行压缩处理
    // 原Mac数据格式：[000000000140,1559177788,八卦岭,114.09508148721412,22.56193897883047,9]
    // 转换后Mac格式：000000000140,1559177788,八卦岭,停留时间-秒数

    /*-----------------------------------------------------------------------------------------*/

//    // 读取数据量超大的ID过滤掉
//    val macIDFile = sc.textFile(args(0))
//      .map(line => {
//        val fields = line.split(',')
//        val id = "'" + fields(0).drop(1) + "'"
//        val count = fields(1).dropRight(1).toInt
//        (id, count)
//      }).filter(_._2 > 3000).map(x => x._1)
//    // 转换成字符串便于构造sql语句
//    val macIdString = "(" + macIDFile.reduce(_ + ',' + _) + ")"

    // 开始处理Mac数据集
    // 读取mac数据并转换为数据库Table格式
    val macFile = sq.read.parquet(args(0))
    macFile.printSchema()
//    val macSchema = macFile.schema
//    macFile.createOrReplaceTempView("MacTable")
//    macFile.registerTempTable("MacTable")
    // 1559318400是指2019/06/01 00:00:00对应的unix时间戳
//    val macRDD = sq.sql("select * from MacTable").rdd

    // 过滤掉数据量超大的数据避免数据倾斜
    val macRDD = macFile.select("mac","time","stationName").filter("macCount > 20 and macCount < 100000").rdd
//    macFile.select("mac","time","stationName","macCount").rdd.take(10).foreach(println)
//    println(macRDD.count())

    val transformedMacRDD = macRDD.map(line => {
      val fields = line.toString().split(',')
      val macId = fields(0).drop(1)
      val time = fields(1).toLong
      val station = fields(2).dropRight(1)
//      val macCount = fields.last.dropRight(1).toInt
      (macId, (time, station))
    })
//    println(transformedMacRDD.count())

    val groupAndSort = transformedMacRDD.groupByKey().mapValues(_.toList.sortBy(_._1)).repartition(100)

//    // 自定义combineByKey函数，目的是代替groupByKey以提高效率，但运行时发现可能存在问题，遂暂时弃用
//    def myCreateCombiner(v :(Long, String)) : List[(Long, String)] = List(v)
//    def myMergeValue(l : List[(Long, String)], v : (Long, String)) : List[ (Long, String)] = l :+ v
//    def myMergeCombiners(l1 : List[(Long, String)], l2 : List[(Long, String)]) : List[(Long, String)] = l1 ::: l2
//    val combineRDD = transformedMacRDD.combineByKey(myCreateCombiner, myMergeValue, myMergeCombiners)
//    println(combineRDD.count())


    // 统计停留时间
    val compressedMacRDD = groupAndSort.flatMap(line => {
      var tempStation = "null"
      var tempTime = 0L
      var interval = 0L
      var preTime = 0L
      val processedData = new ListBuffer[(Long, String, Long)]
      for (x <- line._2){
        if (x._2.equals(tempStation) && x._1 - preTime < 600) {
          interval = x._1 - tempTime
          preTime = x._1
        }
        else {
          if (tempStation != "null"){
            processedData.append((tempTime, tempStation, interval))
            interval = 0
          }
          tempStation = x._2
          tempTime = x._1
          preTime = x._1
        }
      }
      processedData.append((tempTime, tempStation, interval))
      for (v <- processedData) yield {
        (line._1, v._1, v._2, v._3)
      }
    })

    compressedMacRDD.saveAsTextFile(args(1))

//    // 将时间格式从时间戳转换为时间字符串
//    val changeTimeFormat = compressedMacRDD.map(line => {
//      (line._1, transTimeToString(line._2), line._3, line._4)
//    })
//    changeTimeFormat.saveAsTextFile(args(2))

//    val personalMacRDD = compressionRDD.map(line => {
//      val time = transTimeToString(line._2.toString)
//      (line._1, time + '/' + line._3)
//    }).groupByKey().mapValues(v => v.toList.sorted.reduce(_ + ',' + _))
//    personalMacRDD.saveAsTextFile(args(1))

    sc.stop()
  }


  def transTimeToString(time_tamp : Long) : String = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val time = dateFormat.format(time_tamp * 1000)
    time
  }
}
