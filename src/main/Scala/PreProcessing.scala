import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer
import scala.math.min

object PreProcessing {
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("Pre-Processing")
    val sc = new SparkContext(conf)
    //val sq = SparkSession.builder().config(conf).getOrCreate()
    val sq = new SQLContext(sc)

    /*-----------------------------------------------------------------------------------------*/

    // 开始处理OD数据集,包括统计每个站点的客流量和提取某个乘客的OD信息
    val ODFile = sc.textFile(args(1))

    val ODFileRDD = ODFile.filter(_.split(',').length == 8).map(line => {
      val fields = line.split(',')
      val pid = fields(1)
      val time = fields(4).replace('T', ' ').dropRight(5)
      val station = fields(6)
      (pid, time, station)
    }).cache()

    // 统计每个站点的客流量,按照客流量升序排序并保存
    val PassengerFlowOfStation = ODFileRDD.map(x => (x._3, 1)).reduceByKey(_+_).sortBy(_._2, ascending = true)
    //PassengerFlowOfStation.repartition(1).saveAsTextFile(args(2))

    val minFlow = PassengerFlowOfStation.first()._2.toFloat
    //val maxFlow = PassengerFlowOfStation.take(PassengerFlowOfStation.count().toInt).last._2

    // 对各个站点根据其客流量设置打分参数:rsz
    val RankScoreOfStation = PassengerFlowOfStation.map(line => {
      val score = minFlow / line._2
      (line._1, score)
    })

    // 声明为广播变量用于查询
    val scoreInfo = sc.broadcast(RankScoreOfStation.collect())

    // 提取某个乘客的OD记录
    val personalOD = ODFileRDD.filter(_._1 == args(2))

    // 获取该乘客出现过的站点
    val stationAppear = personalOD.map(_._3).distinct().collect()

    //按照站点和用户ID分组，统计一个站点内该ID出现的次数
    val OD_cntRDD = personalOD.groupBy(line => {(line._3, line._1)}).mapValues(v => v.toList.length).map(line=>{
      (line._1._1, (line._1._2, line._2))
    })

    //(<station, list(<T.id, T.cnt>)>)
    //    val groupedODStaCnt = OD_cntRDD.groupBy(_._1._1).mapValues(v => {
    //      val listTraj = new ListBuffer[(String, Int)]
    //      v.foreach(x => listTraj.append((x._1._2, x._2)))
    //      listTraj.toList
    //    })

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
    }).filter(x =>{stationAppear.contains(x._3)}).sortBy(line => (line._1, line._2), ascending = true)
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
    })
    //println("after compression:" + compressionRDD.count())
    //compressionRDD.saveAsTextFile(args(2))

    // 按照站点和macID分组，统计一个站点内该macID出现的次数
    val macID_cntRDD = compressionRDD.groupBy(line => {(line._3, line._1)}).mapValues(v => v.toList.length).map(line=>{
      (line._1._1, (line._1._2, line._2))
    })
    //macID_cntRDD.saveAsTextFile(args(3))

    // 生成(<station, list(<T.id, T.cnt>)>)
//    val groupedMacStaCnt = macID_cntRDD.groupBy(_._1._1).mapValues(v => {
//      val listTraj = new ListBuffer[(String, Int)]
//      v.foreach(x => listTraj.append((x._1._2, x._2)))
//      listTraj.toList
//    })

    /*-----------------------------------------------------------------------------------------*/

    // 生成(TA.id, (TB.id, station, cnt))
//    val mergedStaCnt = groupedODStaCnt.leftOuterJoin(groupedMacStaCnt).flatMap(line => {
//      //val listTemp = new ListBuffer[(String, (String, String, Int))]
//      val station = line._1
//      val listTA = line._2._1
//      val listTB = line._2._2.get
//      for (x <- listTA.indices; y <- listTB.indices) yield{
//        //listTemp.append((listTA(x)._1, (listTB(y)._1, station, min(listTA(x)._2, listTB(y)._2))))
//        (listTA(x)._1, (listTB(y)._1, station, min(listTA(x)._2, listTB(y)._2)))
//      }
//    })

    // 生成(TA.id, (TB.id, station, cnt))
    val mergedStaCnt = OD_cntRDD.leftOuterJoin(macID_cntRDD).map(line => {
      (line._2._1._1, (line._2._2.get._1, line._1, min(line._2._1._2, line._2._2.get._2)))
    })

    //mergedStaCnt.saveAsTextFile(args(3))

    /*-----------------------------------------------------------------------------------------*/

    // (景田,612565)
    // (深大,610612)
    // (福永,526212)
    // (塘朗,521382)
    // Increase the ranking score of TB.id by (rsz * o)
    // 选择top-50为候选集合
    val CandidateSet = mergedStaCnt.map(line => {
      val station = line._2._2
      val rsz =  scoreInfo.value.toMap.get(station).get
      val score = line._2._3 * rsz
      (line._1, (line._2._1, line._2._2, line._2._3, score))
    }).sortBy(_._2._4, ascending = false)

    //val topQ_Candidates = sc.makeRDD(CandidateSet)
    //topQ_Candidates.repartition(1).saveAsTextFile(args(3))
    CandidateSet.saveAsTextFile(args(3))

    /*-----------------------------------------------------------------------------------------*/

    sc.stop()
//    sq.stop()

  }

}
